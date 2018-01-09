import { setInterval, clearInterval } from 'timers';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';

import {
  Task,
  TaskMeta,
  TaskState,
  RetryStrategy,
} from './common';

import {
  getRetryDelayMs,
  getDelayQueue,
  getDelayQueueOptions,
} from './helper';

const log = debug('amqp-worker:consumer');

export interface Logger {
  info: (obj: object) => void;
  error: (obj: object) => void;
}

export interface ProcessFunc {
  (data: any, task?: Task): Promise<any>;
}

export interface ConsumerConfig {
  /**
   *  The queue name is in the form of `${taskName}_${queueSuffix}`, default 'queue'
   */
  queueSuffix?: string;

  /**
   *  Amqp exchange name, please be sure it is same as it in the producer, default 'worker-exchange'
   */
  exchangeName?: string;

  /**
   *  Amqp url, default 'amqp://localhost'
   */
  url?: string;

  /**
   *  Socket options for amqp connec
   */
  socketOptions?: any;

  /**
   *  Logger instance
   */
  logger?: Logger;

  /**
   *  Apm wrap, such as newrelic
   */
  processWrap?: (func: ProcessFunc) => ProcessFunc;

  /**
   *  Rre process hook
   */
  preProcess?: (task: Task) => void;

  /**
   *  Post process hook
   */
  postProcess?: (task: Task, state: TaskState, errorOrResult: any) => void;

  /**
   * Global concurrency for task, will be overrided by task, default: 256
   */
  globalConcurrency?: number;
}

export class Consumer {
  public static instances: Consumer[] = [];

  private static connection: amqp.Connection;
  private static isConnecting: boolean = false;
  private static alreadyClosed: boolean;

  private config: ConsumerConfig;
  private channel: amqp.Channel = null;
  private isChannelCreating: boolean = false;
  private queueName: string = null;
  private taskMeta: TaskMeta;
  private processFunc: ProcessFunc;
  private consumerTag: string;
  private checkJob: NodeJS.Timer;

  public constructor(config: ConsumerConfig = {}) {
    const defaultConfig: ConsumerConfig = {
      logger: {
        info: _.noop,
        error: _.noop,
      },
      processWrap: null,
      queueSuffix: 'queue',
      exchangeName: 'worker-exchange',
      url: 'amqp://localhost',
      globalConcurrency: 256,
    };
    this.config = Object.assign({}, defaultConfig, config);

    // Collected for moniting
    Consumer.instances.push(this);
  }

  public async createConnection(): Promise<amqp.Connection> {
    if (Consumer.connection) return Consumer.connection;
    log('Begin create connection');

    Consumer.connection = await amqp.connect(this.config.url, this.config.socketOptions);
    log('Got connection');

    Consumer.connection.on('error', (err) => {
      console.error(`Connection error ${err} stack: ${err.stack}`);
    });

    Consumer.connection.on('close', () => {
      console.log('Connection close');
      Consumer.connection = null;
    });

    return Consumer.connection;
  }

  private async createChannel(): Promise<amqp.Channel> {
    if (this.channel) return this.channel;

    const taskMeta = this.taskMeta;
    const prefetchSize = taskMeta.concurrency || this.config.globalConcurrency;

    this.queueName = `${taskMeta.name}_${this.config.queueSuffix}`;
    log(`Creating task queue: ${this.queueName}, prefetch size: ${prefetchSize}`);
    const conn = await this.createConnection();

    log('Begin create channel');
    const channel = await conn.createChannel();

    channel.on('error', (err) => {
      console.error(`Channel error ${err} stack: ${err.stack}`);
    });

    channel.on('close', () => {
      console.log('Channel close');
      this.channel = null;
    });

    const options: amqp.Options.AssertQueue = {
      durable: true,
    };
    if (taskMeta.maxPriority) {
      options.maxPriority = taskMeta.maxPriority;
    }

    await channel.assertExchange(this.config.exchangeName, 'direct');

    await Promise
      .all([
        channel.assertQueue(this.queueName, options),
        channel.bindQueue(this.queueName, this.config.exchangeName, taskMeta.name),
        channel.prefetch(prefetchSize),
      ]);
    log('Got channel');
    this.channel = channel;
    return channel;
  }

  public async checkHealth(): Promise<any> {
    if (this.channel && this.queueName) {
      return this.channel.checkQueue(this.queueName);
    }
    return {};
  }

  private async checkConnection(enableInterval?: boolean): Promise<void> {
    log('Check connection');

    if (enableInterval) {
      this.checkJob = setInterval(this.checkConnection.bind(this), 500);
    }

    if (!Consumer.connection) {
      if (Consumer.isConnecting) return;

      Consumer.isConnecting = true;
      try {
        await this.createConnection();
      } catch (e) {
        Consumer.isConnecting = false;
        throw e;
      }
      Consumer.isConnecting = false;
    }

    if (!this.channel) {
      if (this.isChannelCreating) return;

      try {
        await this.createChannel();
      } catch (e) {
        this.isChannelCreating = false;
        throw e;
      }

      this.isChannelCreating = false;

      await this.consume();
    }
  }

  private async taskRetry(task: Task): Promise<number> {
    if (task.retryCount >= task.maxRetry) {
      return Promise.resolve(0);
    }
    const delayMs = getRetryDelayMs(task);

    // Increase retry count
    task.retryCount += 1;

    const delayQueue = getDelayQueue(delayMs, this.config.exchangeName, task.name);
    const queueDaclareOptions = getDelayQueueOptions(delayMs, this.config.exchangeName, task.name);

    await this.channel.assertQueue(delayQueue, queueDaclareOptions);
    await this.channel.sendToQueue(delayQueue, new Buffer(JSON.stringify(task)), {
      persistent: true,
    });
    return task.maxRetry - task.retryCount;
  }

  private logSuccess(msg: amqp.Message, startTime: number, result: object): void {
    this.config.logger.info({
      taskName: this.taskMeta.name,
      data: msg.content,
      duration: Date.now() - startTime,
      result: JSON.stringify(result),
      amqp: {
        fields: msg.fields,
        properties: msg.properties,
      },
    });
  }

  private logFail(msg: amqp.Message, startTime: number, e: Error): void {
    this.config.logger.error({
      taskName: this.taskMeta.name,
      data: msg.content,
      duration: Date.now() - startTime,
      error: e,
      errorStacks: e.stack && e.stack.split('\n'),
      amqp: {
        fields: msg.fields,
        properties: msg.properties,
      },
    });
  }

  public async cancel(): Promise<void> {
    if (!this.channel || !this.consumerTag) return;
    this.channel.cancel(this.consumerTag);
    this.channel = null;
    this.consumerTag = null;
  }

  private async consume(): Promise<any> {
    const channel = this.channel;
    const _this = this;

    let processFunc = _this.processFunc;
    if (this.config.processWrap) {
      processFunc = this.config.processWrap(processFunc);
    }

    log('Begin to consume');
    const taskName = _this.taskMeta.name.replace(/-/g, '_');
    async function processFuncWrap(msg) {
      if (msg === null) return;

      const startTime = Date.now();

      let task: Task;
      try {
        task = JSON.parse(msg.content.toString());
      } catch (e) {
        _this.logFail(msg, startTime, e);
        channel.ack(msg);
        return;
      }

      if (_this.config.preProcess) {
        _this.config.preProcess.bind(this);
        _this.config.preProcess(task);
      }

      try {
        const result = await processFunc(task.body, task);

        if (_this.config.postProcess) {
          _this.config.postProcess.bind(this);
          _this.config.postProcess(task, TaskState.SUCCEED, result);
        }

        _this.logSuccess(msg, startTime, result);

        channel.ack(msg);
      } catch (e) {
        e.retryLeft = await _this.taskRetry(task);
        e.state = TaskState.FAILED;

        if (e.retryLeft === 0) {
          e.state = TaskState.PERMANENT_FAILED;
        }

        if (_this.config.postProcess) {
          _this.config.postProcess.bind(this);
          _this.config.postProcess(task, e.state, e);
        }

        _this.logFail(msg, startTime, e);

        channel.ack(msg);
      }
    }

    const { consumerTag } = await channel.consume(this.queueName, processFuncWrap, {
      noAck: false,
    });

    this.consumerTag = consumerTag;
  }

  register(taskMeta: TaskMeta, processFunc: ProcessFunc) {
    this.taskMeta = taskMeta;
    this.processFunc = processFunc;

    this.checkConnection(true);

    return this;
  }
}
