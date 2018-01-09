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
  private static connection: amqp.Connection;
  private static alreadyClosed: boolean;

  private config: ConsumerConfig;
  private channel: amqp.Channel = null;
  private queueName: string = null;
  private connecting: boolean = false;
  private taskMeta: TaskMeta;
  private processFunc: ProcessFunc;

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
      console.error('Connection close');
      this.channel = null;
    });
    Consumer.connection.on('blocked', () => {
      console.error('Connection blocked');
    });
    Consumer.connection.on('unblocked', () => {
      console.error('Connection unblocked');
    });

    if (!Consumer.alreadyClosed) {
      Consumer.alreadyClosed = true;
      process.once('exit', (sig) => {
        if (Consumer.connection) {
          Consumer.connection.close();
          Consumer.connection = null;
        }
        log(`Exit with sig: ${sig}`);
      });
    }

    return Consumer.connection;
  }

  private async createChannel(): Promise<amqp.Channel> {
    const taskMeta = this.taskMeta;
    const prefetchSize = taskMeta.concurrency || this.config.globalConcurrency;

    this.queueName = `${taskMeta.name}_${this.config.queueSuffix}`;
    log(`Creating task queue: ${this.queueName}, prefetch size: ${prefetchSize}`);
    const conn = await this.createConnection();

    log('Begin create channel');
    const channel = await conn.createChannel();

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

  private async checkConnection(): Promise<void> {
    log('Check connection');
    if (!this.channel && !this.connecting) {
      this.connecting = true;
      await this.createChannel();

      this.connecting = false;
      this.consume();
    }
    setTimeout(this.checkConnection.bind(this), 1000);
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
      taskType: this.taskMeta.name,
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
      taskType: this.taskMeta.name,
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

        if (_this.config.postProcess) {
          _this.config.postProcess.bind(this);
          _this.config.postProcess(task, TaskState.FAILED, e);
        }

        _this.logFail(msg, startTime, e);

        channel.ack(msg);
      }
    }

    channel.consume(this.queueName, processFuncWrap, {
      noAck: false,
    });
  }

  register(taskMeta: TaskMeta, processFunc: ProcessFunc) {
    this.taskMeta = taskMeta;
    this.processFunc = processFunc;

    this.checkConnection();

    return this;
  }
}
