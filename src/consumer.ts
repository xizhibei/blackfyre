import { EventEmitter } from 'events';
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

const log = debug('blackfyre:consumer');
const eventLog = debug('blackfyre:consumer:event');

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
   *  Amqp exchange name, default: 'direct'
   */
  exchangeType?: string;

  /**
   *  Options for assert AssertExchange
   */
  assertExchangeOptions?: amqp.Options.AssertExchange;

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
  processWrap?: (taskName: string, func: ProcessFunc) => ProcessFunc;

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

interface TaskRegister {
  taskMeta: TaskMeta;
  processFunc: ProcessFunc;
  queueName: string;
  consumerTag?: string;
  channel?: amqp.Channel;
}

interface TaskRegisterMap {
  [taskName: string]: TaskRegister;
}

export class Consumer extends EventEmitter {
  protected connection: amqp.Connection;
  private isConnecting: boolean = false;

  private config: ConsumerConfig;
  private checkJob: NodeJS.Timer;

  private taskRegisterMap: TaskRegisterMap = {};
  private waitQueue: TaskRegister[] = [];

  public constructor(config: ConsumerConfig = {}) {
    super();

    const defaultConfig: ConsumerConfig = {
      logger: {
        info: _.noop,
        error: _.noop,
      },
      processWrap: null,
      queueSuffix: 'queue',
      exchangeName: 'worker-exchange',
      exchangeType: 'direct',
      assertExchangeOptions: null,
      url: 'amqp://localhost',
      globalConcurrency: 256,
    };
    this.config = Object.assign({}, defaultConfig, config);

    this.checkJob = setInterval(this.checkConnection.bind(this), 500);
  }

  public async createConnection(): Promise<amqp.Connection> {
    if (this.connection) return this.connection;
    log('Begin create connection');

    this.connection = await amqp.connect(this.config.url, this.config.socketOptions);
    log('Got connection');

    this.connection.on('error', (err) => {
      this.emit('conn-error', err);
      eventLog(`Connection error ${err} stack: ${err.stack}`);
    });

    this.connection.on('close', () => {
      this.emit('conn-close');
      eventLog('Connection close');
      this.connection = null;
    });

    return this.connection;
  }

  private async createChannel(register: TaskRegister): Promise<amqp.Channel> {
    if (register.channel) return register.channel;

    const taskMeta = register.taskMeta;
    const prefetchSize = taskMeta.concurrency || this.config.globalConcurrency;

    log('Creating queue: %s, prefetch size: %d', register.queueName, prefetchSize);
    const conn = await this.createConnection();

    log('Begin create channel');
    const channel = await conn.createChannel();

    channel.on('error', (err) => {
      this.emit('channel-error', err);
      eventLog(`Channel error ${err} stack: ${err.stack}`);
    });

    channel.on('close', () => {
      this.emit('channel-close');
      eventLog('Channel close');
      register.channel = null;
    });

    const options: amqp.Options.AssertQueue = {
      durable: true,
    };
    if (taskMeta.maxPriority) {
      options.maxPriority = taskMeta.maxPriority;
    }

    await channel.assertExchange(
      this.config.exchangeName,
      this.config.exchangeType,
      this.config.assertExchangeOptions,
    );

    await Promise
      .all([
        channel.assertQueue(register.queueName, options),
        channel.bindQueue(register.queueName, this.config.exchangeName, taskMeta.name),
        channel.prefetch(prefetchSize),
      ]);

    log('Got channel');
    register.channel = channel;

    this.emit('channel-created', channel);

    return channel;
  }

  public async checkHealth(): Promise<any> {
    return Promise.map(_.values(this.taskRegisterMap), async (taskRegister) => {
      if (taskRegister.channel && taskRegister.queueName) {
        return taskRegister.channel.checkQueue(taskRegister.queueName);
      }
      return [];
    });
  }

  private async getConnection(): Promise<amqp.Connection> {
    if (this.connection) return this.connection;
    if (this.isConnecting) return null;

    this.isConnecting = true;
    try {
      await this.createConnection();
      await this.drainQueue();
    } catch (e) {
      this.isConnecting = false;
      this.emit('error', e);
      return null;
    }
    this.isConnecting = false;
    return this.connection;
  }

  private async drainQueue(): Promise<void> {
    log('Queue size %d', this.waitQueue.length);
    while (this.waitQueue.length) {
      const taskRegister = this.waitQueue.pop();
      await this.createChannelAndConsume(taskRegister);
    }
  }

  private async createChannelAndConsume(taskRegister: TaskRegister): Promise<void> {
    if (!this.connection) {
      this.waitQueue.push(taskRegister);
      return;
    }
    log('Create channel and consume %s', taskRegister.taskMeta.name);
    try {
      await this.createChannel(taskRegister);
      await this.consume(taskRegister);
      eventLog('Emit ready %s', taskRegister.taskMeta.name);
      this.emit('ready', taskRegister.taskMeta.name);
    } catch (e) {
      this.emit('error', e);
      return;
    }
  }

  private async checkConnection(): Promise<void> {
    await Promise.map(_.values(this.taskRegisterMap), async (taskRegister) => {
      if (taskRegister.channel) return;
      return this.createChannelAndConsume(taskRegister);
    });

    if (!this.connection) {
      await this.getConnection();
    }
  }

  private async taskRetry(taskRegister: TaskRegister, task: Task): Promise<number> {
    if (task.retryCount >= task.maxRetry) {
      return Promise.resolve(0);
    }
    const delayMs = getRetryDelayMs(task);

    // Increase retry count
    task.retryCount += 1;

    const delayQueue = getDelayQueue(delayMs, this.config.exchangeName, task.name);
    const queueDaclareOptions = getDelayQueueOptions(delayMs, this.config.exchangeName, task.name);

    await taskRegister.channel.assertQueue(delayQueue, queueDaclareOptions);
    await taskRegister.channel.sendToQueue(delayQueue, new Buffer(JSON.stringify(task)), {
      persistent: true,
    });
    return task.maxRetry - task.retryCount;
  }

  private logSuccess(msg: amqp.Message, startTime: number, result: object): void {
    this.config.logger.info({
      taskName: msg.fields.routingKey,
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
      taskName: msg.fields.routingKey,
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

  private async consume(taskRegister: TaskRegister): Promise<any> {
    const channel = taskRegister.channel;
    const that = this;

    let processFunc = taskRegister.processFunc;
    if (this.config.processWrap) {
      processFunc = this.config.processWrap(taskRegister.taskMeta.name, processFunc);
    }

    log('Begin to consume');
    async function processFuncWrap(msg) {
      if (msg === null) return;

      const startTime = Date.now();

      let task: Task;
      try {
        task = JSON.parse(msg.content.toString());
      } catch (e) {
        that.logFail(msg, startTime, e);
        channel.ack(msg);
        return;
      }

      if (that.config.preProcess) {
        that.config.preProcess.bind(this);
        that.config.preProcess(task);
      }

      try {
        const result = await processFunc(task.body, task);

        if (that.config.postProcess) {
          that.config.postProcess.bind(this);
          that.config.postProcess(task, TaskState.SUCCEED, result);
        }

        that.logSuccess(msg, startTime, result);

        channel.ack(msg);
      } catch (e) {
        if (!e.noRetry) {
          e.retryLeft = await that.taskRetry(taskRegister, task);
          e.state = TaskState.FAILED;

          if (e.retryLeft === 0) {
            e.state = TaskState.PERMANENT_FAILED;
          }
        } else {
          e.state = TaskState.PERMANENT_FAILED;
        }

        if (that.config.postProcess) {
          that.config.postProcess.bind(this);
          that.config.postProcess(task, e.state, e);
        }

        that.logFail(msg, startTime, e);

        channel.ack(msg);
      }
    }

    const { consumerTag } = await channel.consume(taskRegister.queueName, processFuncWrap, {
      noAck: false,
    });

    taskRegister.consumerTag = consumerTag;
  }

  public async registerTask(taskMeta: TaskMeta, processFunc: ProcessFunc): Promise<void> {
    const taskRegister = <TaskRegister>{
      taskMeta,
      processFunc,
      queueName: `${taskMeta.name}_${this.config.queueSuffix}`,
    };
    this.taskRegisterMap[taskMeta.name] = taskRegister;

    await this.getConnection();

    await this.createChannelAndConsume(taskRegister);
  }

  public async closeChannel(taskName: string): Promise<void> {
    const register = this.taskRegisterMap[taskName];
    /* istanbul ignore else */
    if (register && register.channel) {
      await register.channel.close();
    }
  }

  public async closeConnection(): Promise<void> {
    /* istanbul ignore else */
    if (this.connection) {
      await this.connection.close();
    }
  }
}
