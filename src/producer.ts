import { EventEmitter } from 'events';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as uuid from 'uuid';
import * as Bluebird from 'bluebird';

import {
  Task,
  RetryStrategy,
} from './common';

import {
  getDelayQueue,
  getDelayQueueOptions,
} from './helper';

const log = debug('blackfyre:producer');
const eventLog = debug('blackfyre:consumer:event');

Promise = Bluebird as any;

export interface ProducerConfig {
  /**
   * Amqp exchange name, please be sure it is same as it in the consumer, default 'worker-exchange'
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
   * Test mode, useful for testing, default false
   */
  isTestMode?: boolean;

  /**
   * Amqp url, default 'amqp://localhost'
   */
  url?: string;

  /**
   *  Socket options for amqp connec
   */
  socketOptions?: amqp.Options.Connect;

  /**
   * Global max retry for task, will be override by task metadata, default: 0
   */
  globalMaxRetry?: number;

  /**
   * Global init delay for task, will be override by task metadata, default: 100
   */
  globalInitDelayMs?: number;

  /**
   * Global retry strategy for task, will be override by task metadata, default: FIBONACCI
   */
  globalretryStrategy?: RetryStrategy;
}

interface PublishFunc {
  (channel: amqp.ConfirmChannel): Promise<any>;
}

export class Producer extends EventEmitter {
  protected connection: amqp.Connection;
  private channel: amqp.ConfirmChannel = null;
  private isConnecting: boolean = false;
  private waitQueue: any[] = [];
  private config: ProducerConfig;

  public createdTasks: Task[] = [];

  constructor(config: ProducerConfig = {}) {
    super();

    const defaultConfig: ProducerConfig = {
      exchangeName: 'worker-exchange',
      exchangeType: 'direct',
      assertExchangeOptions: null,
      isTestMode: false,
      url: 'amqp://localhost',
      globalMaxRetry: 0,
      globalInitDelayMs: 100,
      globalretryStrategy: RetryStrategy.FIBONACCI,
    };
    this.config = Object.assign({}, defaultConfig, config);
  }

  private async createConnection(): Promise<amqp.Connection> {
    const url = this.config.url;
    this.connection = await amqp.connect(url, this.config.socketOptions);

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

  private async createChannel(): Promise<amqp.ConfirmChannel> {
    const conn = await this.createConnection();

    log('Created connection !!!');
    const channel = await conn.createConfirmChannel();

    channel.on('error', (err) => {
      this.emit('channel-error', err);
      eventLog(`Channel error ${err} stack: ${err.stack}`);
    });

    channel.on('close', () => {
      this.emit('channel-close');
      eventLog('Channel close');
      this.channel = null;
    });

    log('Created channel !!!');
    await channel.assertExchange(
      this.config.exchangeName,
      this.config.exchangeType,
      this.config.assertExchangeOptions,
    );

    this.channel = channel;

    this.emit('channel-created', channel);

    return channel;
  }

  private async drainQueue(): Promise<void> {
    log('Drain queue', this.waitQueue.length);
    while (this.waitQueue.length > 0) {
      const wait = this.waitQueue.pop();
      await wait(this.channel);
    }
  }

  private async getChannel(): Promise<amqp.ConfirmChannel> {
    log(`Wait queue length: ${this.waitQueue.length}`);
    if (!this.channel) {
      try {
        await this.createChannel();
      } catch (e) {
        this.emit('error', e);
        return;
      }
    }
    await this.drainQueue();
    this.emit('ready');
    return this.channel;
  }

  private getPushlistFunc(task: Task): PublishFunc {
    const that = this;
    return async function publish(ch) {
      const data = JSON.stringify(task);

      const exchangeName = that.config.exchangeName;
      const routingKey = task.name;

      const publishOptions: amqp.Options.Publish = {
        persistent: true,
        priority: task.priority,
      };

      if (task.eta && task.eta > Date.now()) {
        const delayMs = task.eta - Date.now();
        const delayQueue = getDelayQueue(delayMs, exchangeName, routingKey);
        const queueDaclareOptions = getDelayQueueOptions(delayMs, exchangeName, routingKey);

        await ch.assertQueue(delayQueue, queueDaclareOptions);

        return ch.sendToQueue(delayQueue, new Buffer(data), publishOptions);
      }

      return new Promise((resolve, reject) => {
        return ch.publish(exchangeName, routingKey, new Buffer(data), publishOptions, (err, ok) => {
          /* istanbul ignore if */
          if (err) return reject(err);
          return resolve(ok);
        });
      });
    };
  }

  public async createTask(task: Task): Promise<any> {
    task.id = uuid.v4();
    task.retryCount = 0;

    task.maxRetry = task.maxRetry || this.config.globalMaxRetry;
    task.initDelayMs = task.initDelayMs || this.config.globalInitDelayMs;
    task.retryStrategy = task.retryStrategy || this.config.globalretryStrategy;

    if (this.config.isTestMode) {
      this.createdTasks.push(task);
      return;
    }

    if (!this.isConnecting && !this.connection) {
      this.getChannel();
    }

    const publish: PublishFunc = this.getPushlistFunc(task);

    if (this.channel) return publish(this.channel);

    return new Promise((resolve, reject) => {
      this.waitQueue.push((channel) => {
        return publish(channel).then(resolve, reject);
      });
    });
  }

  public async closeChannel(): Promise<void> {
    /* istanbul ignore else */
    if (this.channel) {
      await this.channel.close();
    }
  }

  public async closeConnection(): Promise<void> {
    /* istanbul ignore else */
    if (this.connection) {
      await this.connection.close();
    }
  }
}
