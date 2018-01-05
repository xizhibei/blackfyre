import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as uuid from 'uuid';

import {
  Task,
  RetryStrategy,
} from './common';

import {
  getDelayQueue,
  getDelayQueueOptions,
} from './helper';

const log = debug('amqp-worker:producer');

export interface ProducerConfig {
  /**
   * Amqp exchange name, please be sure it is same as it in the consumer, default 'worker-exchange'
   */
  exchangeName?: string;

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
}

export class Producer {
  private connection: amqp.Connection;
  private channel: amqp.ConfirmChannel = null;
  private connecting: boolean = false;
  private waitQueue: any[] = [];
  private config: ProducerConfig;

  public createdTasks: Task[] = [];

  constructor(config: ProducerConfig = {}) {

    const defaultConfig: ProducerConfig = {
      exchangeName: 'worker-exchange',
      isTestMode: false,
      url: 'amqp://localhost',
    };
    this.config = Object.assign({}, defaultConfig, config);
  }

  private async getConnection(): Promise<amqp.Connection> {
    const url = this.config.url;
    this.connection = await amqp.connect(url, this.config.socketOptions);

    this.connection.on('error', (err) => {
      console.error(`Connection error ${err} stack: ${err.stack}`);
    });
    this.connection.on('close', () => {
      console.error('Connection close');
      this.connection = null;
      this.channel = null;
    });
    this.connection.on('blocked', () => {
      console.error('Connection blocked');
    });
    this.connection.on('unblocked', () => {
      console.error('Connection unblocked');
    });
    return this.connection;
  }

  private async createChannel(): Promise<amqp.ConfirmChannel> {
    const conn = await this.getConnection();

    log('Created connection !!!');
    this.channel = await conn.createConfirmChannel();

    log('Created channel !!!');
    await this.channel.assertExchange(this.config.exchangeName, 'direct');
    return this.channel;
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
      await this.createChannel();
    }
    await this.drainQueue();
    return this.channel;
  }

  public async createTask(taskMetadata: Task): Promise<any> {
    const that = this;

    taskMetadata.id = uuid.v4();

    if (this.config.isTestMode) {
      this.createdTasks.push(taskMetadata);
      return;
    }

    const publishOptions: amqp.Options.Publish = {
      persistent: true,
      priority: taskMetadata.priority,
    };

    if (!this.connecting) {
      this.connecting = true;
      this.getChannel();
    }

    async function publish(ch) {
      const data = JSON.stringify(taskMetadata);

      const exchangeName = that.config.exchangeName;
      const routingKey = taskMetadata.name;

      if (taskMetadata.eta && taskMetadata.eta > Date.now()) {
        const delayMs = taskMetadata.eta - Date.now();
        const delayQueue = getDelayQueue(delayMs, exchangeName, routingKey);
        const queueDaclareOptions = getDelayQueueOptions(delayMs, exchangeName, routingKey);

        await ch.assertQueue(delayQueue, queueDaclareOptions);

        return ch.sendToQueue(delayQueue, new Buffer(data), publishOptions);
      }

      return new Promise((resolve, reject) => {
        return ch.publish(exchangeName, routingKey, new Buffer(data), publishOptions, (err, ok) => {
          if (err) return reject(err);
          return resolve(ok);
        })
      });
    }

    if (this.channel) return publish(this.channel);

    return new Promise((resolve, reject) => {
      this.waitQueue.push((channel) => {
        return publish(channel).then(resolve, reject);
      })
    });
  }
}
