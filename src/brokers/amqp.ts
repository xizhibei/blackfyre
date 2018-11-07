import { setInterval, clearInterval } from 'timers';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as Bluebird from 'bluebird';

import { Broker, TaskRegister, TaskRegisterMap } from './interface';
import { ProcessFunc } from '../consumer';
import { Task, TaskMeta, TaskState } from '../common';

import {
  getRetryDelayMs,
  getDelayQueue,
  getDelayQueueOptions,
} from '../helper';

Promise = Bluebird as any;

const log = debug('blackfyre:broker:amqp');
const eventLog = debug('blackfyre:broker:amqp:event');

export interface AMQPBrokerOptions {
  /**
   *  Amqp url, default 'amqp://localhost'
   */
  url?: string;

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
   *  Options for assert exchange
   */
  exchangeOptions?: amqp.Options.AssertExchange;

  /**
   *  Options for assert queue (consumer)
   */
  queueOptions?: amqp.Options.AssertQueue;

  /**
   *  Socket options for amqp connec
   */
  socketOptions?: any;
}

interface AMQPTaskRegister extends TaskRegister {
  queueName: string;
  consumerTag?: string;
  channel?: amqp.Channel;
}


export class AMQPBroker extends Broker {
  private options: AMQPBrokerOptions;
  private connection: amqp.Connection;
  private isConnecting: boolean = false;
  private producerChannel: amqp.ConfirmChannel;
  private isProducerChannelCreating: boolean = false;

  private taskRegisterMap: TaskRegisterMap = {};
  private taskWaitQueue: any[] = [];

  private checkJob: NodeJS.Timer;

  constructor(options: AMQPBrokerOptions = {}) {
    super();

    this.options = _.assign({
      url: 'amqp://localhost',
      queueSuffix: 'queue',
      exchangeName: 'worker-exchange',
      exchangeType: 'direct',
      socketOptions: null,
      exchangeOptions: null,
    }, options);

    this.options.queueOptions = _.assign({
      durable: true,
    }, options.queueOptions);

  }

  private async createConnection(): Promise<amqp.Connection> {
    const url = this.options.url;
    this.connection = await amqp.connect(url, this.options.socketOptions);

    this.connection.on('error', (err) => {
      this.emit('error', err);
      eventLog(`Connection error ${err} stack: ${err.stack}`);
    });

    this.connection.on('close', () => {
      this.emit('close', 'connection');
      eventLog('Connection close');
      this.connection = null;
    });

    return this.connection;
  }

  private async getConnection(): Promise<amqp.Connection> {
    /* istanbul ignore if */
    if (this.connection) {
      await this.drainQueue();
      return this.connection;
    }
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

  private async createProducerChannel(): Promise<amqp.ConfirmChannel> {
    /* istanbul ignore if */
    if (this.producerChannel) return this.producerChannel;

    log('Created connection !!!');
    const channel = await this.connection.createConfirmChannel();

    channel.on('error', (err) => {
      this.emit('error', err);
      eventLog(`Channel error ${err} stack: ${err.stack}`);
    });

    channel.on('close', () => {
      this.emit('close', 'channel');
      eventLog('Channel close');
      this.producerChannel = null;
    });

    log('Created channel !!!');
    await channel.assertExchange(
      this.options.exchangeName,
      this.options.exchangeType,
      this.options.exchangeOptions,
    );

    this.producerChannel = channel;

    this.emit('channel-created', channel);

    return channel;
  }

  private async createConsumerChannel(register: AMQPTaskRegister) {
    /* istanbul ignore if */
    if (register.channel) return register.channel;

    const taskMeta = register.taskMeta;
    const prefetchSize = taskMeta.concurrency;

    log('Begin create channel');
    const channel = await this.connection.createChannel();

    channel.on('error', (err) => {
      this.emit('error', err);
      eventLog(`Channel error ${err} stack: ${err.stack}`);
    });

    channel.on('close', () => {
      this.emit('close', 'channel');
      eventLog('Channel close');
      register.channel = null;
      register.channeling = false;
    });

    await channel.assertExchange(
      this.options.exchangeName,
      this.options.exchangeType,
      this.options.exchangeOptions,
    );

    const queueOptions: amqp.Options.AssertQueue = _.clone(this.options.queueOptions);
    if (taskMeta.maxPriority) {
      queueOptions.maxPriority = taskMeta.maxPriority;
    }

    await Promise
      .all([
        channel.assertQueue(register.queueName, queueOptions),
        channel.bindQueue(register.queueName, this.options.exchangeName, taskMeta.name),
        channel.prefetch(prefetchSize),
      ]);

    log('Got channel');
    register.channel = channel;

    this.emit('channel-created', channel);

    return channel;
  }

  private async getProducerChannel(): Promise<amqp.ConfirmChannel> {
    /* istanbul ignore if */
    if (this.producerChannel) return this.producerChannel;

    if (this.isProducerChannelCreating) return null;
    this.isProducerChannelCreating = true;

    try {
      await this.createProducerChannel();
      eventLog('Producer ready');
      this.emit('ready');
    } catch (e) {
      this.isProducerChannelCreating = false;
      this.emit('error', e);
      return;
    }

    this.isProducerChannelCreating = false;
    return this.producerChannel;
  }

  private async drainQueue(): Promise<void> {
    const unregisteredTasks = _.filter(_.values(this.taskRegisterMap), registerTask => {
      if (registerTask.channeling) return false
      registerTask.channeling = true;
      return true
    })
    log('TaskRegister Queue size %d', unregisteredTasks.length);
    await Promise.map(unregisteredTasks, async (taskRegister) => {
      await this.createChannelAndConsume(taskRegister);
    });

    log('Task queue size %d', this.taskWaitQueue.length);
    if (this.taskWaitQueue.length) {
      await this.getProducerChannel();
      if (!this.producerChannel) return;
      while (this.taskWaitQueue.length) {
        const func = this.taskWaitQueue.pop();
        await func();
      }
    }
  }

  private async checkConsumerConnection(): Promise<void> {
    await this.getConnection();
  }

  private async createChannelAndConsume(taskRegister: AMQPTaskRegister): Promise<void> {
    log('Create channel and consume %s', taskRegister.taskMeta.name);
    try {
      await this.createConsumerChannel(taskRegister);
      await this.consume(taskRegister);
      eventLog('Consumer ready %s', taskRegister.taskMeta.name);
      this.emit('ready', taskRegister.taskMeta.name);
    } catch (e) {
      this.emit('error', e);
      return;
    }
  }

  private async retryTask(task: Task): Promise<number> {
    const taskRegister = this.taskRegisterMap[task.name];

    if (task.retryCount >= task.maxRetry) {
      return Promise.resolve(0);
    }
    const delayMs = getRetryDelayMs(task);

    // Increase retry count
    task.retryCount += 1;

    const delayQueue = getDelayQueue(delayMs, this.options.exchangeName, task.name);
    const queueDaclareOptions = getDelayQueueOptions(delayMs, this.options.exchangeName, task.name);

    await taskRegister.channel.assertQueue(delayQueue, queueDaclareOptions);
    await taskRegister.channel.sendToQueue(delayQueue, Buffer.from(JSON.stringify(task)), {
      persistent: true,
    });
    return task.maxRetry - task.retryCount;
  }

  private async consume(taskRegister: AMQPTaskRegister): Promise<void> {
    const channel = taskRegister.channel;
    const that = this;
    async function processFuncWrap(msg) {
      /* istanbul ignore if */
      if (msg === null) return;

      let task: Task;
      try {
        task = JSON.parse(msg.content.toString());
      } catch (e) {
        channel.ack(msg);
        that.emit('error', e);
        return;
      }

      try {
        const result = await taskRegister.processFunc(task.body, task);
      } catch (e) {
        if (!e.noRetry) {
          await that.retryTask(task);
        }
      }
      channel.ack(msg);
    }

    log('consume %s', taskRegister.queueName);
    const { consumerTag } = await taskRegister.channel.consume(
      taskRegister.queueName,
      processFuncWrap,
      {
        noAck: false,
      },
    );

    taskRegister.consumerTag = consumerTag;
  }

  private async send(task: Task): Promise<any> {
    const ch = this.producerChannel;

    const data = JSON.stringify(task);

    const exchangeName = this.options.exchangeName;
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

      return ch.sendToQueue(delayQueue, Buffer.from(data), publishOptions);
    }

    return new Promise((resolve, reject) => {
      return ch.publish(
        exchangeName,
        routingKey,
        Buffer.from(data),
        publishOptions,
        (err, ok) => {
          /* istanbul ignore if */
          if (err) return reject(err);
          return resolve(ok);
        },
      );
    });
  }

  public async registerTask(taskMeta: TaskMeta, processFunc: ProcessFunc): Promise<void> {
    const taskRegister = <TaskRegister>{
      taskMeta,
      processFunc,
      queueName: `${taskMeta.name}_${this.options.queueSuffix}`,
    };
    this.taskRegisterMap[taskMeta.name] = taskRegister;

    await this.getConnection();

    if (!this.checkJob) {
      this.checkJob = setInterval(this.checkConsumerConnection.bind(this), 500);
    }
  }

  public async publish(task: Task): Promise<any> {
    const p = new Promise((resolve, reject) => {
      log('task %s pushed to wait queue', task.name);
      this.taskWaitQueue.push(() => {
        return this.send(task).then(resolve, reject);
      });
    });

    await this.getConnection();

    return p;
  }

  public async checkHealth(): Promise<any> {
    return Promise.props({
      consumer: Promise.map(_.values(this.taskRegisterMap), async (taskRegister) => {
        if (taskRegister.channel && taskRegister.queueName) {
          return taskRegister.channel.checkQueue(taskRegister.queueName);
        }
        return [];
      }),
      producer: this.producerChannel && this.producerChannel.checkExchange(this.options.exchangeName),
    });
  }

  public async close(): Promise<void> {
    /* istanbul ignore if */
    if (this.connection) {
      await this.connection.close();
    }
  }

}
