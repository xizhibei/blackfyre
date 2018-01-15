import { EventEmitter } from 'events';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as uuid from 'uuid';
import * as Bluebird from 'bluebird';

import { Task, RetryStrategy } from './common';

import { BackendType, Backend } from './backends/interface';
import { MongodbBackend, MongodbBackendOptions } from './backends/mongodb';
import { BrokerType, Broker } from './brokers/interface';
import { AMQPBrokerOptions, AMQPBroker } from './brokers/amqp';
import { registerEvent } from './helper';

const log = debug('blackfyre:producer');
const eventLog = debug('blackfyre:producer:event');

Promise = Bluebird as any;

export interface ProducerOptions {
  /**
   * Test mode, useful for testing, default false
   */
  isTestMode?: boolean;

  /**
   *  Broker type, default: AMQP
   */
  brokerType?: BrokerType;

  /**
   *  Broker options
   */
  brokerOptions?: AMQPBrokerOptions;

  /**
   *  Backend type, default: MongoDB
   */
  backendType?: BackendType;

  /**
   *  Backend options
   */
  backendOptions?: MongodbBackendOptions;

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

export class Producer extends EventEmitter {
  private options: ProducerOptions;

  private backend: Backend;
  private broker: Broker;

  public createdTasks: Task[] = [];

  constructor(options: ProducerOptions = {}) {
    super();

    const defaultConfig: ProducerOptions = {
      isTestMode: false,
      globalMaxRetry: 0,
      globalInitDelayMs: 100,
      globalretryStrategy: RetryStrategy.FIBONACCI,
      backendType: BackendType.MongoDB,
      backendOptions: null,
      brokerType: BrokerType.AMQP,
      brokerOptions: null,
    };
    this.options = Object.assign({}, defaultConfig, options);

    if (this.options.backendType === BackendType.MongoDB) {
      this.backend = new MongodbBackend(this.options.backendOptions);
    }

    if (this.options.brokerType === BrokerType.AMQP) {
      this.broker = new AMQPBroker(this.options.brokerOptions);
    }

    registerEvent(['error', 'ready', 'close'], this.broker, this);
    registerEvent(['error', 'close'], this.backend, this);
  }

  public async createTask(task: Task): Promise<any> {
    task.id = uuid.v4();
    task.retryCount = 0;

    task.maxRetry = task.maxRetry || this.options.globalMaxRetry;
    task.initDelayMs = task.initDelayMs || this.options.globalInitDelayMs;
    task.retryStrategy = task.retryStrategy || this.options.globalretryStrategy;

    if (this.options.isTestMode) {
      this.createdTasks.push(task);
      return;
    }

    return this.broker.publish(task);
  }

  public async close(): Promise<any> {
    return Promise.props({
      broker: this.broker.close(),
      backend: this.backend.close(),
    });
  }

  public async checkHealth(): Promise<any> {
    return Promise.props({
      backend: this.backend.checkHealth(),
      broker: this.broker.checkHealth(),
    });
  }
}
