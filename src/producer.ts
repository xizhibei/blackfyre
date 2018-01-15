import { EventEmitter } from 'events';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as uuid from 'uuid';
import * as Bluebird from 'bluebird';

import { Task, RetryStrategy, RetryOptions } from './common';

import { BackendType, Backend } from './backends/interface';
import { MongodbBackend, MongodbBackendOptions } from './backends/mongodb';
import { BrokerType, Broker } from './brokers/interface';
import { AMQPBrokerOptions, AMQPBroker } from './brokers/amqp';
import { registerEvent } from './helper';

const log = debug('blackfyre:producer');

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
   * Global retry options
   * Default:
   *     maxRetry: 0
   *     initDelayMs: 100
   *     delayMs: 100
   *     retryStrategy: FIBONACCI
   */
  globalRetryOptions?: RetryOptions;
}

export class Producer extends EventEmitter {
  private options: ProducerOptions;

  private backend: Backend;
  private broker: Broker;

  public createdTasks: Task[] = [];

  constructor(options: ProducerOptions = {}) {
    super();

    this.options = Object.assign(<ProducerOptions>{
      isTestMode: false,
      backendType: BackendType.MongoDB,
      backendOptions: null,
      brokerType: BrokerType.AMQP,
      brokerOptions: null,
    }, options);

    this.options.globalRetryOptions = Object.assign(<RetryOptions>{
      maxRetry: 0,
      initDelayMs: 100,
      delayMs: 100,
      retryStrategy: RetryStrategy.FIBONACCI,
    }, options.globalRetryOptions);

    log('Init backend %s', this.options.backendType);
    if (this.options.backendType === BackendType.MongoDB) {
      this.backend = new MongodbBackend(this.options.backendOptions);
      registerEvent(['error', 'close'], this.backend, this);
    }

    log('Init broker %s', this.options.brokerType);
    if (this.options.brokerType === BrokerType.AMQP) {
      this.broker = new AMQPBroker(this.options.brokerOptions);
      registerEvent(['error', 'ready', 'close'], this.broker, this);
    }

  }

  public async createTask(task: Task): Promise<any> {
    task.id = task.id || uuid.v4();
    task.retryCount = 0;

    task = Object.assign({}, this.options.globalRetryOptions, task);

    if (this.options.isTestMode) {
      this.createdTasks.push(task);
      return;
    }

    await this.backend && this.backend.setTaskStatePending(task);

    return this.broker.publish(task);
  }

  public async close(): Promise<any> {
    return Promise.props({
      broker: this.broker.close(),
      backend: this.backend && this.backend.close(),
    });
  }

  public async checkHealth(): Promise<any> {
    return Promise.props({
      backend: this.backend && this.backend.checkHealth(),
      broker: this.broker.checkHealth(),
    });
  }
}
