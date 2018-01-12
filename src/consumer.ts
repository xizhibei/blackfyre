import { EventEmitter } from 'events';
import { setInterval, clearInterval } from 'timers';

import * as _ from 'lodash';
import * as amqp from 'amqplib';
import * as debug from 'debug';
import * as Bluebird from 'bluebird';

import {
  Task,
  TaskMeta,
  TaskState,
} from './common';

import { BackendType, Backend } from './backends/interface';
import { MongodbBackend, MongodbBackendOptions } from './backends/mongodb';
import { BrokerType, Broker } from './brokers/interface';
import { AMQPBroker, AMQPBrokerOptions } from './brokers/amqp';
import { registerEvent } from './helper';

Promise = Bluebird as any;

const log = debug('blackfyre:consumer');
const eventLog = debug('blackfyre:consumer:event');

export interface Logger {
  info: (obj: object) => void;
  error: (obj: object) => void;
}

export interface ProcessFunc {
  (data: any, task?: Task): Promise<any>;
}

export interface ConsumerOptions {
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

export class Consumer extends EventEmitter {
  private options: ConsumerOptions;

  private backend: Backend;
  private broker: Broker;

  public constructor(config: ConsumerOptions = {}) {
    super();

    const defaultOptions: ConsumerOptions = {
      logger: {
        info: _.noop,
        error: _.noop,
      },
      processWrap: null,
      globalConcurrency: 256,
      backendType: BackendType.MongoDB,
      backendOptions: null,
      brokerType: BrokerType.AMQP,
      brokerOptions: null,
    };
    this.options = Object.assign({}, defaultOptions, config);

    if (this.options.backendType === BackendType.MongoDB) {
      this.backend = new MongodbBackend(this.options.backendOptions);
    }

    if (this.options.brokerType === BrokerType.AMQP) {
      this.broker = new AMQPBroker(this.options.brokerOptions);
    }

    registerEvent(['error', 'ready', 'close'], this.broker, this);
    registerEvent(['error', 'close'], this.backend, this);
  }

  public async checkHealth(): Promise<any> {
    return Promise.props({
      backend: this.backend.checkHealth(),
      broker: this.broker.checkHealth(),
    });
  }

  private logSuccess(task: Task, startTime: number, result: object): void {
    this.options.logger.info({
      task,
      duration: Date.now() - startTime,
      result: JSON.stringify(result),
    });
  }

  private logFail(task: Task, startTime: number, e: Error): void {
    this.options.logger.error({
      task,
      duration: Date.now() - startTime,
      error: e,
      errorStacks: e.stack && e.stack.split('\n'),
    });
  }

  public async registerTask(taskMeta: TaskMeta, processFunc: ProcessFunc): Promise<void> {
    const that = this;

    taskMeta.concurrency = taskMeta.concurrency || this.options.globalConcurrency;

    if (this.options.processWrap) {
      processFunc = this.options.processWrap(taskMeta.name, processFunc);
    }

    async function processFuncWrap(body: any, task: Task) {
      await that.backend.setTaskStateReceived(task);

      const startTime = Date.now();

      if (that.options.preProcess) {
        that.options.preProcess.bind(this);
        that.options.preProcess(task);
      }

      try {
        await that.backend.setTaskStateStarted(task);
        const result = await processFunc(task.body, task);
        await that.backend.setTaskStateSucceed(task, result);

        if (that.options.postProcess) {
          that.options.postProcess.bind(this);
          that.options.postProcess(task, TaskState.SUCCEED, result);
        }

        that.logSuccess(task, startTime, result);
      } catch (e) {
        if (!e.noRetry || task.retryCount === task.maxRetry) {
          e.state = TaskState.FAILED;
          await that.backend.setTaskStateFailed(task, e);
        } else {
          e.state = TaskState.RETRYING;
          await that.backend.setTaskStateRetrying(task, e);
        }

        if (that.options.postProcess) {
          that.options.postProcess.bind(this);
          that.options.postProcess(task, e.state, e);
        }

        that.logFail(task, startTime, e);
        throw e;
      }
    }

    await this.broker.registerTask(taskMeta, processFuncWrap);
  }

  public async close(): Promise<any> {
    return Promise.props({
      broker: this.broker.close(),
      backend: this.backend.close(),
    });
  }
}
