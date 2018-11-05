import { EventEmitter } from 'events';

import * as amqp from 'amqplib';

import { Task, TaskMeta } from '../common';
import { ProcessFunc } from '../consumer';
import { TaskState } from '../index';

export interface TaskRegister {
  taskMeta: TaskMeta;
  processFunc: ProcessFunc;
  queueName: string;
  consumerTag?: string;
  channel?: amqp.Channel;
  channeling?: boolean;
}

export interface TaskRegisterMap {
  [taskName: string]: TaskRegister;
}

export abstract class Broker extends EventEmitter {
  abstract registerTask(taskMeta: TaskMeta, processFunc: ProcessFunc): Promise<void>;
  abstract publish(task: Task): Promise<any>;
  abstract close(): Promise<any>;
  abstract checkHealth(): Promise<any>;
}

export class BrokerError extends Error {
  state: TaskState;
  retryLeft: number;
}

export enum BrokerType {
  AMQP = 'amqp',
}
