import { EventEmitter } from 'events';
import { Task, TaskState } from '../common';

export interface TaskStore extends Task {
  state?: TaskState;
  error?: string;
  errorStack?: string[];
  result?: any;
  created?: Date;
  updated?: Date;
}

export abstract class Backend extends EventEmitter {
  abstract setTaskStatePending(task: TaskStore): Promise<void>;
  abstract setTaskStateReceived(task: TaskStore): Promise<void>;
  abstract setTaskStateStarted(task: TaskStore): Promise<void>;
  abstract setTaskStateRetrying(task: TaskStore, err: Error): Promise<void>;
  abstract setTaskStateSucceed(task: TaskStore, result: any): Promise<void>;
  abstract setTaskStateFailed(task: TaskStore, err: Error): Promise<void>;

  abstract getTask(taskID: string): Promise<TaskStore>;
  abstract removeTask(taskID: string): Promise<void>;

  abstract checkHealth(): Promise<any>;
  abstract close(): Promise<any>;
}

export enum BackendType {
  MongoDB = 'mongodb',
  None = 'none',
}
