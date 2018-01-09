import { Options } from 'amqplib';

export interface TaskMeta {
  /**
   * Task name, will use as the routing key
   */
  name: string;

  /**
   * Task concurrency, setting as the prefetch size
   */
  concurrency: number;

  /**
   * Priority queue task, set as the queue max priority
   * Note: the priority in task can not be greater than this value
   */
  maxPriority?: number;
}

export interface Task {
  /**
   * Task id, default: `uuid.v4()`
   */
  id: string;

  /**
   * Task name, will use as the routing key
   */
  name: string;

  /**
   * Task body
   */
  body: any;

  /**
   *  ETA for delay task, timestamp, eg. `Date.now() + 5000`
   */
  eta?: number;

  /**
   *  Retry count, just for internal use
   */
  retryCount?: number;
  /**
   *  Max retry for task failure, default is 0, means no retry
   */
  maxRetry?: number;

  /**
   *  Init delay for retry, in millonseconds
   */
  initDelayMs?: number;

  /**
   *  Task priority for the priority queue
   */
  priority?: number;

  /**
   *  Retry stragegy, the default is FIBONACCI
   */
  retryStrategy?: RetryStrategy;
}

export enum RetryStrategy {
  FIBONACCI = 'fib',
  LINEAR = 'lne',
  EXPONENTIAL = 'exp',
}

export enum TaskState {
  /**
   * The task is succeed
   */
  SUCCEED = 'succeed',

  /**
   * The task is failed, and will be retried
   */
  FAILED = 'failed',

  /**
   * The task is permanent failed, won't be retried
   */
  PERMANENT_FAILED = 'permanent-failed',
}
