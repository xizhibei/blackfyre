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

/**
 * The task retry options
 * Algorithm: initDelayMs + delayMs x retryStrategy(retryCount)
 */
export interface RetryOptions {

  /**
   *  Max retry for task failure, default is 0, means no retry
   */
  maxRetry?: number;

  /**
   *  Init delay for retry, in millonseconds
   */
  initDelayMs?: number;

  /**
   *  Delay factor for retry, in millonseconds
   */
  delayMs?: number;

  /**
   *  Retry stragegy, the default is FIBONACCI
   */
  retryStrategy?: RetryStrategy;
}

export interface Task extends RetryOptions {
  /**
   * Task version, fixed: `1.0`
   */
  v: string,

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
   *  Task priority for the priority queue
   */
  priority?: number;

  /**
   *  Retry count, just for internal use
   */
  retryCount?: number;
}

export enum RetryStrategy {
  FIBONACCI = 'fib',
  LINEAR = 'lne',
  EXPONENTIAL = 'exp',
}

export enum TaskState {
  /**
   * The task just sent
   */
  PENDING = 'pending',

  /**
   * The task received by consumer
   */
  RECEIVED = 'received',

  /**
   * Task is started
   */
  STARTED = 'started',

  /**
   * Task is being retried
   */
  RETRYING = 'retrying',

  /**
   * The task is succeed
   */
  SUCCEED = 'succeed',

  /**
   * The task is failed, and won't be retried
   */
  FAILED = 'failed',
}
