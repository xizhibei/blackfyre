import * as _ from 'lodash';
import { RetryStrategy } from './common';
import { Options } from 'amqplib';
import { Task } from './index';

function _fibonacci(): (() => number) {
  let a: number = 0;
  let b: number = 1;
  return (): number => {
    [a, b] = [b, a + b];
    return a;
  };
}

export function fibonacci(times: number): number {
  const fib = _fibonacci();
  let num: number = fib();
  _.times(times, () => {
    num = fib();
  });
  return num;
}

export function getDelayQueue(delayMs: number, exchange: string, routingKey: string) {
  return `delay.${delayMs}.${exchange}.${routingKey}`;
}

export function getDelayQueueOptions(delayMs: number, exchange: string, routingKey: string): Options.AssertQueue {
  return {
    durable: true,
    deadLetterExchange: exchange,
    deadLetterRoutingKey: routingKey,
    messageTtl: delayMs,
    expires: delayMs * 2,
  };
}

const strategies = {
  [RetryStrategy.FIBONACCI]: (task: Task) => {
    return task.initDelayMs * fibonacci(task.retryCount);
  },
  [RetryStrategy.EXPONENTIAL]: (task: Task) => {
    return task.initDelayMs * Math.pow(2, task.retryCount);
  },
  [RetryStrategy.LINEAR]: (task: Task) => {
    return task.initDelayMs * (task.retryCount + 1);
  },
};

export function getRetryDelayMs(task: Task): number {
  let strategy = strategies[task.retryStrategy];
  if (!strategy) {
    // Fallback to FIBONACCI
    strategy = strategies[RetryStrategy.FIBONACCI];
  }
  return strategy(task);
}

