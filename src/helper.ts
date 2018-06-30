import * as _ from 'lodash';
import { RetryStrategy } from './common';
import { Options } from 'amqplib';
import { Task } from './index';
import { PROTOCAL_VERSION } from './constant';

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
  [RetryStrategy.FIBONACCI]: (retryCount: number) => {
    return fibonacci(retryCount);
  },
  [RetryStrategy.EXPONENTIAL]: (retryCount: number) => {
    return Math.pow(2, retryCount);
  },
  [RetryStrategy.LINEAR]: (retryCount: number) => {
    return retryCount + 1;
  },
};

export function getRetryDelayMs(task: Task): number {
  let strategy = strategies[task.retryStrategy];
  /* istanbul ignore if */
  if (!strategy) {
    // Fallback to FIBONACCI
    strategy = strategies[RetryStrategy.FIBONACCI];
  }
  return task.initDelayMs + task.delayMs * strategy(task.retryCount);
}

export function registerEvent(events, source, target) {
  events.forEach(e => {
    source.on(e, (...args) => target.emit(e, ...args));
  });
}

export function checkVersion(version: string = '1.0') {
  const [, major, minor] = version.match(/^(\d+).(\d+)$/);
  const [, currentMajor, currentMinor] = PROTOCAL_VERSION.match(/^(\d+).(\d+)$/);

  if (major !== currentMajor) {
    throw new Error(`protocal not compatible: ${version}, current: ${PROTOCAL_VERSION}`);
  }

  return {
    major,
    minor,
    currentMajor,
    currentMinor,
  };
}
