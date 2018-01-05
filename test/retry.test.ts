import test from 'ava';

import { Producer, Consumer, Task, RetryStrategy, TaskDefination } from '../src/index';
import * as Bluebird from 'bluebird';
import * as _ from 'lodash';

Promise = Bluebird as any;


test('#retry task fib', async t => {
  const taskName = 'test-retry-fib'

  const maxRetry = 5;
  t.plan(maxRetry + 1);

  const consumer = new Consumer();
  await consumer.createConnection();
  consumer.register(<TaskDefination>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.true(true);
    throw new Error('test');
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      initDelayMs: 10,
      maxRetry,
      retryStrategy: RetryStrategy.FIBONACCI,
    });

  await Promise.delay(2000);
});

test('#retry task exp', async t => {
  const taskName = 'test-retry-exp';

  const maxRetry = 5;
  t.plan(maxRetry + 1);

  const consumer = new Consumer();
  await consumer.createConnection();
  consumer.register(<TaskDefination>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.true(true);
    throw new Error('test');
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      initDelayMs: 10,
      maxRetry,
      retryStrategy: RetryStrategy.EXPONENTIAL,
    });

  await Promise.delay(2000);
});

test('#retry task lne', async t => {
  const taskName = 'test-retry-lne';

  const maxRetry = 5;
  t.plan(maxRetry + 1);

  const consumer = new Consumer();
  await consumer.createConnection();
  consumer.register(<TaskDefination>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.true(true);
    throw new Error('test');
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      initDelayMs: 10,
      maxRetry,
      retryStrategy: RetryStrategy.LINEAR,
    });

  await Promise.delay(2000);
});
