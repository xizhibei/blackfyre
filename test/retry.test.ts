import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import { Producer, Consumer, Task, RetryStrategy, TaskMeta } from '../src/index';
import { waitUtilDone } from './utils';

Promise = Bluebird as any;

async function testRetry(t, retryStrategy: RetryStrategy) {
  const taskName = `test-retry-${retryStrategy}`;

  const maxRetry = 5;
  t.plan(maxRetry + 1);
  const {promise, doneOne} = waitUtilDone(maxRetry + 1);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.true(true);
    doneOne();
    throw new Error('test');
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      initDelayMs: 50,
      maxRetry,
      retryStrategy,
    });

  await promise;
}

test('#retry task fib', async t => {
  await testRetry(t, RetryStrategy.FIBONACCI);
});

test('#retry task exp', async t => {
  await testRetry(t, RetryStrategy.EXPONENTIAL);
});

test('#retry task lne', async t => {
  await testRetry(t, RetryStrategy.LINEAR);
});
