import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import { Producer, Consumer, Task, TaskMeta } from '../src/index';
import { waitUtilDone } from './utils';

Promise = Bluebird as any;

test('#worker health check', async (t) => {
  const taskName = 'test-health-check';
  const consumer = new Consumer();

  await consumer.createConnection();
  consumer.register(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  await Promise.delay(100);

  const result = await consumer.checkHealth();
  t.is(result.queue, 'test-health-check_queue');
  t.is(result.consumerCount, 1);
});

test('#worker wait producer to be ready', async (t) => {
  const taskName = 'test-producer-ready';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();

  await consumer.createConnection();
  consumer.register(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  const producer = await new Producer();
  await Promise.delay(100);
  producer.createTask(<Task>{
    name: taskName,
    body: { test: 'test' }
  });
  await promise;
});

test('#normal task', async (t) => {
  const taskName = 'test-normal';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();

  await consumer.createConnection();
  consumer.register(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' }
    });

  await promise;
});

test('#priority task', async (t) => {
  const taskName = 'test-priority';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();
  await consumer.createConnection();
  consumer.register(<TaskMeta>{
    name: taskName,
    concurrency: 20,
    maxPriority: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      priority: 10,
    });

  await promise;
});

test('#delay task', async t => {
  const taskName = 'test-delay';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();
  await consumer.createConnection();
  consumer.register(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  await (new Producer())
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
      eta: Date.now() + 500,
    });

  await promise;
});

