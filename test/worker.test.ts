import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import { Producer, Consumer, Task, TaskMeta } from '../src/index';
import { waitUtilDone } from './utils';

Promise = Bluebird as any;

test('#worker health check', async (t) => {
  const taskName = 'test-health-check';
  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(1);

  consumer.on('ready', async () => {
    const result = await consumer.checkHealth();
    const [r] = result.broker.consumer;
    t.is(r.queue, 'test-health-check_queue');
    t.is(r.consumerCount, 1);
    doneOne();
  });

  await promise;
});

test('#worker wait producer to be ready', async (t) => {
  const taskName = 'test-producer-ready';

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  const producer = await new Producer();

  producer.on('ready', () => {
    producer.createTask(<Task>{
      name: taskName,
      body: { test: 'test' }
    });
  });

  producer.createTask(<Task>{
    name: taskName,
    body: { test: 'test' }
  });

  await promise;
});

test('#consumer register race', async (t) => {
  const taskName = 'test-producer-ready';

  t.plan(5);
  const { promise, doneOne } = waitUtilDone(5);

  const consumer = new Consumer();
  consumer.on('ready', (taskName) => {
    t.true(true);
    doneOne();
  });

  await Promise.map(_.times(5), async (i) => {
    return consumer.registerTask(<TaskMeta>{
      name: taskName + i,
      concurrency: 20,
    }, async (data) => {
    });
  });

  await promise;
});

test('#normal task', async (t) => {
  const taskName = 'test-normal';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
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
  consumer.registerTask(<TaskMeta>{
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
  consumer.registerTask(<TaskMeta>{
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

