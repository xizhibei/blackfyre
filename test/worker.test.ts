import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import { Producer, Consumer, Task, TaskMeta, ProducerOptions } from '../src/index';
import { waitUtilDone } from './utils';
import { ConsumerOptions } from '../src/consumer';
import { BackendType } from '../src/backends/interface';

Promise = Bluebird as any;

test('#consumer health check', async (t) => {
  const taskName = 'test-consumer-health-check';
  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(1);

  consumer.on('ready', async () => {
    await Promise.delay(100);
    const result = await consumer.checkHealth();
    const [r] = result.broker.consumer;
    t.is(r.queue, `${taskName}_queue`);
    t.is(r.consumerCount, 1);
    doneOne();
  });

  await promise;
});

test('#producer health check', async (t) => {
  const taskName = 'test-producer-health-check';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const producer = await new Producer();

  producer.on('ready', async () => {
    await Promise.delay(100);
    const result = await producer.checkHealth();
    t.is(_.size(result.broker.producer), 0);
    doneOne();
  });

  await producer
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' }
    });

  await promise;
});

test('#producer register race', async (t) => {
  const taskName = 'test-producer-race';

  t.plan(5);
  const { promise, doneOne } = waitUtilDone(5);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  consumer.on('ready', async () => {
    const producer = await new Producer();
    await Promise.map(_.times(5), async (i) => {
      await producer
        .createTask(<Task>{
          name: taskName,
          body: { test: 'test' }
        });
    });
  });

  await promise;
});

test('#consumer register race', async (t) => {
  const taskName = 'test-producer-race';

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

  consumer.on('ready', async () => {
    await (new Producer())
      .createTask(<Task>{
        name: taskName,
        body: { test: 'test' }
      });
  });

  await promise;
});

test('#normal task with none backend', async (t) => {
  const taskName = 'test-none-backend';

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer(<ConsumerOptions>{
    backendType: BackendType.None,
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    doneOne();
  });

  consumer.on('ready', async () => {
    await (new Producer(<ProducerOptions>{
      backendType: BackendType.None,
    }))
      .createTask(<Task>{
        name: taskName,
        body: { test: 'test' }
      });
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

  consumer.on('ready', async () => {
    await (new Producer())
      .createTask(<Task>{
        name: taskName,
        body: { test: 'test' },
        priority: 10,
      });
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

  consumer.on('ready', async () => {
    await (new Producer())
      .createTask(<Task>{
        name: taskName,
        body: { test: 'test' },
        eta: Date.now() + 500,
      });
  });

  await promise;
});

