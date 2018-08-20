import test from 'ava';

import * as Bluebird from 'bluebird';
import { Producer, Consumer, Task, TaskMeta } from '../src/index';
import { waitUtilDone } from './utils';
import { PROTOCOL_VERSION } from '../src/constant';

Promise = Bluebird as any;

test('#protocol', async t => {
  const taskName = `test-protocol`;

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data, task) => {
    t.is(task.v, PROTOCOL_VERSION);
    doneOne();
  });

  consumer.on('ready', async () => {
    await (new Producer())
      .createTask(<Task>{
        name: taskName,
        body: { test: 'test' },
        initDelayMs: 50,
      });
  });

  await promise;
});
