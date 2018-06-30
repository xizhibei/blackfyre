import test from 'ava';

import * as Bluebird from 'bluebird';
import { Producer, Consumer, Task, TaskMeta } from '../src/index';
import { waitUtilDone } from './utils';
import { PROTOCAL_VERSION } from '../src/constant';

Promise = Bluebird as any;

test('#protocal', async t => {
  const taskName = `test-protocal`;

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer();

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data, task) => {
    t.is(task.v, PROTOCAL_VERSION);
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
