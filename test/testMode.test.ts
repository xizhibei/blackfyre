import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';

import {
  Producer,
  Consumer,
  Task,
  RetryStrategy,
  TaskMeta,
  ProducerConfig,
} from '../src/index';

Promise = Bluebird as any;

test('#test mode', async (t) => {
  const producer = new Producer(<ProducerConfig>{
    isTestMode: true,
  });

  await producer
    .createTask(<Task>{
      name: 'example-task',
      body: { test: 'test' }
    });

  t.is(producer.createdTasks.length, 1);
  const task = producer.createdTasks[0];
  t.is(task.body.test, 'test');
});
