import test from 'ava';
import * as Bluebird from 'bluebird';
import * as promClient from 'prom-client';

import {
  Producer,
  Consumer,
  Task,
  ConsumerOptions,
  TaskState,
  ProcessFunc,
  TaskMeta,
} from '../src/index';

import { waitUtilDone } from './utils';

Promise = Bluebird as any;

test('#task preProcess', async (t) => {
  const taskName = 'hook-test-1';

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer(<ConsumerOptions>{
    preProcess(task: Task): void {
      t.true(true);
    },
  });

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

test('#task postProcess: success', async (t) => {
  const taskName = 'hook-test-2';

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer(<ConsumerOptions>{
    postProcess(task: Task, state: TaskState, errorOrResult: any): void {
      t.is(state, TaskState.SUCCEED);
      t.is(errorOrResult, 'test');
      doneOne();
    },
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    return 'test';
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

test('#task postProcess: fail', async (t) => {
  const taskName = 'hook-test-3';

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(1);

  const consumer = new Consumer(<ConsumerOptions>{
    postProcess(task: Task, state: TaskState, errorOrResult: any): void {
      t.is(state, TaskState.FAILED);
      t.is(errorOrResult.message, 'test');
      doneOne();
    },
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
    t.is(data.test, 'test');
    throw new Error('test');
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

test('#task hook for prom client', async (t) => {
  const taskName = 'hook-test-prom-client';

  const summary = new promClient.Summary({
    name: 'job_summary',
    help: 'Summary of jobs',
    percentiles: [0.5, 0.75, 0.9, 0.99, 0.999],
    labelNames: ['state', 'taskName'],
  });

  const consumer = new Consumer(<ConsumerOptions>{
    preProcess(task: Task) {
      this.endTimer = summary.startTimer({ taskName: task.name });
    },
    postProcess(task: Task, state: TaskState, errorOrResult: any) {
      this.endTimer({ state });
    },
  });

  const { promise, doneOne } = waitUtilDone(1);

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
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

  await Promise.delay(100);

  const metric = promClient.register.getSingleMetricAsString('job_summary');
  t.regex(metric, /taskName="hook-test-prom-client",state="succeed"/i);
});

test('#task apm wrap', async (t) => {
  const taskName = 'hook-test-4';
  t.plan(1);

  const consumer = new Consumer(<ConsumerOptions>{
    processWrap(taskName: string, func: ProcessFunc): ProcessFunc {
      return func;
    },
  });

  const { promise, doneOne } = waitUtilDone(1);

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
