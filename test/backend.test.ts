import test from 'ava';

import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { Producer, Consumer, Task, TaskMeta, TaskState } from '../src/index';
import { waitUtilDone } from './utils';

import { BackendType, Backend } from '../src/backends/interface';
import { MongodbBackend, MongodbBackendOptions } from '../src/backends/mongodb';

Promise = Bluebird as any;

test('#backend mongodb', async (t) => {
  const backend = new MongodbBackend();

  const task = <Task>{
    id: uuid.v4(),
    name: 'test-backend',
    body: 'test-body',
  };
  await backend.setTaskStatePending(task);
  t.is((await backend.getTask(task.id)).state, TaskState.PENDING);

  await backend.setTaskStateReceived(task);
  t.is((await backend.getTask(task.id)).state, TaskState.RECEIVED);

  await backend.setTaskStateStarted(task);
  t.is((await backend.getTask(task.id)).state, TaskState.STARTED);

  await backend.setTaskStateSucceed(task, 'haha');
  const succeedTask = await backend.getTask(task.id);
  t.is(succeedTask.state, TaskState.SUCCEED);
  t.is(succeedTask.result, 'haha');

  await backend.setTaskStateRetrying(task, new Error('test'));
  const retryingTask = await backend.getTask(task.id);
  t.is(retryingTask.state, TaskState.RETRYING);
  t.is(retryingTask.error, 'Error: test');
  t.true(retryingTask.errorStack.length > 0);


  await backend.setTaskStateFailed(task, new Error('test'));
  const failedTask = await backend.getTask(task.id);
  t.is(failedTask.state, TaskState.FAILED);
  t.is(failedTask.error, 'Error: test');
  t.true(failedTask.errorStack.length > 0);

  await backend.removeTask(task.id);
  t.is(await backend.getTask(task.id), null);
});
