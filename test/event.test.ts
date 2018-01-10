import test from 'ava';
import * as Bluebird from 'bluebird';
import * as promClient from 'prom-client';
import * as amqp from 'amqplib';

import {
  Producer,
  Consumer,
  Task,
  ConsumerConfig,
  TaskState,
  ProcessFunc,
  TaskMeta,
  ProducerConfig,
} from '../src/index';

import { waitUtilDone } from './utils';

Promise = Bluebird as any;

class NewConsumer extends Consumer {
  public async emitConnectionError() {
    this.connection.emit('error', new Error('test'));
  }
}

class NewProducer extends Producer {
  public async emitConnectionError() {
    this.connection.emit('error', new Error('test'));
  }
}

test('#events producer close', async (t) => {
  const taskName = 'event-test-producer';

  const producer = new NewProducer();

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(3);
  producer.on('channel-close', () => {
    t.true(true);
    doneOne();
  });

  producer.on('conn-close', () => {
    t.true(true);
    doneOne();
  });

  producer.on('ready', async () => {
    t.true(true);
    await producer.closeChannel();
    await producer.closeConnection();
    doneOne();
  });

  await producer
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  await promise;
});

test('#events producer channel error', async (t) => {
  const taskName = 'event-test-producer';

  const producerDefault = new NewProducer(<ConsumerConfig>{
    exchangeType: 'direct',
  });
  await producerDefault
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  const producer = new NewProducer(<ProducerConfig>{
    exchangeType: 'topic',
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  producer.on('channel-error', () => {
    t.true(true);
    doneOne();
  });

  producer.on('error', () => {
    t.true(true);
    doneOne();
  });

  producer
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  await promise;
});

test('#events producer connection error', async (t) => {
  const taskName = 'event-test-producer';

  const producer = new NewProducer();

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  producer.on('conn-error', () => {
    t.true(true);
    doneOne();
  });

  producer.on('ready', async () => {
    t.true(true);
    await producer.emitConnectionError();
    doneOne();
  });

  producer
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  await promise;
});

test('#events consumer close', async (t) => {
  const taskName = 'event-test-consumer';

  const consumer = new NewConsumer();

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(2);
  consumer.on('channel-close', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('conn-close', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('ready', async () => {
    t.true(true);

    await consumer.closeChannel(taskName);
    await consumer.closeConnection();
    doneOne();
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  await promise;
});

test('#events consumer channel error', async (t) => {
  const taskName = 'event-test-consumer';

  const consumerDefault = new NewConsumer(<ConsumerConfig>{
    exchangeType: 'direct',
  });

  consumerDefault.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  const consumer = new NewConsumer(<ConsumerConfig>{
    exchangeType: 'topic',
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  consumer.on('channel-error', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('error', () => {
    t.true(true);
    doneOne();
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  await promise;
});

test('#events consumer connection error', async (t) => {
  const taskName = 'event-test-consumer';

  const consumer = new NewConsumer();

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  consumer.on('conn-error', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('ready', async () => {
    t.true(true);

    await consumer.emitConnectionError();
    doneOne();
  });

  consumer.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  await promise;
});

