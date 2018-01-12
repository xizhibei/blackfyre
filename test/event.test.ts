import test from 'ava';
import * as Bluebird from 'bluebird';
import * as promClient from 'prom-client';
import * as amqp from 'amqplib';

import {
  Producer,
  Consumer,
  Task,
  ConsumerOptions,
  TaskState,
  ProcessFunc,
  TaskMeta,
  ProducerOptions,
} from '../src/index';

import { waitUtilDone } from './utils';
import { AMQPBrokerOptions } from '../src/brokers/amqp';

Promise = Bluebird as any;

test.skip('#events producer close', async (t) => {
  const taskName = 'event-test-producer';

  const producer = new Producer();

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);
  producer.on('close', () => {
    t.true(true);
    doneOne();
  });

  producer.on('error', () => {

  })

  producer.on('ready', async () => {
    try {
      await producer.close();
    } catch (e) {

    }
    t.true(true);
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

  const producerDefault = new Producer(<ConsumerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'direct',
    },
  });
  await producerDefault
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  const producer = new Producer(<ProducerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'topic',
    },
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

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

test('#events consumer close', async (t) => {
  const taskName = 'event-test-consumer';

  const consumer = new Consumer();

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(2);
  consumer.on('close', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('ready', async () => {
    try {
      await consumer.close();
    } catch (e) {

    }
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

test('#events consumer channel error', async (t) => {
  const taskName = 'event-test-consumer';

  const consumerDefault = new Consumer(<ConsumerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'direct',
    },
  });

  consumerDefault.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  const consumer = new Consumer(<ConsumerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'topic',
    },
  });

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

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

