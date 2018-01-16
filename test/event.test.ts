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

test('#events producer close', async (t) => {
  const taskName = 'event-test-producer-close';

  const producer = new Producer();

  t.plan(4);
  const { promise, doneOne } = waitUtilDone(4);

  // mongodb close, amqp close, channel close
  producer.on('close', () => {
    t.true(true);
    doneOne();
  });

  producer.on('ready', async () => {
    await Promise.delay(100);
    await producer.close();
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
  const taskName = 'event-test-producer-error';

  const producerDefault = new Producer(<ConsumerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'direct',
    },
  });

  t.plan(2);
  const { promise, doneOne } = waitUtilDone(2);

  producerDefault.on('ready', () => {
    const producer = new Producer(<ProducerOptions>{
      brokerOptions: <AMQPBrokerOptions>{
        exchangeType: 'topic',
      },
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
  });

  await producerDefault
    .createTask(<Task>{
      name: taskName,
      body: { test: 'test' },
    });

  await promise;
});

test('#events consumer close', async (t) => {
  const taskName = 'event-test-consumer-close';

  const consumer = new Consumer();

  t.plan(3);
  const { promise, doneOne } = waitUtilDone(2);

  // amqp close, channel close, no task run so no mongo connected
  consumer.on('close', () => {
    t.true(true);
    doneOne();
  });

  consumer.on('ready', async () => {
    await Promise.delay(100);
    await consumer.close();
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
  const taskName = 'event-test-consumer-error';

  const consumerDefault = new Consumer(<ConsumerOptions>{
    brokerOptions: <AMQPBrokerOptions>{
      exchangeType: 'direct',
    },
  });

  t.plan(1);
  const { promise, doneOne } = waitUtilDone(1);

  consumerDefault.on('ready', () => {
    const consumer = new Consumer(<ConsumerOptions>{
      brokerOptions: <AMQPBrokerOptions>{
        exchangeType: 'topic',
      },
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
  });

  consumerDefault.registerTask(<TaskMeta>{
    name: taskName,
    concurrency: 20,
  }, async (data) => {
  });

  await promise;
});

