'use strict';

const Promise = require('bluebird');
const _ = require('lodash');
const blackfyre = require('../');

const producer = new blackfyre.Producer({
  backendType: blackfyre.BackendType.MongoDB,
  backendOptions: {
    url: 'mongodb://localhost',
    collectionName: 'tasks',
    dbName: 'blackfyre-example',
    resultsExpireIn: 600,
  },
  brokerType: blackfyre.BrokerType.AMQP,
  brokerOptions: {
    url: 'amqp://guest:guest@localhost',
    exchangeName: 'worker-exchange',
    queueSuffix: 'queue',
    queueOptions: {
      expires: 5000,
    },
  },
  globalRetryOptions: {
    delayMs: 100,
    initDelayMs: 100,
    maxRetry: 3,
  },
  isTestMode: false,
});

setInterval(async() => {
  console.log(`---- time: ${new Date()} ---`);
  console.log(JSON.stringify(await producer.checkHealth()));
}, 1000);

Promise.map(_.range(1000), async() => {
    return producer
      .createTask({
        name: 'example',
        body: {
          delay: _.random(100, 300),
          retry: _.random() > 0.98,
        },
      });
  }, {
    concurrency: 200,
  })
  .catch(console.log)
  .finally(() => process.exit());
