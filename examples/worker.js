'use strict';

const Promise = require('bluebird');
const promClient = require('prom-client');

const summary = new promClient.Summary({
  name: 'job_summary',
  help: 'Summary of tasks',
  percentiles: [0.5, 0.75, 0.9, 0.99, 0.999],
  labelNames: ['state', 'taskName'],
});

const blackfyre = require('../');

const consumer = new blackfyre.Consumer({
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
  preProcess(task) {
    this.endTimer = summary.startTimer({
      taskName: task.name,
    });
  },
  postProcess(task, state, errorOrResult) {
    this.endTimer({
      state,
    });
  },
});

setInterval(async() => {
  console.log(`---- time: ${new Date()} ---`);
  console.log(JSON.stringify(await consumer.checkHealth()));
  console.log(promClient.register.getSingleMetricAsString('job_summary'));
}, 5000);

consumer.registerTask({
  name: 'example',
  concurrency: 200,
}, async(data) => {
  if (data.retry) {
    await Promise.delay(100);
    throw new Error('example');
  }
  return Promise.delay(data.delay || 500);
});
