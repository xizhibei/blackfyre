# Blackfyre

Distributed asynchronous task queue/job queue

### Warning
- *In beta test*
- *Not production ready yet.*

## Installation
```bash
npm install blackfyre
```

## Features

- Distribution of parallel work load
- Real time operation
- Delayed job
- Priority job
- Task retry with different strategies
- Process function with pre & post hook
- APM wrap function for easy

## TODO

- Backend store
- Rate limit
- Event handler & emiter

## Overview

### Using newrelic in process wrap
```ts
import * as newrelic from 'newrelic';

const consumer = new Consumer(<ConsumerConfig>{
    processWrap(processFunc): ProcessFunc {
        return newrelic.startBackgroundTransaction(taskName, async (data: any, task: Task) => {
            try {
                const result = await processFunc(data, task);
                return result;
            } catch (e) {
                newrelic.noticeError(e);
                throw e;
            }
        });
    }
});
```

### Using prom client moniter
```ts
const summary = new promClient.Summary({
    name: 'job_summary',
    help: 'Summary of jobs',
    percentiles: [0.5, 0.75, 0.9, 0.99, 0.999],
    labelNames: ['state', 'taskName'],
});

const consumer = new Consumer(<ConsumerConfig>{
    preProcess(task: Task) {
      // Yes, `this` binded to the process warp function,
      // so you may share some vars with the `postPostprocess`
      this.endTimer = summary.startTimer({ taskName: task.name });
    },
    postProcess(task: Task, state: TaskState, errorOrResult: any) {
      this.endTimer({ state });
    },
});
```

### Tesing

```ts
  const producer = new Producer(<ProducerConfig>{
    isTestMode: true,
  });

  await producer
    .createTask(<Task>{
      name: 'example-task',
      body: { test: 'test' }
    });

console.log(producer.createdTasks[0].body);

/**
 * The output: { test: 'test' }
 */

```

## License
MIT
