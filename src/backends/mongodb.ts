import * as _ from 'lodash';
import * as mongodb from 'mongodb';

import { TaskState } from '../common';
import { Backend, TaskStore } from './interface';
import { registerEvent } from '../helper';

export interface MongodbBackendOptions {
  url?: string;
  dbName?: string;
  collectionName?: string;
  mongoClientOptions?: mongodb.MongoClientOptions;
  resultsExpireIn?: number;
}

export class MongodbBackend extends Backend {
  private options: MongodbBackendOptions;
  private client: mongodb.MongoClient;

  constructor(options: MongodbBackendOptions = {}) {
    super();

    this.options = _.extend({
      url: 'mongodb://localhost:27017',
      dbName: 'blackfyre',
      collectionName: 'tasks',
      mongoClientOptions: null,
      resultsExpireIn: 3600 * 24,
    }, options);
  }

  private async getCollection(): Promise<mongodb.Collection> {
    if (!this.client) {
      this.client = await mongodb.MongoClient.connect(this.options.url, this.options.mongoClientOptions);
      registerEvent(['error', 'close'], this.client, this);
    }
    const coll = this.client.db(this.options.dbName).collection<TaskState>(this.options.collectionName);
    await coll.createIndex({
      state: 1,
    }, {
        background: true,
        expireAfterSeconds: this.options.resultsExpireIn,
      });
    return coll;
  }

  async updateState(task: TaskStore): Promise<void> {
    const coll = await this.getCollection();

    task.created = task.created || new Date();
    task.updated = new Date();

    await coll.updateOne({
      _id: task.id,
    }, {
        $set: task,
      }, <mongodb.ReplaceOneOptions>{
        upsert: true,
      });
  }

  async setTaskStatePending(task: TaskStore): Promise<void> {
    task.state = TaskState.PENDING;
    return this.updateState(task);
  }

  async setTaskStateReceived(task: TaskStore): Promise<void> {
    task.state = TaskState.RECEIVED;
    return this.updateState(task);
  }

  async setTaskStateStarted(task: TaskStore): Promise<void> {
    task.state = TaskState.STARTED;
    return this.updateState(task);
  }

  async setTaskStateRetrying(task: TaskStore, err: Error): Promise<void> {
    task.state = TaskState.RETRYING;
    task.error = err.toString();
    task.errorStack = err.stack.split('\n');
    return this.updateState(task);
  }

  async setTaskStateSucceed(task: TaskStore, result: any): Promise<void> {
    task.state = TaskState.SUCCEED;
    task.result = result;
    return this.updateState(task);
  }

  async setTaskStateFailed(task: TaskStore, err: Error): Promise<void> {
    task.state = TaskState.FAILED;
    task.error = err.toString();
    task.errorStack = err.stack.split('\n');
    return this.updateState(task);
  }

  async getTask(taskID: string): Promise<TaskStore> {
    const coll = await this.getCollection();
    return coll.findOne<TaskStore>({
      _id: taskID,
    });
  }

  async removeTask(taskID: string): Promise<void> {
    const coll = await this.getCollection();
    await coll.remove({
      _id: taskID,
    });
  }

  async checkHealth(): Promise<any> {
    if (!this.client) return {};
    return this.client.db(this.options.dbName).admin().ping();
  }

  async close(): Promise<any> {
    if (this.client) {
      this.client.close();
    }
  }
}
