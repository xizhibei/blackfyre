import * as _ from 'lodash';
import * as Bluebird from 'bluebird';

Promise = Bluebird as any;

export function waitUtilDone(times: number) {
  const dones = [];
  return {
    promise: Promise.map(_.range(times), async () => {
      return new Promise(resolve => {
        dones.push(resolve);
      });
    }),
    doneOne() {
      const done = dones.pop();
      if (done) done();
    },
  };
}
