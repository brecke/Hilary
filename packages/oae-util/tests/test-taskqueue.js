/*
 * Copyright 2013 Apereo Foundation (AF) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://opensource.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import assert from 'assert';

import * as TaskQueue from 'oae-util/lib/taskqueue';

describe('TaskQueue', () => {
  describe('#bind()', () => {
    /**
     * Verify that a bound worker starts receiving tasks.
     */
    it('verify a bound worker can receive a task', () => {
      const testQueue = 'testQueue-' + new Date().getTime();
      TaskQueue.bind(
        testQueue,
        (data, taskCallback) => {
          return new Promise((resolve, reject) => {
            assert.ok(data);
            assert.strictEqual(data.activity, 'you stink!');

            resolve(taskCallback());
          });
        },
        null
      ).then(() => {
        TaskQueue.submit(testQueue, { activity: 'you stink!' });
      });
    });

    /**
     * Verify that binding a worker when there is already one doesn't invoke an error
     */
    it("verify binding an existing queue doesn't invoke an error", () => {
      const testQueue = 'testQueue-' + new Date().getTime();
      TaskQueue.bind(
        testQueue,
        () => {
          return new Promise(resolve => resolve());
        },
        null
      ).then(() => {
        // Simply make sure the callback gets executed and we can carry on
        return TaskQueue.bind(
          testQueue,
          () => {
            return new Promise(resolve => resolve());
          },
          null
        );
      });
    });

    /**
     * Verify that processing continues safely when an exception is thrown from within a worker.
     */
    it('verify an exception is caught when thrown from a task handler', callback => {
      const testQueue = 'testQueue-' + new Date().getTime();
      TaskQueue.bind(
        testQueue,
        data => {
          throw new Error('Hard-coded exception to verify application remains stable.');
        },
        null
      )
        .then(() => {
          return TaskQueue.submit(testQueue, { activity: 'blah' });
        })
        .then(() => {
          // Simply make sure tests continue normally when the exception is thrown
          callback();
        });
    });
  });

  describe('#unbind()', () => {
    /**
     * Verify that unbinding a non-existing worker does not invoke an error
     */
    it('verify unbind non-existing queue is safe', () => {
      const testQueue = 'testQueue-' + new Date().getTime();
      // Simply make sure there is no exception
      Promise.resolve(TaskQueue.unbind(testQueue));
    });

    /**
     * Verify a worker can be bound, then unbound, the rebound and still receive tasks
     */
    it('verify unbinding and then rebinding', () => {
      const testQueue = 'testQueue-' + new Date().getTime();
      TaskQueue.bind(
        testQueue,
        () => {
          // Dead end. if this is the effective method the test will hang and time out
        },
        null
      )
        .then(() => {
          // Now unbind it so we can re-bind with a valid handler
          return TaskQueue.unbind(testQueue);
        })
        .then(() => {
          return TaskQueue.bind(
            testQueue,
            (data, taskCallback) => {
              assert.ok(data);
              assert.strictEqual(data.activity, 'you stink!');
              return new Promise((resolve, reject) => {
                resolve(taskCallback);
              });
            },
            null
          );
        })
        .then(() => {
          return TaskQueue.submit(testQueue, { activity: 'you stink!' });
        });
    });
  });
});
