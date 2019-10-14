/*
 * Copyright 2014 Apereo Foundation (AF) Licensed under the
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
import util from 'util';
import _ from 'underscore';
import ShortId from 'shortid';

import * as MQ from 'oae-util/lib/mq';

describe.skip('MQ', () => {
  /**
   * Verify that re-initializing the MQ doesn't invoke an error
   */
  it('verify re-initialization is safe', callback => {
    // Ensure processing continues, and that MQ is still stable with the tests that follow
    MQ.init({}, err => {
      assert.ok(!err);
      return callback();
    });
  });

  describe('#purge()', () => {
    /**
     * Test that verifies the parameters
     */
    it('verify parameter validation', callback => {
      const queueName = util.format('testQueue-%s', ShortId.generate());
      MQ.purge(queueName, err => {
        assert.strictEqual(err.code, 400);
        return callback();
      });
    });

    /**
     * Verify that a queue can be purged of its tasks.
     */
    it('verify a queue can be purged', callback => {
      let called = 0;
      const taskHandler = function(data, taskCallback) {
        called++;
        setTimeout(taskCallback, 2000);
      };

      const testQueue = 'testQueue-' + new Date().getTime();
      MQ.subscribe(testQueue, taskHandler, () => {
        // Submit a couple of tasks.
        for (let i = 0; i < 10; i++) {
          MQ.submitJSON(testQueue, { foo: 'bar' });
        }

        // Purge the queue.
        MQ.purge(testQueue, () => {
          // Because of the asynchronous nature of node/rabbitmq it's possible that a task gets delivered
          // before the purge command is processed.
          // That means we should have only handled at most 1 task.
          assert.ok(called <= 1);
          callback();
        });
      });
    });
  });

  describe('#purgeAll()', () => {
    /**
     * Verify that all known queues can be purged of its tasks.
     */
    it('verify all queues can be purged', callback => {
      const called = { a: 0, b: 0 };
      const taskHandler = function(data, taskCallback) {
        called[data.queue]++;
        setTimeout(taskCallback, 2000);
      };

      const testQueueA = 'testQueueA-' + new Date().getTime();
      const testQueueB = 'testQueueB-' + new Date().getTime();
      MQ.subscribe(testQueueA, taskHandler, () => {
        MQ.subscribe(testQueueB, taskHandler, () => {
          // Submit a couple of tasks.
          for (let i = 0; i < 10; i++) {
            MQ.submitJSON(testQueueA, { queue: 'a' });
            MQ.submitJSON(testQueueB, { queue: 'b' });
          }

          // Purge all the queues.
          MQ.purgeAll(() => {
            // Because of the asynchronous nature of node/rabbitmq it's possible that a task gets delivered
            // before the purge command is processed.
            // That means we should have only handled at most 1 task.
            assert.ok(called.a <= 10);
            assert.ok(called.b <= 10);
            callback();
          });
        });
      });
    });
  });

  describe('#submit()', () => {
    /**
     * Test that verifies the passed in parameters
     */
    it('verify parameter validation', callback => {
      const exchangeName = util.format('testExchange-%s', ShortId.generate());
      const queueName = util.format('testQueue-%s', ShortId.generate());
      const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
      const data = { text: 'The truth is out there' };

      // An exchange must be provided
      MQ.submit(null, routingKey, data, null, err => {
        assert.strictEqual(err.code, 400);

        // A routing-key must be provided
        MQ.submit(exchangeName, null, data, null, err => {
          assert.strictEqual(err.code, 400);

          // Sanity check
          MQ.submit(exchangeName, routingKey, data, null, err => {
            assert.ok(!err);
            return callback();
          });
        });
      });
    });

    /**
     * Test that verifies that the callback function in the submit handler is properly executed
     */
    it('verify callback', callback => {
      let exchangeName = util.format('testExchange-%s', ShortId.generate());
      const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
      const data = { text: 'The truth is out there' };

      let noConfirmCalled = 0;
      MQ.submit(exchangeName, routingKey, data, null, err => {
        assert.ok(!err);

        // This should only be executed once
        noConfirmCalled++;
        assert.strictEqual(noConfirmCalled, 1);

        // Declare an exchange that acknowledges the message
        exchangeName = util.format('testExchange-%s', ShortId.generate());

        let confirmCalled = 0;
        MQ.submit(exchangeName, routingKey, data, null, err => {
          assert.ok(!err);

          // This should only be executed once
          confirmCalled++;
          assert.strictEqual(confirmCalled, 1);
          return callback();
        });
      });
    });

    /**
     * Test that verifies when an amqp message is redelivered (rejected or failed), it gets sent into a
     * redelivery queue for manual intervention, rather than refiring the listener
     */
    it('verify redelivered messages are not re-executed', callback => {
      const exchangeName = util.format('testExchange-%s', ShortId.generate());
      const queueName = util.format('testQueue-%s', ShortId.generate());
      const routingKey = util.format('testRoutingKey-%s', ShortId.generate());

      // Make sure the redeliver queue is empty to start
      MQ.purge('oae-util-mq-redeliverqueue', err => {
        assert.ok(!err);

        // A listener that ensures it only handles the rejected message once
        let handledMessages = 0;
        const listener = function(msg, callback) {
          handledMessages++;
          if (handledMessages > 1) {
            // Throw in a new tick to ensure it doesn't get caught by MQ for automatic acknowledgement
            process.nextTick(() => {
              assert.fail('Should only have handled the message at most once');
            });
          }
        };

        // Subscribe to the queue and allow it to start accepting messages on the exchange
        MQ.subscribeQueue(queueName, { ack: true }, listener, err => {
          assert.ok(!err);

          // Submit a message that we can handle
          MQ.submit(exchangeName, routingKey, { data: 'test' }, null, err => {
            assert.ok(!err);
          });

          // When the raw message comes in, reject it so it gets redelivered
          _bindPreHandleOnce(queueName, (_queueName, data, headers, deliveryInfo, message) => {
            // Reject the message, indicating that we want it requeued and redelivered
            MQ.rejectMessage(message, true, () => {
              // Ensure that rabbitmq intercepts the redelivery of the rejected message and stuffs it in the redelivery queue
              // for manual intervention
              MQ.emitter.once('storedRedelivery', _queueName => {
                // Here we make sure that the listener received the message the first time. But this does not
                // ensure it doesn't receive it the second time. That is what the `assert.fail` is for in the
                // listener
                assert.strictEqual(handledMessages, 1);
                assert.strictEqual(queueName, _queueName);

                // Make sure we can take the item off the redelivery queue
                MQ.subscribeQueue('oae-util-mq-redeliverqueue', { prefetchCount: 1 }, (data, listenerCallback) => {
                  assert.ok(data);
                  assert.ok(data.headers);
                  assert.strictEqual(data.deliveryInfo.queue, queueName);
                  assert.strictEqual(data.deliveryInfo.exchange, exchangeName);
                  assert.strictEqual(data.deliveryInfo.routingKey, routingKey);
                  assert.strictEqual(data.data.data, 'test');

                  // Don't accept any more messages on this queue
                  MQ.unsubscribeQueue('oae-util-mq-redeliverqueue', err => {
                    assert.ok(!err);

                    // Acknowledge the redelivered message so it doesn't go in an infinite redelivery loop
                    listenerCallback();

                    return callback();
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});

/**
 * Bind a listener to the MQ preHandle event for a particular queue name. The bound function
 * will be unbound immediately after the first message on the queue is received.
 *
 * @param  {String}     handlingQueueName   The name of the queue on which to listen to a message
 * @param  {Function}   handler             The listener to invoke when a message comes. Same as the MQ event `preHandle`
 * @api private
 */
const _bindPreHandleOnce = function(handlingQueueName, handler) {
  /*!
   * Filters tasks by those on the expected queue, and immediately unbinds the
   * handler so it only gets invoked once. The parameters are the MQ preHandle
   * event parameters.
   */
  const _handler = function(queueName, data, headers, deliveryInfo, message) {
    if (queueName !== handlingQueueName) {
      return;
    }

    MQ.emitter.removeListener('preHandle', _handler);
    return handler(queueName, data, headers, deliveryInfo, message);
  };

  MQ.emitter.on('preHandle', _handler);
};
