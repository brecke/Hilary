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
import * as TaskQueue from 'oae-util/lib/taskqueue';

/**
 * Some options that can be used to bind to a message queue.
 */
const purgeQueueOptions = {
  subscribe: {
    prefetchCount: 1
  },
  queue: {
    durable: false
  }
};

describe('MQ', async () => {
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
    it('verify parameter validation', () => {
      const name = util.format('testQueue-%s', ShortId.generate());
      return MQ.purge(name).catch(error => {
        assert.strictEqual(error.code, 400);
      });
    });
  });

  /**
   * Verify that a queue can be purged of its tasks.
   */
  it('verify a queue can be purged', () => {
    let called = 0;
    const taskHandler = function(data, taskCallback) {
      return new Promise((resolve, reject) => {
        called++;

        setTimeout(resolve(taskCallback()), 2000);
      });
    };

    const testQueue = 'testQueue-' + new Date().getTime();
    TaskQueue.bind(testQueue, taskHandler, purgeQueueOptions, () => {
      // Submit a couple of tasks.
      for (let i = 0; i < 10; i++) {
        TaskQueue.submit(testQueue, { foo: 'bar' });
      }

      // Purge the queue.
      MQ.purge(testQueue).then(() => {
        // Because of the asynchronous nature of node/rabbitmq it's possible that a task gets delivered
        // before the purge command is processed.
        // That means we should have only handled at most 1 task.
        assert.ok(called <= 1);
      });
    });
  });
});

describe('#purgeAll()', () => {
  /**
   * Verify that all known queues can be purged of its tasks.
   */
  it('verify all queues can be purged', () => {
    const called = { a: 0, b: 0 };

    const taskHandler = (data, taskCallback) => {
      return new Promise((resolve, reject) => {
        called[data.queue]++;
        resolve();
        // setTimeout(resolve(taskCallback()), 2000);
      });
    };

    const testQueueA = 'testQueueA-' + new Date().getTime();
    const testQueueB = 'testQueueB-' + new Date().getTime();
    return TaskQueue.bind(testQueueA, taskHandler, purgeQueueOptions)
      .then(() => {
        return TaskQueue.bind(testQueueB, taskHandler, purgeQueueOptions);
      })
      .then(() => {
        // Submit a couple of tasks.
        return Promise.all(
          [...new Array(10)].map(() => {
            return TaskQueue.submit(testQueueA, { queue: 'a' });
          })
        );
      })
      .then(() => {
        return Promise.all(
          [...new Array(10)].map(() => {
            return TaskQueue.submit(testQueueB, { queue: 'b' });
          })
        );
      })
      .then(() => {
        // Purge all the queues.
        return MQ.purgeAll();
      })
      .then(() => {
        // Because of the asynchronous nature of node/rabbitmq it's possible that a task gets delivered
        // before the purge command is processed.
        // That means we should have only handled at most 1 task.
        assert.ok(called.a <= 10);
        assert.ok(called.b <= 10);
      })
      .catch(error => {
        throw error;
      });
  });
});

describe('#declareExchange()', () => {
  /**
   * Test that verifies that the parameters are validated
   */
  it('verify parameter validation', () => {
    MQ.declareExchange(null, { type: 'direct', durable: false, autoDelete: true })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        // Sanity check
        const exchangeName = util.format('testExchange-%s', ShortId.generate());
        return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true });
      });
  });

  /**
   * Test that verifies that exchanges cannot be declared twice
   */
  it('verify exchanges cannot be declared twice', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true });
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      });
  });
});

describe('#declareQueue()', () => {
  /**
   * Test that verifies that the parameters are validated
   */
  it('verify parameter validation', () => {
    return MQ.declareQueue(null, { durable: false, autoDelete: true }).catch(error => {
      assert.strictEqual(error.code, 400);
      // Sanity check
      const queueName = util.format('testQueue-%s', ShortId.generate());
      return MQ.declareQueue(queueName, { durable: false, autoDelete: true });
    });
  });

  /**
   * Test that verifies that queues cannot be declared twice
   */
  it('verify queues cannot be declared twice', () => {
    const queueName = util.format('testQueue-%s', ShortId.generate());
    return MQ.declareQueue(queueName, { durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: true });
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      });
  });
});

describe('#isQueueDeclared()', () => {
  /**
   * Test that verifies that it can be retrieved whether or not queues are declared
   */
  it('verify isQueueDeclared works', () => {
    const queueName = util.format('testQueue-%s', ShortId.generate());
    // const exchangeName = util.format('testExchange-%s', ShortId.generate());

    let isDeclared = MQ.isQueueDeclared(queueName);
    assert.strictEqual(isDeclared, false);

    return MQ.declareQueue(queueName, { durable: false, autoDelete: true }).then(() => {
      isDeclared = MQ.isQueueDeclared(queueName);
      assert.strictEqual(isDeclared, true);
    });
  });
});

describe('#bindQueueToExchange()', () => {
  /**
   * Test that verifies that the parameters are validated
   */
  it('verify parameter validation', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: true });
      })
      .then(() => {
        return MQ.bindQueueToExchange(null, exchangeName, routingKey);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, null, routingKey);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, exchangeName, null);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        // Sanity check that the queue can be bound
        return MQ.bindQueueToExchange(queueName, exchangeName, routingKey);
      })
      .then(() => {
        // Tidy up after ourselves and remove the binding
        return MQ.unbindQueueFromExchange(queueName, exchangeName, routingKey);
      })
      .catch(error => {
        throw error;
      });
  });

  /**
   * Test that verifies a queue can be bound to an exchange
   */
  it('verify functionality', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
    const data = { text: 'The truth is out there' };

    const listener = function(msg) {
      // Verify the message we receive is correct
      assert.strictEqual(msg.text, data.text);
      // Unbind the queue so both the queue and exchange will go away when we restart rabbitmq-server
      return MQ.unbindQueueFromExchange(queueName, exchangeName, routingKey);
    };

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: true });
      })
      .then(() => {
        return MQ.subscribeQueue(queueName, {}, listener);
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, exchangeName, routingKey);
      })
      .then(() => {
        return MQ.submit(exchangeName, routingKey, data);
      })
      .catch(error => {
        throw error;
      });
  });

  /**
   * Test that verifies you can bind queues to exchanges in parallel
   */
  it('verify you can bind queues to exchanges in parallel', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const data = { text: 'The truth is out there' };

    const numberOfKeys = 10;
    const routingKeys = [...new Array(numberOfKeys)].map((each, index) => {
      return `key-${index}`;
    });
    /*
    for (let i = 0; i < numberOfKeys; i++) {
      routingKeys.push('key-' + i);
    }
    */

    let submittingCounter = 0;
    let bindingCounter = 0;
    let listeningCounter = 0;

    const incrementer = () => {
      listeningCounter++;
      /*
      return new Promise((resolve, reject) => {
        resolve(listeningCounter++);
      });
      */
    };

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: false });
      })
      .then(() => {
        return MQ.subscribeQueue(queueName, {}, incrementer);
      })
      .then(() => {
        const allBindings = routingKeys.map(eachRoutingKey => {
          bindingCounter++;
          return MQ.bindQueueToExchange(queueName, exchangeName, eachRoutingKey);
        });

        /*
        return allBindings[0]
          .then(() => {
            return allBindings[1];
          })
          .then(() => {
            return allBindings[2];
          })
          .then(() => {
            return allBindings[3];
          })
          .then(() => {
            return allBindings[4];
          })
          .then(() => {
            return allBindings[5];
          })
          .then(() => {
            return allBindings[6];
          })
          .then(() => {
            return allBindings[7];
          })
          .then(() => {
            return allBindings[8];
          })
          .then(() => {
            return allBindings[9];
          })
          .catch(error => {
            throw error;
          });
          */

        return Promise.all(allBindings);
      })
      .then(() => {
        const allSubmissions = routingKeys.map(eachRoutingKey => {
          submittingCounter++;
          return MQ.submit(exchangeName, eachRoutingKey, data);
        });

        // submit one after the other
        /*
          return allSubmissions
            .reduce((cur, next) => {
              return cur.then(next);
            }, Promise.resolve())
            */
        return Promise.all(allSubmissions);
      })
      .then(() => {
        assert.strictEqual(numberOfKeys, bindingCounter);
        assert.strictEqual(numberOfKeys, submittingCounter);
        assert.strictEqual(numberOfKeys, listeningCounter);
      })
      .catch(error => {
        throw error;
      });
  });
});

describe('#unbindQueueFromExchange()', () => {
  /**
   * Test that verifies that the parameters are validated
   */
  it('verify parameter validation', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: false });
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, exchangeName, routingKey);
      })
      .then(() => {
        return MQ.unbindQueueFromExchange(null, exchangeName, routingKey);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        return MQ.unbindQueueFromExchange(queueName, null, routingKey);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        return MQ.unbindQueueFromExchange(queueName, exchangeName, null);
      })
      .catch(error => {
        assert.strictEqual(error.code, 400);
      })
      .then(() => {
        // Sanity-check and tidy up
        return MQ.unbindQueueFromExchange(queueName, exchangeName, routingKey);
      })
      .catch(error => {
        throw error;
      });
  });

  /**
   * Test that verifies a queue can be unbound from an exchange
   */
  it('verify functionality', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
    const data = { text: 'The truth is out there' };

    let handledMessages = 0;
    const listener = function(msg) {
      handledMessages++;

      // We should only receive one message
      assert.strictEqual(handledMessages, 1);

      // Verify the message we receive is correct
      assert.strictEqual(msg.text, data.text);
    };

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.declareQueue(queueName, { durable: false, autoDelete: false });
      })
      .then(() => {
        return MQ.subscribeQueue(queueName, {}, listener);
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, exchangeName, routingKey);
      })
      .then(() => {
        return MQ.submit(exchangeName, routingKey, data);
      })
      .then(() => {
        // Unbind the queue from the exchange, we should no longer receive any messages
        return MQ.unbindQueueFromExchange(queueName, exchangeName, routingKey);
        /*
      })
      .then(() => {
        // debug
        console.log(queueName);
        // Submit one more message. If it ends up at our listener the test will fail
        return MQ.submit(exchangeName, routingKey, data);
      })
      .then(ok => {
        console.log('coco');
        */
      })
      .catch(error => {
        throw error;
      });
  });
});

describe('#submit()', () => {
  /**
   * Test that verifies the passed in parameters
   */
  it('verify parameter validation', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    // const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
    const data = { text: 'The truth is out there' };

    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.submit(null, routingKey, data, null);
      })
      .catch(error => {
        // An exchange must be provided
        assert.strictEqual(error.code, 400);

        return MQ.submit(exchangeName, null, data, null);
      })
      .catch(error => {
        // A routing-key must be provided
        assert.strictEqual(error.code, 400);
        // Sanity check
        return MQ.submit(exchangeName, routingKey, data, null);
      })
      .catch(error => {
        throw error;
      });
  });

  /**
   * Test that verifies that the callback function in the submit handler is properly executed
   */
  it('verify callback', () => {
    let exchangeName = util.format('testExchange-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());
    const data = { text: 'The truth is out there' };

    let confirmCalled = 0;
    let noConfirmCalled = 0;
    return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true })
      .then(() => {
        return MQ.submit(exchangeName, routingKey, data, null);
      })
      .then(() => {
        // This should only be executed once
        noConfirmCalled++;
        assert.strictEqual(noConfirmCalled, 1);
        // Declare an exchange that acknowledges the message
        exchangeName = util.format('testExchange-%s', ShortId.generate());
        return MQ.declareExchange(exchangeName, { type: 'direct', durable: false, autoDelete: true, confirm: true });
      })
      .then(() => {
        return MQ.submit(exchangeName, routingKey, data, null);
      })
      .then(() => {
        // This should only be executed once
        confirmCalled++;
        assert.strictEqual(confirmCalled, 1);
      })
      .catch(error => {
        throw error;
      });
  });

  /**
   * Test that verifies when an amqp message is redelivered (rejected or failed), it gets sent into a
   * redelivery queue for manual intervention, rather than refiring the listener
   */
  it.only('verify redelivered messages are not re-executed', () => {
    const exchangeName = util.format('testExchange-%s', ShortId.generate());
    const queueName = util.format('testQueue-%s', ShortId.generate());
    const routingKey = util.format('testRoutingKey-%s', ShortId.generate());

    // A listener that ensures it only handles the rejected message once
    let handledMessages = 0;

    const listener = function(/* msg */) {
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          handledMessages++;
          if (handledMessages > 1) {
            // Throw in a new tick to ensure it doesn't get caught by MQ for automatic acknowledgement
            process.nextTick(() => {
              assert.fail('Should only have handled the message at most once');
              reject();
            });
          }

          resolve();
        }, 10);
      });
    };

    const goodListener = function(msg, done) {
      handledMessages++;
      if (handledMessages > 1) {
        // Throw in a new tick to ensure it doesn't get caught by MQ for automatic acknowledgement
        process.nextTick(() => {
          assert.fail('Should only have handled the message at most once');
          return done(new Error('Shit'));
        });
      }

      return done();
    };

    const handler = (_queueName, data, headers, deliveryInfo, message) => {
      // Reject the message, indicating that we want it requeued and redelivered
      return MQ.rejectMessage(message, true)
        .then(() => {
          // Ensure that rabbitmq intercepts the redelivery of the rejected message and stuffs it in the redelivery queue
          // for manual intervention
          MQ.emitter.once('storedRedelivery', _queueName => {
            // Here we make sure that the listener received the message the first time. But this does not
            // ensure it doesn't receive it the second time. That is what the `assert.fail` is for in the
            // listener
            assert.strictEqual(handledMessages, 1);
            assert.strictEqual(queueName, _queueName);
          });
          // Make sure we can take the item off the redelivery queue
          return MQ.subscribeQueue('oae-util-mq-redeliverqueue', { prefetchCount: 1 }, data => {
            assert.ok(data);
            assert.ok(data.headers);
            assert.strictEqual(data.deliveryInfo.queue, queueName);
            assert.strictEqual(data.deliveryInfo.exchange, exchangeName);
            assert.strictEqual(data.deliveryInfo.routingKey, routingKey);
            assert.strictEqual(data.data.data, 'test');
          });
        })
        .then(() => {
          // Don't accept any more messages on this queue
          return MQ.unsubscribeQueue('oae-util-mq-redeliverqueue');
        })
        .then(() => {
          // Acknowledge the redelivered message so it doesn't go in an infinite redelivery loop
          // listenerCallback();
        })
        .catch(error => {
          throw error;
        });
    };

    // debug
    console.log(queueName);
    console.log(exchangeName);

    // Make sure the redeliver queue is empty to start
    return MQ.purge('oae-util-mq-redeliverqueue')
      .then((err, ok) => {
        return MQ.declareExchange(exchangeName, { type: 'direct', durable: true, autoDelete: false }); // TODO autoDelete
      })
      .then(() => {
        // Create the exchange and queue on which we'll deliver a message and reject it
        return MQ.declareQueue(queueName, { durable: true, autoDelete: false }); // TODO
      })
      .then(() => {
        // Subscribe to the queue and allow it to start accepting messages on the exchange
        return MQ.subscribeQueue(queueName, { ack: true }, goodListener);
      })
      .then(() => {
        return MQ.bindQueueToExchange(queueName, exchangeName, routingKey);
      })
      .then(() => {
        // When the raw message comes in, reject it so it gets redelivered
        _bindPreHandleOnce(queueName, handler);
        // Submit a message that we can handle
        return MQ.submit(exchangeName, routingKey, { data: 'test' }, null);
      })
      .catch(error => {
        throw error;
      })
      .finally(() => {
        console.log('No more time, exiting in 10s...');
        setTimeout(() => {
          console.log('exit!');
        }, 10000);
      });
    /*
      .then(() => {
        console.log('Waiting for 5s...');
        return new Promise(resolve => {
          setTimeout(resolve, 5000);
        });
      })
      */
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
});
