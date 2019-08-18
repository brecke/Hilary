/*!
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

import util from 'util';
import _ from 'underscore';
import amqp from 'amqp-connection-manager';

import { logger } from 'oae-logger';
import * as EmitterAPI from 'oae-emitter';
import OaeEmitter from './emitter';
import * as OAE from './oae';

const log = logger('mq');

const MqConstants = {
  REDELIVER_EXCHANGE_NAME: 'oae-util-mq-redeliverexchange',
  REDELIVER_EXCHANGE_OPTIONS: {
    type: 'direct',
    durable: true,
    autoDelete: false
  },
  REDELIVER_QUEUE_NAME: 'oae-util-mq-redeliverqueue',
  REDELIVER_QUEUE_OPTIONS: {
    durable: true,
    autoDelete: false,
    arguments: {
      // Additional information on highly available RabbitMQ queues can be found at http://www.rabbitmq.com/ha.html. We
      // use `all` as the policy: Queue is mirrored across all nodes in the cluster. When a new node is added to the
      // cluster, the queue will be mirrored to that node
      'x-ha-policy': 'all'
    }
  },
  REDELIVER_SUBMIT_OPTIONS: {
    // DeliveryMode=2 indicates "persistent", which ensures a redelivered message we have stored here won't disappear when
    // rabbitmq is restarted
    deliveryMode: 2
  }
};

let connection = null;
let channel = null;
let channelWrapper = null;
const queues = {};
const exchanges = {};

let initialized = false;

// Determines whether or not we should purge queues on first connect. Also ensures we only purge the
// first time it connects (i.e., on "startup"), however any disconnect / connects while the server is
// running will not repurge
let purgeQueuesOnStartup = false;
const startupPurgeStatus = {};

let numMessagesInProcessing = 0;
let messagesInProcessing = {};

const MAX_NUM_MESSAGES_IN_PROCESSING = 1000;
const NUM_MESSAGES_TO_DUMP = 10;

/**
 * ## RabbitMQ API
 *
 * ### Events
 *
 *  * `preSubmit(routingKey, data)`                                 - Invoked just before a message is submitted to the exchange (in the same process tick)
 *  * `preHandle(queueName, data, headers, deliveryInfo)`           - Invoked just before a message handler is invoked with the message data (in the same process tick in which the message was received)
 *  * `postHandle(err, queueName, data, headers, deliveryInfo)`     - Invoked after the message handler finishes processing (or if an exception is thrown in the same process tick in which it is invoked)
 *  * `prePurge(queueName)`                                         - Invoked just before a queue is purged. All mesages which are not awaiting acknowledgement will be removed
 *  * `postPurge(queueName, count)`                                 - Invoked after the queue has been purged. `count` is the number of messages that are purged from the queue
 *  * `idle`                                                        - Invoked when all current messages have been completed and the workers are no longer processing any messages
 *  * `storedRedelivery(queueName, data, headers, deliveryInfo)`    - Invoked when a redelivered message has been aborted and stored in the redelivery queue to be manually intervened
 */
const MQ = new EmitterAPI.EventEmitter();

const deferredTaskHandlers = {};
// Let ready = false;

OaeEmitter.on('ready', () => {
  // Let ready = true;

  const numberToBind = _.keys(deferredTaskHandlers).length;
  let numberBound = 0;
  let returned = false;

  /*!
   * Monitors all the deferred task handlers that have been bound, emitting a 'ready' event
   * when all have been bound.
   */
  const _monitorBinding = function(err) {
    if (returned) {
      // Do nothing, we've called back
      return;
    }

    if (err) {
      MQ.emit('ready', err);
      returned = true;
      return;
    }

    numberBound++;
    if (!returned && numberBound >= numberToBind) {
      MQ.emit('ready');
      returned = true;
    }
  };

  if (numberToBind > 0) {
    // Bind all the deferred task handlers now that the container is ready
    _.each(deferredTaskHandlers, (handlerInfo, taskName) => {
      // eslint-disable-next-line no-undef
      bind(taskName, handlerInfo.listener, handlerInfo.options, _monitorBinding);
      delete deferredTaskHandlers[taskName];
    });
  } else {
    // No deferred task handlers, we're just immediately ready
    MQ.emit('ready');
  }
});

/**
 * Reject a message through the channel object
 * @function rejectMessage
 * @param  {Object} message  The message to be rejected
 * @param  {Boolean} requeue  Whether the message should be requeued
 * @param  {Function} callback Standard callback function
 */
const rejectMessage = function(message, requeue) {
  return new Promise(resolve => {
    /*
    if (rejection) resolve(rejection);
    else reject(rejection);
    */
    console.log('Gonna reject this shit');
    resolve(channel.nack(message, true, requeue));
  });
  /*
  return Promise.resolve(channel.reject(message, false, requeue)).catch(error => {
    // TODO
    log().error(error);
    throw error;
  });
  */
};

/**
 * Initialize the Message Queue system so that it can start sending and receiving messages.
 *
 * @param  {Object}    mqConfig        The MQ Configuration object
 * @param  {Function}  callback        Standard callback function
 * @param  {Object}    callback.err    An error that occurred, if any
 */
const init = async function(mqConfig, callback) {
  //  Const taskConfig = mqConfig.tasks || {};

  if (connection) {
    log().warn('Attempted to initialize an existing RabbitMQ connector. Ignoring');
    const queue = await _createRedeliveryQueue();
    return callback(null, queue);
  }

  log().info('Initializing RabbitMQ connector');

  const arrayOfHostsToConnectTo = _.map(mqConfig.connection.host, eachHost => {
    return `amqp://${eachHost}`;
  });

  const retryTimeout = 5;
  const establishConnection = () => {
    return amqp.connect(arrayOfHostsToConnectTo, {
      json: true,
      reconnectTimeInSeconds: retryTimeout,
      heartbeatIntervalInSeconds: 0,
      connectionOptions: {
        heartbeat: 0
      }
    });
  };

  connection = establishConnection();

  connection.on('disconnect', err => {
    log().error('Error connecting to rabbitmq, retrying in ' + retryTimeout + 's...');
    // debug
    console.log(err);
    // reconnect
    /*
    connection = establishConnection();
    return _createRedeliveryQueue().then(queue => {
      return callback(null, queue);
    });
    */
  });

  // Connect to channel
  channelWrapper = connection.createChannel({
    json: true,
    setup(ch, cb) {
      // `channel` here is a regular amqplib `ConfirmChannel`.
      channel = ch;
      log().info('Connection channel to RabbitMQ established.');

      // We only honour the purge-queue setting if the environment is not production
      if (mqConfig.purgeQueuesOnStartup === true) {
        if (process.env.NODE_ENV === 'production') {
          log().warn(
            'Attempted to set config.mq.purgeQueuesOnStartup to true when in production mode. Ignoring and not purging queues.'
          );
          purgeQueuesOnStartup = false;
        } else {
          purgeQueuesOnStartup = true;
        }
      }

      return cb();
    }
  });

  channelWrapper.waitForConnect(() => {
    log().info('Connection to RabbitMQ established.');
    if (!initialized) {
      initialized = true;
      return _createRedeliveryQueue().then(queue => {
        callback(null, queue);
      });
    }
  });
  connection.on('error', err => {
    // debug
    console.log('\n!!! Closing this shit!\n');
    log().error({ err }, 'Error in the RabbitMQ connection. Reconnecting.');
  });
  connection.on('close', () => {
    // debug
    console.log('\n!!! Closing this shit!\n');
    log().warn('Closed connection to RabbitMQ. Reconnecting.');
  });
};

/**
 * Safely shutdown the MQ service after all current tasks are completed.
 *
 * @param  {Function}   Invoked when shutdown is complete
 * @api private
 */
const _destroy = function() {
  // Unbind all queues so we don't receive new messages
  log().info('Unbinding all queues for shut down...');
  return _unsubscribeAll().then(() => {
    // Give 15 seconds for mq messages to complete processing
    log().info('Waiting until all processing messages complete...');
    return _waitUntilIdle(15000);
  });
};

OAE.registerPreShutdownHandler('mq', null, _destroy);

/**
 * Declare an exchange
 *
 * @param  {String}     exchangeName        The name of the exchange that should be declared
 * @param  {Object}     exchangeOptions     The options that should be used to declare this exchange. See https://github.com/postwait/node-amqp/#connectionexchangename-options-opencallback for a full list of options
 * @param  {Function}   callback            Standard callback function
 */
const declareExchange = function(exchangeName, exchangeOptions) {
  if (!exchangeName) {
    const error = new Error('Tried to declare an exchange without providing an exchange name');
    error.code = 400;
    log().error({
      exchangeName,
      err: error
    });
    // reject(error);
    // throw error;
    return Promise.reject();
    /*
    return callback({
      code: 400,
      msg: 'Tried to declare an exchange without providing an exchange name'
    });
    */
  }

  if (exchanges[exchangeName]) {
    const error = new Error('Tried to declare an exchange twice');
    error.code = 400;
    log().error({ exchangeName, err: error });

    // reject(error);
    return Promise.reject();
    // throw error;
    // return callback({ code: 400, msg: 'Tried to declare an exchange twice' });
  }

  // if the exchange exists already and has properties different to those supplied, the channel will ‘splode

  return channel
    .assertExchange(exchangeName, exchangeOptions.type, exchangeOptions)
    .then(() => {
      exchanges[exchangeName] = exchangeName;
    })
    .catch(error => {
      log().error({ exchangeName, err: new Error('Unable to declare an exchange') });
      /*
      return channel.checkExchange(exchangeName);
    })
    .then(ok => {
      console.log('Result of checkExchange: ' + ok);
    })
    .catch(error => {
      log().error({ exchangeName, err: new Error('Unable to check for an exchange') });
      */
    });
};

/**
 * Declare a queue
 *
 * @param  {String}     queueName           The name of the queue that should be declared
 * @param  {Object}     queueOptions        The options that should be used to declare this queue. See https://github.com/postwait/node-amqp/#connectionqueuename-options-opencallback for a full list of options
 * @param  {Function}   callback            Standard callback function
 */
const declareQueue = function(queueName, queueOptions) {
  if (!queueName) {
    const error = new Error('Tried to declare a queue without providing a name');
    error.code = 400;
    log().error({
      queueName,
      queueOptions,
      err: error
    });
    // throw error;
    return Promise.reject(error);
    // return callback({ code: 400, msg: 'Tried to declare a queue without providing a name' });
  }

  if (queues[queueName]) {
    const error = new Error('Tried to declare a queue twice');
    error.code = 400;
    log().error({ queueName, queueOptions, err: error });
    return Promise.reject(error);
    // throw error;
    // return callback({ code: 400, msg:  });
  }

  // debug
  /*
  console.log('................................................................................');
  console.log('Asserting queue ' + queueName);
  console.dir(queueOptions);
  console.log();
  */

  return channel
    .assertQueue(queueName, queueOptions)
    .then(queue => {
      log().info({ queueName }, 'Created/Retrieved a RabbitMQ queue');
      queues[queueName] = { queue };
    })
    .catch(error => {
      log().error({ queueName, err: new Error('Unable to declare a queue') });
      return Promise.reject(error);
    });
};

/**
 * Checks if a queue has been declared
 *
 * @param  {String}     queueName   The name of the queue that should be checked
 * @return {Boolean}                `true` if the queue exists, `false` otherwise
 */
const isQueueDeclared = function(queueName) {
  return !_.isUndefined(queues[queueName]);
};

/*
 * Because amqp only supports 1 queue.bind at the same time we need to
 * do them one at a time. To ensure that this is happening, we create a little
 * "in-memory queue" with bind actions that need to happen. This is all rather unfortunate
 * but there is currently no way around this.
 *
 * The reason:
 * Each time you do `Queue.bind(exchangeName, routingKey, callback)`, amqp will set an internal
 * property on the Queue object called `_bindCallback`. Unfortunately this means that whenever you
 * do 2 binds before RabbitMQ has had a chance to respond, the initial callback function will have been overwritten.
 *
 * Example:
 *   Queue.bind('house', 'door', cb1);  // Queue._bindCallback points to cb1
 *   Queue.bind('house', 'roof', cb2);  // Queue._bindCallback points to cb2
 *
 * When RabbitMQ responds with a `queueBindOk` frame for the house->door binding, amqp will execute cb2 and set _bindCallback to null.
 * When RabbitMQ responds with a `queueBindOk` frame for the house->roof binding, amqp will do nothing
 */

// A "queue" of bindings that need to happen against RabbitMQ queues
const queuesToBind = [];

// Whether or not the "in-memory queue" is already being processed
let isWorking = false;

/**
 * A function that will pick RabbitMQ queues of the `queuesToBind` queue and bind them.
 * This function will ensure that at most 1 bind runs at the same time.
 */
const singleBindQueue = function(queue) {
  return channel
    .bindQueue(queue.queueName, queue.exchangeName, queue.routingKey)
    .then(() => {
      log().trace(
        {
          queueName: queue.queueName,
          exchangeName: queue.exchangeName,
          routingKey: queue.routingKey
        },
        'Bound a queue to an exchange'
      );

      const doPurge = purgeQueuesOnStartup && !startupPurgeStatus[queue.queueName];
      if (doPurge) {
        // Ensure this queue only gets purged the first time we connect
        startupPurgeStatus[queue.queueName] = true;

        // Purge the queue before subscribing the handler to it if we are configured to do so.
        return purge(queue.queueName).catch(error => {
          const err = new Error('Tried purging an unknown queue');
          err.code = 400;
          // Promise.reject(error);
          throw err;
          // return reject(error);
        });
      }
    })
    .then(() => {
      // resolve();
    })
    .catch(error => {
      log().error({
        queueName: queue.queueName,
        err: new Error('Unable to bind queue to an exchange')
      });
      // return reject(error);
      return Promise.reject(error);
    });
};

const _processBindQueue = function() {
  // If there is something to do and we're not already doing something we can do some work
  // if (queuesToBind.length > 0 && !isWorking) {
  isWorking = true;

  const allBindings = queuesToBind.map(eachQueue => {
    return singleBindQueue(eachQueue);
  });

  return allBindings
    .reduce(function(cur, next) {
      return cur.then(next);
    }, Promise.resolve())
    .then(function() {
      // all executed

      // set queuesToBind to empty after all have been processed
      queuesToBind.length = 0;
      // return _processBindQueue();
    })
    .catch(error => {
      return Promise.reject(error);
    })
    .finally(() => {
      isWorking = false;
    });

  /*
      .reduce((p, queue) => {
        return p.then(() => singleBindQueue(queue));
      }, Promise.resolve())
      .then(() => {
      })
      .catch(error => {
        return Promise.reject(error);
      });
      */
  // todo.callback({ code: 400, msg:  });
  // }
  // todo.callback();
};

/**
 * Binds a queue to an exchange.
 * The queue will be purged upon connection if the server has been configured to do so.
 *
 * @param  {String}     queueName       The name of the queue to bind
 * @param  {String}     exchangeName    The name of the exchange to bind too
 * @param  {String}     routingKey      A string that should be used to bind the queue too the exchange
 * @param  {Function}   callback        Standard callback function
 * @param  {Object}     callback.err    An error that occurred, if any
 */
const bindQueueToExchange = function(queueName, exchangeName, routingKey) {
  if (!queues[queueName]) {
    const error = new Error('Tried to bind a non existing queue to an exchange, have you declared it first?');
    error.code = 400;
    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    // throw error;
    return Promise.reject(error);

    /*
    return callback({
      code: 400,
      msg: 'Tried to bind a non existing queue to an exchange, have you declared it first?'
    });
    */
  }

  if (!exchanges[exchangeName]) {
    const error = new Error('Tried to bind a queue to a non-existing exchange, have you declared it first?');
    error.code = 400;
    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    return Promise.reject(error);
    // throw error;
    /*
    return callback({
      code: 400,
      msg: 'Tried to bind a queue to a non-existing exchange, have you declared it first?'
    });
    / */
  }

  if (!routingKey) {
    const error = new Error('Tried to bind a queue to an existing exchange without specifying a routing key');
    error.code = 400;

    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    return Promise.reject(error);

    // throw error;
    // return callback({ code: 400, msg: 'Missing routing key' });
  }

  const todo = {
    queue: queues[queueName].queue,
    queueName,
    exchangeName,
    routingKey
  };

  return singleBindQueue(todo)
    .then(() => {})
    .catch(error => {
      return Promise.reject(error);
    });
};

/**
 * Unbinds a queue from an exchange.
 *
 * @param  {String}     queueName       The name of the queue to unbind
 * @param  {String}     exchangeName    The name of the exchange to unbind from
 * @param  {String}     routingKey      A string that should be used to unbind the queue from the exchange
 * @param  {Function}   callback        Standard callback function
 * @param  {Object}     callback.err    An error that occurred, if any
 */
const unbindQueueFromExchange = function(queueName, exchangeName, routingKey) {
  if (!queues[queueName]) {
    const error = new Error('Tried to unbind a non existing queue from an exchange, have you declared it first?');
    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    error.code = 400;
    return Promise.reject(error);
    // throw error;
    /*
    return callback({
      code: 400,
      msg: 'Tried to unbind a non existing queue from an exchange, have you declared it first?'
    });
  */
  }

  if (!exchanges[exchangeName]) {
    const error = new Error('Tried to unbind a queue from a non-existing exchange, have you declared it first?');
    error.code = 400;
    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    return Promise.reject(error);
    // throw error;
    /*
    return callback({
      code: 400,
      msg: 'Tried to unbind a queue from a non-existing exchange, have you declared it first?'
    });
    */
  }

  if (!routingKey) {
    const error = new Error('Tried to unbind a queue from an exchange without providing a routingKey');
    error.code = 400;
    log().error({
      queueName,
      exchangeName,
      routingKey,
      err: error
    });
    return Promise.reject(error);
    // throw error;
    // return callback({ code: 400, msg: 'No routing key was specified' });
  }

  // Queues[queueName].queue.unbind(exchangeName, routingKey);
  return channel
    .unbindQueue(queueName, exchangeName, routingKey, {})
    .then(() => {
      log().trace({ queueName, exchangeName, routingKey }, 'Unbound a queue from an exchange');
    })
    .catch(error => {
      // debug
      console.log(error);
      return Promise.reject(error);
      // throw error;
    });
  // callback();
};

/**
 * Subscribe the given `listener` function to the provided queue.
 *
 * @param  {Queue}      queueName           The queue to which we'll subscribe the listener
 * @param  {Object}     subscribeOptions    The options with which we wish to subscribe to the queue
 * @param  {Function}   listener            The function that will handle messages delivered from the queue
 * @param  {Object}     listener.data       The data that was sent in the message. This is different depending on the type of job
 * @param  {Function}   listener.callback   The listener callback. This must be invoked in order to acknowledge that the message was handled
 * @param  {Function}   callback            Standard callback function
 * @param  {Object}     callback.err        An error that occurred, if any
 */
const subscribeQueue = function(queueName, subscribeOptions, listener) {
  if (!queues[queueName]) {
    const error = new Error('Tried to subscribe to an unknown queue');
    error.code = 400;
    log().error({
      queueName,
      subscribeOptions,
      err: error
    });
    return Promise.reject(error);
    // throw error;
    // return callback({ code: 400, msg: 'Tried to subscribe to an unknown queue' });
  }

  return channel
    .consume(
      queueName,
      msg => {
        const { headers } = msg.properties;
        const data = JSON.parse(msg.content.toString());
        const deliveryInfo = msg.fields;
        deliveryInfo.queue = queueName;

        log().trace(
          {
            queueName,
            data,
            headers,
            deliveryInfo
          },
          'Received an MQ message.'
        );

        const deliveryKey = util.format('%s:%s', deliveryInfo.queue, deliveryInfo.deliveryTag);

        // When a message arrives that was redelivered, we do not give it to the handler. Auto-acknowledge
        // it and push it into the redelivery queue to be inspected manually
        if (deliveryInfo.redelivered) {
          const redeliveryData = {
            headers,
            deliveryInfo,
            data
          };

          return submit(
            MqConstants.REDELIVER_EXCHANGE_NAME,
            MqConstants.REDELIVER_QUEUE_NAME,
            redeliveryData,
            MqConstants.REDELIVER_SUBMIT_OPTIONS
          )
            .then(() => {
              console.log('Resubmitted the fucking message!!!');
            })
            .catch(error => {
              log().warn({ error }, 'An error occurred delivering a redelivered message to the redelivery queue');
              // TODO throw here?
              // return Promise.reject(error);
              // throw error;
            })
            .finally(() => {
              MQ.emit('storedRedelivery', queueName, data, headers, deliveryInfo, msg);
              // debug
              console.log('RESUBMITTED');
              channel.ack(msg);
              console.log('acked the resubmitted!');
              /*
            })
            .then(() => {
              return delay(10000);
            })
            .then(() => {
              return runListener(listener, {
                queueName,
                headers,
                deliveryInfo,
                msg,
                data,
                subscribeOptions,
                deliveryKey
              });
              */
            });
        }

        // debug
        console.log('Gonna run the listener!');

        return runListener(listener, { queueName, headers, deliveryInfo, msg, data, subscribeOptions, deliveryKey });
      },
      subscribeOptions
    )
    .then(ok => {
      if (!ok) {
        log().error({ queueName, err: new Error('Error binding worker for queue') });
        return unsubscribeQueue(queueName).then(() => {
          const err = new Error('Error binding a worker for queue');
          err.code = 500;
          return Promise.reject(err);
        });
      }

      // Keep the consumerTag so we can unsubscribe later
      queues[queueName].consumerTag = ok.consumerTag;
    });
};

const runListener = (listener, subscription) => {
  const { data, queueName, headers, deliveryInfo, subscribeOptions, deliveryKey, msg } = subscription;
  // Indicate that this server has begun processing a new task
  _incrementProcessingTask(deliveryKey, data, deliveryInfo);
  MQ.emit('preHandle', queueName, data, headers, deliveryInfo, msg);
  // Pass the message data to the subscribed listener
  const params = {};
  params.error = null;
  listener(data, err => {
    if (err) {
      data.error = err;
      log().error(
        {
          err,
          queueName,
          data
        },
        'An error occurred processing a task'
      );
    } else {
      log().trace(
        {
          queueName,
          data,
          headers,
          deliveryInfo
        },
        'MQ message has been processed by the listener'
      );
    }

    // Acknowledge that we've seen the message.
    // Note: We can't use queue.shift() as that only acknowledges the last message that the queue handed to us.
    // This message and the last message are not necessarily the same if the prefetchCount was higher than 1.
    if (subscribeOptions.ack !== false) {
      channel.ack(msg);
    }

    // Indicate that this server has finished processing the task
    _decrementProcessingTask(deliveryKey, deliveryInfo);
    // debug
    console.log('redelivered? ' + deliveryInfo.redelivered);
    MQ.emit('postHandle', params.eror, queueName, data, headers, deliveryInfo);
  });
};

/**
 * Stop consuming messages from a queue.
 *
 * @param  {String}    queueName       The name of the message queue to unsubscribe from
 * @param  {Function}  callback        Standard callback function
 * @param  {Object}    callback.err    An error that occurred, if any
 */
const unsubscribeQueue = function(queueName) {
  let queue = queues[queueName];
  if (!queue || !queue.queue) {
    const err = new Error('Attempted to unbind listener from non-existant job queue. Ignoring');
    log().warn({ queueName }, err.message);
    return Promise.resolve(err);
    // throw err;
  }

  const { consumerTag } = queue;
  queue = queue.queue;

  log().info({ queueName }, 'Attempting to unbind a queue');

  return channel
    .cancel(consumerTag)
    .then(ok => {
      delete queues[queueName];

      if (!ok) {
        const error = new Error('An unknown error occurred unsubscribing a queue');
        error.code = 500;
        log().error({ queueName }, error.message);
        return Promise.reject();
        // throw error;
      }

      log().info({ queueName }, 'Successfully unbound a queue');
    })
    .catch(error => {
      // TODO
      log().error({ queueName }, error.message);
      return Promise.reject(error);
    });
};

/**
 * Stop consuming messages from **ALL** the queues.
 *
 * @param  {Function}   callback    Standard callback function
 * @api private
 */
const _unsubscribeAll = async function() {
  let queuesUnbound = 0;
  const queueNames = _.keys(queues);
  if (queueNames.length > 0) {
    for (let index = 0; index < queueNames.length; index++) {
      const eachQueueName = queueNames[index];
      await unsubscribeQueue(eachQueueName);
      queuesUnbound++;
    }

    if (queuesUnbound !== queueNames.length) {
      // TODO refactor this!
      // throw new Error('Didnt unsubscribe all the queues...');
      return Promise.reject(new Error('Didnt unsubscribe all the queues...'));
    }
  }
};

/**
 * Submit a message to an exchange
 *
 * @param  {String}     exchangeName                The name of the exchange to submit the message too
 * @param  {String}     routingKey                  The key with which the message can be routed
 * @param  {Object}     [data]                      The data to send with the message. This will be received by the worker for this type of task
 * @param  {Object}     [options]                   A set of options to publish the message with. See https://github.com/postwait/node-amqp#exchangepublishroutingkey-message-options-callback for more information
 * @param  {Function}   [callback]                  Invoked when the job has been submitted, note that this does *NOT* guarantee that the message reached the exchange as that is not supported by amqp
 * @param  {Object}     [callback.err]              Standard error object, if any
 */
const submit = function(exchangeName, routingKey, data, options) {
  options = options || {};
  options.mandatory = false;

  if (!exchanges[exchangeName]) {
    const error = new Error('Tried to submit a message to an unknown exchange');
    log().error({
      exchangeName,
      routingKey,
      err: error
    });
    error.code = 400;
    return Promise.reject(error);
    // throw error;
    // return callback({ code: 400, msg: 'Tried to submit a message to an unknown exchange' });
  }

  if (!routingKey) {
    const error = new Error('Tried to submit a message without specifying a routingKey');
    error.code = 400;
    log().error({
      exchangeName,
      routingKey,
      err: error
    });
    return Promise.reject(error);
    // throw error;
    /*
    return callback({
      code: 400,
      msg: 'Tried to submit a message without specifying a routingKey'
    });
    */
  }

  MQ.emit('preSubmit', routingKey);

  // return channelWrapper
  return Promise.resolve(channelWrapper.publish(exchangeName, routingKey, data, options))
    .then(() => {
      log().info({ exchangeName, routingKey, data, options }, 'Submitted a message to an exchange');
    })
    .catch(error => {
      log().error({ exchangeName, routingKey, data, options }, 'Failed to submit a message to an exchange');
      return Promise.reject(error);
      // throw error;
    });
  /*
  , err => {
    if (err) {
      return callback(err);
    }
    */
};

/**
 * Get the names of all the queues that have been declared with the application and currently have a listener bound to it
 *
 * @return {String[]}   A list of all the names of the queues that are declared with the application and currently have a listener bound to it
 */
const getBoundQueueNames = function() {
  return _.chain(queues)
    .keys()
    .filter(queueName => {
      return !_.isUndefined(queues[queueName].consumerTag);
    })
    .value();
};

/**
 * Wait until our set of pending tasks has drained. If it takes longer than `maxWaitMillis`, it will
 * dump the pending tasks in the log that are holding things up and force continue.
 *
 * @param  {Number}     maxWaitMillis   The maximum amount of time (in milliseconds) to wait for pending tasks to finish
 * @param  {Function}   callback        Standard callback function
 * @api private
 */
const delay = t => new Promise(resolve => setTimeout(resolve, t));

const _waitUntilIdle = function(maxWaitMillis) {
  if (numMessagesInProcessing <= 0) {
    log().info('Successfully entered into idle state.');
    // return callback();
    // return new Promise(resolve => setTimeout(resolve, maxWaitMillis));
    return delay(maxWaitMillis);
  }

  /*!
   * Gives at most `maxWaitMillis` time to finish. If it doesn't, we suspect we have leaked messages and
   * output a certain amount of them to the logs for inspection.
   */
  const forceContinueHandle = new Promise(resolve =>
    setTimeout(() => {
      _dumpProcessingMessages('Timed out ' + maxWaitMillis + 'ms while waiting for tasks to complete.');
      MQ.removeListener('idle');
      resolve();
      // return callback();
    }, maxWaitMillis)
  );

  MQ.once('idle', () => {
    log().info('Successfully entered into idle state.');
    clearTimeout(forceContinueHandle);
    // return callback();
  });
};

/**
 * Purge a queue.
 *
 * @param  {String}     queueName       The name of the queue to purge.
 * @param  {Function}   [callback]      Standard callback method
 * @param  {Object}     [callback.err]  An error that occurred purging the queue, if any
 */
const purge = function(queueName) {
  if (!queues[queueName]) {
    const err = new Error('Tried purging an unknown queue');
    err.code = 400;
    log().error({ queueName, err });
    return Promise.reject(err);
  }

  log().info({ queueName }, 'Purging queue');
  MQ.emit('prePurge', queueName);

  return channel
    .purgeQueue(queueName)
    .then(data => {
      if (!data) {
        const err = new Error('Error purging queue: ' + queueName);
        err.code = 500;
        log().error({ queueName, err });
        // throw err;

        return Promise.reject(err);
      }

      MQ.emit('postPurge', queueName, data.messageCount);
    })
    .catch(error => {
      log().error({ queueName, err: new Error('Unable to purge queue') });
    });
};

/**
 * Purges all the known queues.
 * Note: This does *not* purge all the queues that are in RabbitMQ.
 * It only purges the queues that are known to the OAE system.
 *
 * @param  {Function}   [callback]      Standard callback method
 * @param  {Object}     [callback.err]  An error that occurred purging the queue, if any
 */
const purgeAll = function() {
  // Get all the known queues we can purge
  const toPurge = _.keys(queues);
  log().info({ queues: toPurge }, 'Purging all known queues.');

  /*!
   * Purges one of the known queues and calls the callback method when they are all purged (or when an error occurs)
   *
   * @param  {Object}  err    Standard error object (if any)
   */
  if (_.isEmpty(toPurge)) {
    // TODO rephrase maybe?
    // TODO reject instead?
    throw new Error('Nothing more to purge! Exiting...');
  }

  return Promise.all(
    toPurge.map(eachQueue => {
      return purge(eachQueue);
    })
  )
    .then(() => {
      log().info({ queues: toPurge }, 'Purged all known queues. Exiting...');
    })
    .catch(error => {
      // TODO rephrase maybe?
      // TODO reject instead?
      console.log('Trouble purging all queues...');
    });
};

/**
 * Create a queue that will be used to hold on to messages that were rejected / failed to acknowledge
 *
 * @param  {Function}   callback        Standard callback function
 * @param  {Object}     callback.err    An error that occurred, if any
 * @api private
 */
const _createRedeliveryQueue = function() {
  // Don't declare if we've already declared it on this node
  if (!queues[MqConstants.REDELIVER_QUEUE_NAME]) {
    // Declare an exchange with a queue whose sole purpose is to hold on to messages that were "redelivered". Such
    // situations include errors while processing or some kind of client error that resulted in the message not
    // being acknowledged
    return declareExchange(MqConstants.REDELIVER_EXCHANGE_NAME, MqConstants.REDELIVER_EXCHANGE_OPTIONS)
      .then(() => {
        return declareQueue(MqConstants.REDELIVER_QUEUE_NAME, MqConstants.REDELIVER_QUEUE_OPTIONS);
      })
      .then(() => {
        /*
      , err => {
        if (err) {
          return callback(err);
        }
        */

        /*
       , err => {
         if (err) {
           return callback(err);
          }
          */

        return bindQueueToExchange(
          MqConstants.REDELIVER_QUEUE_NAME,
          MqConstants.REDELIVER_EXCHANGE_NAME,
          MqConstants.REDELIVER_QUEUE_NAME
        );
      })
      .catch(error => {
        throw error;
      });
  }
};

/**
 * Record the fact that we have begun processing this task.
 *
 * @param  {String}     deliveryKey         A (locally) unique identifier for this message
 * @param  {Object}     data                The task data
 * @param  {Object}     deliveryInfo        The delivery info from RabbitMQ
 * @api private
 */
const _incrementProcessingTask = function(deliveryKey, data, deliveryInfo) {
  if (numMessagesInProcessing >= MAX_NUM_MESSAGES_IN_PROCESSING) {
    _dumpProcessingMessages(
      'Reached maximum number of concurrent messages allowed in processing (' +
        MAX_NUM_MESSAGES_IN_PROCESSING +
        '), this probably means there were many messages received that were never acknowledged.' +
        ' Clearing "messages in processing" to avoid a memory leak. Please analyze the set of message information (messages)' +
        ' dumped in this log and resolve the issue of messages not being acknowledged.'
    );

    messagesInProcessing = {};
    numMessagesInProcessing = 0;
  }

  messagesInProcessing[deliveryKey] = { data, deliveryInfo };
  numMessagesInProcessing++;
};

/**
 * Record the fact that we have finished processing this task.
 *
 * @param  {String}     deliveryKey         A (locally) unique identifier for this message
 * @api private
 */
const _decrementProcessingTask = function(deliveryKey) {
  delete messagesInProcessing[deliveryKey];
  numMessagesInProcessing--;

  if (numMessagesInProcessing === 0) {
    MQ.emit('idle');
  } else if (numMessagesInProcessing < 0) {
    // In this case, what likely happened was we overflowed our concurrent tasks, flushed it to 0, then
    // some existing tasks completed. This is the best way I can think of handling it that will "self
    // recover" eventually. Concurrent tasks overflowing is a sign of a leak (i.e., a task is handled
    // but never acknowleged). When this happens there should be a dump of some tasks in the logs and
    // and they should be investigated and resolved.
    numMessagesInProcessing = 0;
  }
};

/**
 * Log a message with the in-processing messages in the log line. This will log at must `NUM_MESSAGES_TO_DUMP`
 * messages.
 *
 * @param  {String}     logMessage
 * @api private
 */
const _dumpProcessingMessages = function(logMessage) {
  log().warn({ messages: _.values(messagesInProcessing).slice(0, NUM_MESSAGES_TO_DUMP) }, logMessage);
};

export {
  MQ as emitter,
  rejectMessage,
  init,
  declareExchange,
  declareQueue,
  isQueueDeclared,
  bindQueueToExchange,
  unbindQueueFromExchange,
  subscribeQueue,
  unsubscribeQueue,
  submit,
  getBoundQueueNames,
  purge,
  purgeAll
};
