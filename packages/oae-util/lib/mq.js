/*!
 * Copyright 2014 Apereo Foundation (AF) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://opensource.org/licenses/ECL-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import _ from 'underscore';

import { EventEmitter } from 'oae-emitter';
import { logger } from 'oae-logger';
import * as Redis from './redis';
import OaeEmitter from './emitter';
import * as OAE from './oae';
import { Validator } from './validator';

const log = logger('mq');
const emitter = new EventEmitter();

/**
 * Task queueing logic
 *
 * For every queue OAE creates, there are two extra queues (redis lists), *-processing and *-redelivery
 * The way tasks are processed is illustrated below for `oae-activity/activity`, but works the same way for all queues:
 *
 * oae-acticity/activity
 * oae-acticity/activity-processing
 * oae-acticity/activity/enable
 * oae-acticity/activity/enable-processing
 * oae-search/index
 * oae-search/index-processing
 * oae-search/delete
 * oae-search/delete-processing
 * oae-search/reindex
 * oae-search/reindex-processing
 * oae-content/etherpad-publish
 * oae-content/etherpad-publish-processing
 * oae-content/ethercalc-publish
 * oae-content/ethercalc-publish-processing
 * oae-content/ethercalc-edit
 * oae-content/ethercalc-edit-processing
 * oae-preview-processor/generatePreviews
 * oae-preview-processor/generatePreviews-processing
 * oae-preview-processor/generateFolderPreviews
 * oae-preview-processor/generateFolderPreviews-processing
 * oae-preview-processor/regeneratePreviews
 * oae-preview-processor/regeneratePreviews-processing
 *
 *     ┌──────────────────────────────────────────┬─┐
 *     │           oae-activity/activity          │X│──┐
 *     └──────────────────────────────────────────┴─┘  │
 *                                                     │
 *  ┌──────────────────  brpoplpush   ─────────────────┘
 *  │
 *  │                                                   handler     Λ    returns
 *  │  ┌─┬──────────────────────────────────────────┐   invoked    ╱ ╲    error           lpush   ┌─┬──────────────────────────────────────────┐
 *  └─▷│X│    oae-activity/activity-processing      │────────────▷▕   ▏─────────────▷  ──────────▷│X│    oae-activity/activity-redelivery      │
 *     └─┴──────────────────────────────────────────┘              ╲ ╱                            └─┴──────────────────────────────────────────┘
 *                            △                                     V                                                    │
 *                            │                                     │                                                    │
 *                            │                                                                                          │
 *                            │                                returns OK                                                │
 *                            │                 ┌─┐                                                                      │
 *                            │       lrem (-1) │X│ from            │                                                    │
 *                            │                 └─┘                 ▽                                                    │
 *                            └────────────────────────────────────  ◁───────────────────────────────────────────────────┘
 */

/**
 * Redis configuration which will load from config.js
 */
let redisConfig = null;

/**
 * This will hold a connection used only for PURGE operations
 * See `purgeAllQueues` for details
 */
let theRedisPurger = null;

/**
 * This will hold a connection used only for LLEN operations
 * See `whenTasksEmpty` for details
 * This is only relevant for tests
 */
let theRedisEmptinessChecker = null;

/**
 * This will hold a connection for LPUSHing messages to queues
 * See `submit` for details
 */
let theRedisPublisher = null;

/**
 * This object will track the current bindings
 * meaning every time we subscribe to a queue
 * by assigning a listener to it, we set the binding,
 * and every time we unsubscribe, we do the opposite
 */
const queueBindings = {};

/*+
 * This object contains the different redis clients
 * OAE uses. It is currently one per queue (such as oae-activity/activity,
 * oae-search/reindex, oae-preview-processor/generatePreviews, etc)
 * Every time we subscribe, we block that client while listening
 * on the queue, using redis BRPOPLPUSH.
 * See `_getOrCreateSubscriberForQueue` for details
 */
const subscribers = {};

const PRODUCTION_MODE = 'production';

/**
 * This is kind of legacy but still plays a role booting up tests
 * Previously this would be a "reminder" to bind all the queues
 * which of course is no longer necessary
 */
OaeEmitter.on('ready', () => {
  emitter.emit('ready');
});

/**
 * Safely shutdown the MQ service
 * by closing connections safely
 * Check IOredis API for details:
 * https://github.com/luin/ioredis/blob/master/API.md
 */
OAE.registerPreShutdownHandler('mq', null, done => {
  quitAllConnectedClients();
  return done();
});

/**
 * Initialize the Message Queue system so that it can start sending and receiving messages.
 *
 * @function init
 * @param  {Object}    config           The MQ Configuration object
 * @param  {Function}  callback         Standard callback function
 * @param  {Object}    callback.err     An error that occurred, if any
 * @returns {Function}                  Returns a callback
 */
const init = function(config, callback) {
  redisConfig = config;

  // redis connection possible statuses
  const hasNotBeenCreated = theRedisPurger === null;
  const hasConnectionBeenClosed = theRedisPurger !== null && theRedisPurger.status === 'end';

  // Only init if the connections haven't been opened.
  if (hasNotBeenCreated) {
    Redis.createClient(config, (err, client) => {
      if (err) return callback(err);
      theRedisPurger = client;
      theRedisEmptinessChecker = client.duplicate();
      theRedisPublisher = client.duplicate();

      // Assign names just for fun
      theRedisPurger.queueName = 'purger';
      theRedisEmptinessChecker.queueName = 'checker';
      theRedisPublisher.queueName = 'publisher';

      // if the flag is set, we purge all queues on startup. ONLY if we're NOT in production mode.
      const shallWePurge = redisConfig.purgeQueuesOnStartup && process.env.NODE_ENV !== PRODUCTION_MODE;
      if (shallWePurge) {
        purgeAllBoundQueues(callback);
      } else {
        return callback();
      }
    });
  } else if (hasConnectionBeenClosed) {
    Redis.reconnect(theRedisPurger, err => {
      if (err) return callback(err);
      Redis.reconnect(theRedisPublisher, err => {
        if (err) return callback(err);
        Redis.reconnect(theRedisEmptinessChecker, err => {
          if (err) return callback(err);
          return callback();
        });
      });
    });
  } else {
    return callback();
  }
};

/**
 * Subscribe the given `listener` function to the provided queue.
 *
 * @function collectLatestFromQueue
 * @param  {Queue}      queueName           The queue to which we'll subscribe the listener
 * @param  {Function}   listener            The function that will handle messages delivered from the queue
 * @param  {Object}     listener.data       The data that was sent in the message. This is different depending on the type of job
 * @param  {Function}   listener.callback   The listener callback. This must be invoked in order to acknowledge that the message was handled
 */
const waitForTasksOnQueue = (queueName, listener) => {
  _getOrCreateSubscriberForQueue(queueName, (err, queueSubscriber) => {
    if (err) log().error({ err }, 'Error creating redis client');

    const validConnection = queueSubscriber && !queueSubscriber.manuallyClosing;
    if (validConnection) {
      queueSubscriber.brpoplpush(queueName, getProcessingQueueFor(queueName), 0, (err, queuedMessage) => {
        if (err) log().error({ err }, 'Error while BRPOPLPUSHing redis queue ' + queueName);

        if (queuedMessage) {
          parseMessage(queuedMessage, queueName, listener, () => {
            const processingQueue = getProcessingQueueFor(queueName);
            removeMessageFromQueue(processingQueue, queuedMessage, () => {
              // return waitForTasksOnQueue(queueName, listener);
              log().info(`Finished parsing a message fetched on ${queueName}. Resuming...`);
            });
          });
        } else {
          log().warn(`Fetched an invalid message from ${queueName}. Resuming...`);
        }

        return waitForTasksOnQueue(queueName, listener);
      });
    } else {
      // debug
      log().warn('Something wrong going on here');
    }
  });
};

const removeMessageFromQueue = (processingQueue, queuedMessage, callback) => {
  log().info(`About to remove a message from ${processingQueue}`);
  theRedisPurger.lrem(processingQueue, -1, queuedMessage, (err, count) => {
    if (err) {
      log().error('Unable to LREM from redis, message is kept on ' + processingQueue);
      return callback(err);
    }

    log().info(`Removed ${count} message from ${processingQueue}. Resuming worker...`);
    // return collectLatestFromQueue(queueName, listener);
    return callback();
  });
};

const parseMessage = (queuedMessage, queueName, listener, callback) => {
  const message = JSON.parse(queuedMessage);
  listener(message, err => {
    /**
     * Lets set the convention that if the listener function
     * returns the callback with an error, then something went
     * unpexpectadly wrong, and we need to know about it.
     * Hence, we're sending it to a special queue for analysis
     */
    if (err) {
      log().warn(
        { err },
        `Using the redelivery mechanism for a message that failed running ${listener.name} on ${queueName}`
      );
      // TODO
      const redeliveryQueue = getRedeliveryQueueFor(queueName);
      sendToRedeliveryQueue(redeliveryQueue, queuedMessage, err => {
        if (err) {
          log().warn(`Unable to submit a message to ${redeliveryQueue}`);
        } else {
          log().warn(`Submitted a message to ${redeliveryQueue} following an error`);
        }

        return callback();
      });
    }

    return callback();
  });
};

/**
 * @function isQueueEmpty
 * @param  {String} queueName  The queue name we're checking the size of (which is a redis List)
 * @param  {Object} someRedisConnection A redis client which is used solely for subscribing to this queue
 * @param  {Function} done     Standar callback function
 */
const isQueueEmpty = (queueName, done) => {
  const redisConnection = fetchEmptinessChecker();
  redisConnection.llen(queueName, (err, stillQueued) => {
    if (err) done(err);
    return done(null, stillQueued === 0);
  });
};

/**
 * Sends a message which has just failed to be processed to a special queue for later inspection
 *
 * @function _redeliverToSpecialQueue
 * @param  {String} queueName   The queue name for redelivery, which is a redis List
 * @param  {String} message     The message we need to store in the redelivery queue in JSON format
 */
const sendToRedeliveryQueue = (redeliveryQueue, message, callback) => {
  theRedisPublisher.lpush(redeliveryQueue, message, err => {
    if (err) return callback(err);
    return callback;
  });
};

/**
 * Binds a listener to a queue, meaning that every time a message is pushed to that queue
 * (which is a redis List) that listener will be executed.
 *
 * @function subscribe
 * @param  {String}   queueName     The queue name we want to subscribe to, which is a redis List
 * @param  {Function} listener      The function we need to run for each task sent to the queue
 * @param  {Function} callback      Standard callback function
 * @param  {Object}   callback.err  An error that occurred, if any
 * @return {Function}               Returns the callback
 */
const subscribe = (queueName, listener, callback) => {
  callback = callback || function() {};
  const validator = new Validator();
  validator.check(queueName, { code: 400, msg: 'No channel was provided.' }).notEmpty();
  if (validator.hasErrors()) return callback(validator.getFirstError());

  const queueIsAlreadyBound = queueBindings[queueName];
  if (queueIsAlreadyBound) {
    return callback();
  }

  queueBindings[queueName] = getProcessingQueueFor(queueName);
  waitForTasksOnQueue(queueName, listener);

  return callback();
};

/**
 * Unbinds any listener to a specific queue, meaning that if we then submit messages
 * to that queue, they won't be processed. This happens because we do two things here:
 * 1 We flag the queue as unbound, and `submit` respects this flag, so it won't even PUSH
 * 2 We remove the listener associated with the event which is sent when submit PUSHES to the queue
 *
 * @function unsubscribe
 * @param  {String}    queueName       The name of the message queue to unsubscribe from
 * @param  {Function}  callback        Standard callback function
 * @param  {Object}    callback.err    An error that occurred, if any
 * @returns {Function}                 Returns callback
 */
const unsubscribe = (queueName, callback) => {
  callback = callback || function() {};
  const validator = new Validator();
  validator.check(queueName, { code: 400, msg: 'No channel was provided.' }).notEmpty();
  if (validator.hasErrors()) return callback(validator.getFirstError());

  // update bindings
  delete queueBindings[queueName];

  // disconnect subscriber
  const subscribedClient = subscribers[queueName];
  if (subscribedClient) {
    subscribedClient.disconnect(false);
  }

  return callback();
};

const printBoundQueues = queueName => {
  // debug
  console.log(`Unsubscribed to [${queueName}]. Status:`);
  const coco = _.reject(_.keys(queueBindings), eachKey => {
    return !queueBindings[eachKey];
  });
  console.dir(coco);
};

/**
 * Gets the map-like object where we keep track of which queues are bound and which aren't
 *
 * @function getBoundQueues
 * @return {Object} An map-like object which contains all the queues (String) that are bound to a listener
 */
const getBoundQueues = function() {
  return queueBindings;
};

/**
 * Submit a message to an exchange
 *
 * @function submit
 * @param  {String}     queueName                   The queue name which is a redis List
 * @param  {String}     message                     The data to send with the message in JSON. This will be received by the worker for this type of task
 * @param  {Function}   [callback]                  Invoked when the job has been submitted
 * @param  {Object}     [callback.err]              Standard error object, if any
 * @returns {Function}                              Returns callback
 */
const submit = (queueName, message, callback) => {
  callback = callback || function() {};
  const validator = new Validator();
  validator.check(queueName, { code: 400, msg: 'No channel was provided.' }).notEmpty();
  validator.check(message, { code: 400, msg: 'No message was provided.' }).notEmpty();
  if (validator.hasErrors()) return callback(validator.getFirstError());

  const queueIsBound = queueBindings[queueName];
  if (queueIsBound) {
    theRedisPublisher.lpush(queueName, message, () => {
      return callback();
    });
  } else {
    return callback();
  }
};

/**
 * Gets all the active redis clients
 * This includes the `manager`, the `publisher` and the active `subscribers`
 *
 * @function getAllConnectedClients
 * @return {Array} An Array with all the active redis connections
 */
const getAllActiveClients = () => {
  return _.values(subscribers)
    .concat(theRedisPurger)
    .concat(theRedisPublisher)
    .concat(theRedisEmptinessChecker);
};

/**
 * Purge a queue.
 *
 * @function purgeQueue
 * @param  {String}     queueName       The name of the queue to purge.
 * @param  {Function}   [callback]      Standard callback method
 * @param  {Object}     [callback.err]  An error that occurred purging the queue, if any
 * @returns {Function}                  Returns a callback
 */
const purgeQueue = (queueName, callback) => {
  callback = callback || function() {};
  const validator = new Validator();
  validator.check(queueName, { code: 400, msg: 'No channel was provided.' }).notEmpty();
  if (validator.hasErrors()) return callback(validator.getFirstError());

  emitter.emit('prePurge', queueName);
  theRedisPurger.del(queueName, err => {
    if (err) return callback(err);

    emitter.emit('postPurge', queueName, 1);

    return callback();
  });
};

/**
 * Purge a list of queues, by calling `purgeQueue` recursively
 *
 * @function purgeQueues
 * @param  {Array} allQueues    An array containing all the queue names we want to purge (which are redis Lists).
 * @param  {Function} done      Standard callback method
 * @return {Function}           Returns callback
 */
const purgeQueues = (allQueues, done) => {
  if (allQueues.length === 0) {
    return done();
  }

  const nextQueueToPurge = allQueues.pop();
  purgeQueue(nextQueueToPurge, () => {
    purgeQueue(getProcessingQueueFor(nextQueueToPurge), () => {
      return purgeQueues(allQueues, done);
    });
  });
};

/**
 * Purges the queues which are currently subscribed (or bound to a listener)
 *
 * @function purgeAllBoundQueues
 * @param  {Function} callback Standard callback method
 */
const purgeAllBoundQueues = callback => {
  const queuesToPurge = _.keys(queueBindings);
  purgeQueues(queuesToPurge, callback);
};

/**
 * Quits (aka disconnect) all active redis clients
 *
 * @function quitAllConnectedClients
 * @param  {Function} done Standard callback function
 * @return {Function} Returns `quitAllClients` method
 */
const quitAllConnectedClients = () => {
  const allClients = getAllActiveClients();
  _.each(allClients, each => {
    each.disconnect();
  });
};

/**
 * Fetches the length of a queue (which is a redis list)
 *
 * @function getQueueLength
 * @param  {String}   queueName The queue we want to know the length of
 * @param  {Function} callback  Standard callback function
 */
const getQueueLength = (queueName, callback) => {
  theRedisPurger.llen(queueName, (err, count) => {
    if (err) return callback(err);

    return callback(null, count);
  });
};

/**
 * Fetches the redis connection used to subscribe to a specific queue
 * There is one redis connection per queue
 *
 * @function _getOrCreateSubscriberForQueue
 * @param  {String} queueName   The queue name, which the client is or will be subscribing
 * @param  {Function} callback  Standard callback function
 * @return {Function}           Returns callback
 */
const _getOrCreateSubscriberForQueue = (queueName, callback) => {
  const subscriber = subscribers[queueName];
  // redis connection possible statuses:
  const hasNotBeenCreated = !subscriber;
  const hasConnectionBeenClosed = subscriber && subscriber.status === 'end';

  if (hasNotBeenCreated) {
    // create it then
    Redis.createClient(redisConfig, (err, client) => {
      if (err) return callback(err);

      client.queueName = queueName;
      subscribers[queueName] = client;
      return callback(null, subscribers[queueName]);
    });
  } else if (hasConnectionBeenClosed) {
    subscribers[queueName].connect(err => {
      if (err) return callback(err);
      return callback(null, subscribers[queueName]);
    });
  } else {
    return callback(null, subscribers[queueName]);
  }
};

/**
 * Utility function for getting the name of the corresponding redelivery queue
 * The rule is we just append `-redelivery` to a queueName
 *
 * @function getRedeliveryQueueFor
 * @param  {String} queueName The queue name which we want the corresponding redelivery queue for
 * @return {String} The redelivery queue name
 */
const getRedeliveryQueueFor = queueName => {
  return `${queueName}-redelivery`;
};

/**
 * Utility function for getting the name of the corresponding processiing queue
 * The rule is we just append `-processing` to a queueName
 *
 * @function getProcessingQueueFor
 * @param  {String} queueName The queue name which we want the corresponding processing queue for
 * @return {String} The processing queue name
 */
const getProcessingQueueFor = queueName => {
  return `${queueName}-processing`;
};

// TODO: jsdocs
const fetchEmptinessChecker = () => {
  return theRedisEmptinessChecker;
};

export {
  emitter,
  init,
  subscribe,
  unsubscribe,
  purgeQueue,
  purgeQueues,
  purgeAllBoundQueues,
  getBoundQueues,
  submit,
  getQueueLength,
  getAllActiveClients as getAllConnectedClients,
  quitAllConnectedClients,
  fetchEmptinessChecker,
  isQueueEmpty
};
