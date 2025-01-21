/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/**
 * Creates a non-clearable timer that resolves the promise once elapses.
 * @param {number} ms
 * @returns {Promise<void>}
 */
function delay(ms) {
  return new Promise(r => setTimeout(r, ms || 0));
}

/**
 * Creates a Promise that gets resolved or rejected based on an event.
 * @param {object} emitter
 * @param {string} eventName
 * @returns {Promise}
 */
function fromEvent(emitter, eventName) {
  return new Promise((resolve, reject) =>
    emitter.once(eventName, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    }));
}

/**
 * Creates a Promise from a callback based function.
 * @param {Function} fn
 * @returns {Promise}
 */
function fromCallback(fn) {
  return new Promise((resolve, reject) =>
    fn((err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    }));
}

/**
 * Gets a function that has the signature of a callback that invokes the appropriate promise handler parameters.
 * @param {Function} resolve
 * @param {Function} reject
 * @returns {Function}
 */
function getCallback(resolve, reject) {
  return function (err, result) {
    if (err) {
      reject(err);
    } else {
      resolve(result);
    }
  };
}

async function invokeSequentially(info, length, fn) {
  let index;
  while ((index = info.counter++) < length) {
    await fn(index);
  }
}

/**
 * Invokes the new query plan of the load balancing policy and returns a Promise.
 * @param {LoadBalancingPolicy} lbp The load balancing policy.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @returns {Promise<Iterator>}
 */
function newQueryPlan(lbp, keyspace, executionOptions) {
  return new Promise((resolve, reject) => {
    lbp.newQueryPlan(keyspace, executionOptions, (err, iterator) => {
      if (err) {
        reject(err);
      } else {
        resolve(iterator);
      }
    });
  });
}

/**
 * Method that handles optional callbacks (dual promise and callback support).
 * When callback is undefined it returns the promise.
 * When using a callback, it will use it as handlers of the continuation of the promise.
 * @param {Promise} promise
 * @param {Function?} callback
 * @returns {Promise|undefined}
 */
function optionalCallback(promise, callback) {
  if (!callback) {
    return promise;
  }

  toCallback(promise, callback);
}

/**
 * Invokes the provided function multiple times, considering the concurrency level limit.
 * @param {Number} count
 * @param {Number} limit
 * @param {Function} fn
 * @returns {Promise}
 */
function times(count, limit, fn) {
  if (limit > count) {
    limit = count;
  }

  const promises = new Array(limit);

  const info = {
    counter: 0
  };

  for (let i = 0; i < limit; i++) {
    promises[i] = invokeSequentially(info, count, fn);
  }

  return Promise.all(promises);
}

/**
 * Deals with unexpected rejections in order to avoid the unhandled promise rejection warning or failure.
 * @param {Promise} promise
 * @returns {undefined}
 */
function toBackground(promise) {
  promise.catch(() => {});
}

/**
 * Invokes the callback once outside the promise chain the promise is resolved or rejected.
 * @param {Promise} promise
 * @param {Function?} callback
 * @returns {undefined}
 */
function toCallback(promise, callback) {
  promise
    .then(
      result => process.nextTick(() => callback(null, result)),
      // Avoid marking the promise as rejected
      err => process.nextTick(() => callback(err)));
}

module.exports = {
  delay,
  fromCallback,
  fromEvent,
  getCallback,
  newQueryPlan,
  optionalCallback,
  times,
  toBackground,
  toCallback
};