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

module.exports = {
  fromCallback,
  fromEvent,
  newQueryPlan,
  times
};