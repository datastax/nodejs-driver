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

const util = require('util');
const utils = require('../utils');
const promiseUtils = require('../promise-utils');

const _queueOverflowThreshold = 1000;

/**
 * Debounce protocol events by acting on those events with a sliding delay.
 * @ignore
 * @constructor
 */
class EventDebouncer {

  /**
   * Creates a new instance of the event debouncer.
   * @param {Number} delay
   * @param {Function} logger
   */
  constructor(delay, logger) {
    this._delay = delay;
    this._logger = logger;
    this._queue = null;
    this._timeout = null;
  }

  /**
   * Adds a new event to the queue and moves the delay.
   * @param {{ handler: Function, all: boolean|undefined, keyspace: String|undefined,
   * cqlObject: String|null|undefined }} event
   * @param {Boolean} processNow
   * @returns {Promise}
   */
  eventReceived(event, processNow) {
    return new Promise((resolve, reject) => {
      event.callback = promiseUtils.getCallback(resolve, reject);
      this._queue = this._queue || { callbacks: [], keyspaces: {} };
      const delay = !processNow ? this._delay : 0;
      if (event.all) {
        // when an event marked with all is received, it supersedes all the rest of events
        // a full update (hosts + keyspaces + tokens) is going to be made
        this._queue.mainEvent = event;
      }
      if (this._queue.callbacks.length === _queueOverflowThreshold) {
        // warn once
        this._logger('warn', util.format('Event debouncer queue exceeded %d events', _queueOverflowThreshold));
      }
      this._queue.callbacks.push(event.callback);
      if (this._queue.mainEvent) {
        // a full refresh is scheduled and the callback was added, nothing else to do.
        return this._slideDelay(delay);
      }
      // Insert at keyspace level
      let keyspaceEvents = this._queue.keyspaces[event.keyspace];
      if (!keyspaceEvents) {
        keyspaceEvents = this._queue.keyspaces[event.keyspace] = { events: [] };
      }
      if (event.cqlObject === undefined) {
        // a full refresh of the keyspace, supersedes all child keyspace events
        keyspaceEvents.mainEvent = event;
      }
      keyspaceEvents.events.push(event);
      this._slideDelay(delay);
    });
  }

  /**
   * @param {Number} delay
   * @private
   * */
  _slideDelay(delay) {
    const self = this;
    function process() {
      const q = self._queue;
      self._queue = null;
      self._timeout = null;
      processQueue(q);
    }
    if (delay === 0) {
      // no delay, process immediately
      if (this._timeout) {
        clearTimeout(this._timeout);
      }
      return process();
    }
    const previousTimeout = this._timeout;
    // Add the new timeout before removing the previous one performs better
    this._timeout = setTimeout(process, delay);
    if (previousTimeout) {
      clearTimeout(previousTimeout);
    }
  }

  /**
   * Clears the timeout and invokes all pending callback.
   */
  shutdown() {
    if (!this._queue) {
      return;
    }
    this._queue.callbacks.forEach(function (cb) {
      cb();
    });
    this._queue = null;
    clearTimeout(this._timeout);
    this._timeout = null;
  }
}

/**
 * @param {{callbacks: Array, keyspaces: Object, mainEvent: Object}} q
 * @private
 */
function processQueue (q) {
  if (q.mainEvent) {
    // refresh all by invoking 1 handler and invoke all pending callbacks
    return promiseUtils.toCallback(q.mainEvent.handler(), (err) => {
      for (let i = 0; i < q.callbacks.length; i++) {
        q.callbacks[i](err);
      }
    });
  }

  utils.each(Object.keys(q.keyspaces), function eachKeyspace(name, next) {
    const keyspaceEvents = q.keyspaces[name];
    if (keyspaceEvents.mainEvent) {
      // refresh a keyspace
      return promiseUtils.toCallback(keyspaceEvents.mainEvent.handler(), function mainEventCallback(err) {
        for (let i = 0; i < keyspaceEvents.events.length; i++) {
          keyspaceEvents.events[i].callback(err);
        }

        next();
      });
    }

    // deal with individual handlers and callbacks
    keyspaceEvents.events.forEach(event => {
      // sync handlers
      event.handler();
      event.callback();
    });

    next();
  });
}

module.exports = EventDebouncer;