"use strict";

var util = require('util');
var utils = require('../utils');

var _queueOverflowThreshold = 1000;

/**
 * Debounce protocol events by acting on those events with a sliding delay.
 * @param {Number} delay
 * @param {Function} logger
 * @ignore
 * @constructor
 */
function EventDebouncer(delay, logger) {
  this._delay = delay;
  this._logger = logger;
  this._queue = null;
  this._timeout = null;
}

/**
 * Adds a new event to the queue and moves the delay.
 * @param {{ handler: Function, all: boolean|undefined, keyspace: String|undefined, cqlObject: String|null|undefined,
 * callback: Function|undefined }} event
 * @param {Boolean} processNow
 */
EventDebouncer.prototype.eventReceived = function (event, processNow) {
  event.callback = event.callback || utils.noop;
  this._queue = this._queue || { callbacks: [], keyspaces: {} };
  var delay = !processNow ? this._delay : 0;
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
  var keyspaceEvents = this._queue.keyspaces[event.keyspace];
  if (!keyspaceEvents) {
    keyspaceEvents = this._queue.keyspaces[event.keyspace] = { events: [] };
  }
  if (event.cqlObject === undefined) {
    // a full refresh of the keyspace, supersedes all child keyspace events
    keyspaceEvents.mainEvent = event;
  }
  keyspaceEvents.events.push(event);
  this._slideDelay(delay);
};

/**
 * @param {Number} delay
 * @private
 * */
EventDebouncer.prototype._slideDelay = function (delay) {
  var self = this;
  function process() {
    var q = self._queue;
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
  var previousTimeout = this._timeout;
  // add the new timeout before removing the previous one performs better
  this._timeout = setTimeout(process, delay);
  if (previousTimeout) {
    clearTimeout(previousTimeout);
  }
};

/**
 * Clears the timeout and invokes all pending callback.
 */
EventDebouncer.prototype.shutdown = function () {
  if (!this._queue) {
    return;
  }
  this._queue.callbacks.forEach(function (cb) {
    cb();
  });
  this._queue = null;
  clearTimeout(this._timeout);
  this._timeout = null;
};

/**
 * @param {{callbacks: Array, keyspaces: Object, mainEvent: Object}} q
 * @private
 */
function processQueue (q) {
  if (q.mainEvent) {
    // refresh all by invoking 1 handler and invoke all pending callbacks
    return q.mainEvent.handler(function invokeCallbacks(err) {
      for (var i = 0; i < q.callbacks.length; i++) {
        q.callbacks[i](err);
      }
    });
  }
  utils.each(Object.keys(q.keyspaces), function eachKeyspace(name, next) {
    var keyspaceEvents = q.keyspaces[name];
    if (keyspaceEvents.mainEvent) {
      // refresh a keyspace
      return keyspaceEvents.mainEvent.handler(function mainEventCallback(err) {
        for (var i = 0; i < keyspaceEvents.events.length; i++) {
          keyspaceEvents.events[i].callback(err);
        }
        next();
      });
    }
    // deal with individual handlers and callbacks
    keyspaceEvents.events.forEach(function eachEvent(event) {
      // sync handlers
      event.handler();
      event.callback();
    });
    next();
  });
}

module.exports = EventDebouncer;