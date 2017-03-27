/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var stream = require('stream');
var utils = require('../utils');
var errors = require('../errors');
var clientOptions = require('../client-options');

/** @module types */
/**
 * Readable stream using to yield data from a result or a field
 * @constructor
 */
function ResultStream(opt) {
  stream.Readable.call(this, opt);
  this.buffer = [];
  this.paused = true;
  this._cancelAllowed = false;
  this._handlersObject = null;
  this._highWaterMarkRows = 0;
}

util.inherits(ResultStream, stream.Readable);

ResultStream.prototype._read = function() {
  this.paused = false;
  if (this.buffer.length === 0) {
    this._readableState.reading = false;
  }
  while (!this.paused && this.buffer.length > 0) {
    this.paused = !this.push(this.buffer.shift());
  }
  this._checkBelowHighWaterMark();
  if ( !this.paused && !this.buffer.length && this._readNext ) {
    this._readNext();
    this._readNext = null;
  }
};

/**
 * Allows for throttling, helping nodejs keep the internal buffers reasonably sized.
 * @param {function} readNext function that triggers reading the next result chunk
 * @ignore
 */
ResultStream.prototype._valve = function( readNext ) {
  this._readNext = null;
  if ( !readNext ) {
    return;
  }
  if ( this.paused || this.buffer.length ) {
    this._readNext = readNext;
  }
  else {
    readNext();
  }
};

ResultStream.prototype.add = function (chunk) {
  var length = this.buffer.push(chunk);
  this.read(0);
  this._checkAboveHighWaterMark();
  return length;
};

ResultStream.prototype._checkAboveHighWaterMark = function () {
  if (!this._handlersObject || !this._handlersObject.resumeReadingHandler) {
    return;
  }
  if (this._highWaterMarkRows === 0 || this.buffer.length !== this._highWaterMarkRows) {
    return;
  }
  this._handlersObject.resumeReadingHandler(false);
};

ResultStream.prototype._checkBelowHighWaterMark = function () {
  if (!this._handlersObject || !this._handlersObject.resumeReadingHandler) {
    return;
  }
  if (this._highWaterMarkRows === 0 || this.buffer.length >= this._highWaterMarkRows) {
    return;
  }
  // The consumer has dequeued below the watermark
  this._handlersObject.resumeReadingHandler(true);
};

/**
 * When continuous paging is enabled, allows the client to notify to the server to stop pushing further pages.
 * <p>Note: This is not part of the public API yet.</p>
 * @param {Function} [callback] The cancel method accepts an optional callback.
 * @example <caption>Cancelling a continuous paging execution</caption>
 * const stream = client.stream(query, params, { prepare: true, continuousPaging: true });
 * // ...
 * // Ask the server to stop pushing rows.
 * stream.cancel();
 * @ignore
 */
ResultStream.prototype.cancel = function (callback) {
  if (!this._cancelAllowed) {
    var err = new Error('You can only cancel streaming executions when continuous paging is enabled');
    if (!callback) {
      throw err;
    }
    return callback(err);
  }
  if (!this._handlersObject) {
    throw new errors.DriverInternalError('ResultStream cancel is allowed but the cancel options were not set');
  }
  callback = callback || utils.noop;
  if (!this._handlersObject.cancelHandler) {
    // The handler is not yet set
    // Set the callback as a flag to identify that the cancel handler must be invoked when set
    this._handlersObject.cancelHandler = callback;
    return;
  }
  this._handlersObject.cancelHandler(callback);
};

/**
 * Sets the pointer to the handler to be used to cancel the continuous page execution.
 * @param options
 * @internal
 * @ignore
 */
ResultStream.prototype.setHandlers = function (options) {
  if (!options.continuousPaging) {
    return;
  }
  this._cancelAllowed = true;
  this._handlersObject = options;
  this._highWaterMarkRows =
    options.continuousPaging.highWaterMarkRows || clientOptions.continuousPageDefaultHighWaterMark;
};

module.exports = ResultStream;