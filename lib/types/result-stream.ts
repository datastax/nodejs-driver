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

const { Readable } = require('stream');
const utils = require('../utils');
const errors = require('../errors');
const clientOptions = require('../client-options');

/** @module types */
/**
 * Readable stream using to yield data from a result or a field
 */
class ResultStream extends Readable {
  constructor(opt) {
    super(opt);
    this.buffer = [];
    this.paused = true;
    this._cancelAllowed = false;
    this._handlersObject = null;
    this._highWaterMarkRows = 0;
  }

  _read() {
    this.paused = false;
    if (this.buffer.length === 0) {
      this._readableState.reading = false;
    }
    while (!this.paused && this.buffer.length > 0) {
      this.paused = !this.push(this.buffer.shift());
    }
    this._checkBelowHighWaterMark();
    if (!this.paused && !this.buffer.length && this._readNext) {
      this._readNext();
      this._readNext = null;
    }
  }

  /**
   * Allows for throttling, helping nodejs keep the internal buffers reasonably sized.
   * @param {function} readNext function that triggers reading the next result chunk
   * @ignore
   */
  _valve(readNext) {
    this._readNext = null;
    if (!readNext) {
      return;
    }
    if (this.paused || this.buffer.length) {
      this._readNext = readNext;
    }
    else {
      readNext();
    }
  }

  add(chunk) {
    const length = this.buffer.push(chunk);
    this.read(0);
    this._checkAboveHighWaterMark();
    return length;
  }

  _checkAboveHighWaterMark() {
    if (!this._handlersObject || !this._handlersObject.resumeReadingHandler) {
      return;
    }
    if (this._highWaterMarkRows === 0 || this.buffer.length !== this._highWaterMarkRows) {
      return;
    }
    this._handlersObject.resumeReadingHandler(false);
  }

  _checkBelowHighWaterMark() {
    if (!this._handlersObject || !this._handlersObject.resumeReadingHandler) {
      return;
    }
    if (this._highWaterMarkRows === 0 || this.buffer.length >= this._highWaterMarkRows) {
      return;
    }
    // The consumer has dequeued below the watermark
    this._handlersObject.resumeReadingHandler(true);
  }

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
  cancel(callback) {
    if (!this._cancelAllowed) {
      const err = new Error('You can only cancel streaming executions when continuous paging is enabled');
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
  }

  /**
   * Sets the pointer to the handler to be used to cancel the continuous page execution.
   * @param options
   * @internal
   * @ignore
   */
  setHandlers(options) {
    if (!options.continuousPaging) {
      return;
    }
    this._cancelAllowed = true;
    this._handlersObject = options;
    this._highWaterMarkRows =
      options.continuousPaging.highWaterMarkRows || clientOptions.continuousPageDefaultHighWaterMark;
  }
}

module.exports = ResultStream;