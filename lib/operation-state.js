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
const utils = require('./utils');
const errors = require('./errors');
const requests = require('./requests');
const ExecuteRequest = requests.ExecuteRequest;
const QueryRequest = requests.QueryRequest;

const state = {
  init: 0,
  completed: 1,
  timedOut: 2,
  cancelled: 3
};

/**
 * Maintains the state information of a request inside a Connection.
 */
class OperationState {
  /**
   * Creates a new instance of OperationState.
   * @param {Request} request
   * @param {Function} rowCallback
   * @param {Function} callback
   */
  constructor(request, rowCallback, callback) {
    this.request = request;
    this._rowCallback = rowCallback;
    this._callback = callback;
    this._timeout = null;
    this._state = state.init;
    this._rowIndex = 0;
    /**
     * Stream id that is set right before being written.
     * @type {number}
     */
    this.streamId = -1;
  }

  /**
   * Marks the operation as cancelled, clearing all callbacks and timeouts.
   */
  cancel() {
    if (this._state !== state.init) {
      return;
    }
    if (this._timeout !== null) {
      clearTimeout(this._timeout);
    }
    this._state = state.cancelled;
    this._callback = utils.noop;
  }

  /**
   * Determines if the operation can be written to the wire (when it hasn't been cancelled or it hasn't timed out).
   */
  canBeWritten() {
    return this._state === state.init;
  }

  /**
   * Determines if the response is going to be yielded by row.
   * @return {boolean}
   */
  isByRow() {
    return this._rowCallback && (this.request instanceof ExecuteRequest || this.request instanceof QueryRequest);
  }

  /**
   * Creates the timeout for the request.
   * @param {ExecutionOptions} execOptions
   * @param {Number} defaultReadTimeout
   * @param {String} address
   * @param {Function} onTimeout The callback to be invoked when it times out.
   * @param {Function} onResponse The callback to be invoked if a response is obtained after it timed out.
   */
  setRequestTimeout(execOptions, defaultReadTimeout, address, onTimeout, onResponse) {
    if (this._state !== state.init) {
      // No need to set the timeout
      return;
    }
    const millis = execOptions.getReadTimeout() !== undefined ? execOptions.getReadTimeout() : defaultReadTimeout;
    if (!(millis > 0)) {
      // Read timeout disabled
      return;
    }
    const self = this;
    this._timeout = setTimeout(function requestTimedOut() {
      onTimeout();
      const message = util.format('The host %s did not reply before timeout %d ms', address, millis);
      self._markAsTimedOut(new errors.OperationTimedOutError(message, address), onResponse);
    }, millis);
  }

  setResultRow(row, meta, rowLength, flags, header) {
    this._markAsCompleted();
    if (!this._rowCallback) {
      return this.setResult(new errors.DriverInternalError('RowCallback not found for streaming frame handler'));
    }
    this._rowCallback(this._rowIndex++, row, rowLength);
    if (this._rowIndex === rowLength) {
      this._swapCallbackAndInvoke(null, { rowLength: rowLength, meta: meta, flags: flags }, header.bodyLength);
    }
  }

  /**
   * Marks the current operation as timed out.
   * @param {Error} err
   * @param {Function} onResponse
   * @private
   */
  _markAsTimedOut(err, onResponse) {
    if (this._state !== state.init) {
      return;
    }
    this._state = state.timedOut;
    this._swapCallbackAndInvoke(err, null, null, onResponse);
  }

  _markAsCompleted() {
    if (this._state !== state.init) {
      return;
    }
    if (this._timeout !== null) {
      clearTimeout(this._timeout);
    }
    this._state = state.completed;
  }

  /**
   * Sets the result of this operation, declaring that no further input will be processed for this operation.
   * @param {Error} err
   * @param {Object} [result]
   * @param {Number} [length]
   */
  setResult(err, result, length) {
    this._markAsCompleted();
    this._swapCallbackAndInvoke(err, result, length);
  }

  _swapCallbackAndInvoke(err, result, length, newCallback) {
    const callback = this._callback;
    this._callback = newCallback || utils.noop;
    callback(err, result, length);
  }
}

module.exports = OperationState;