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
   * @param {QueryOptions} options
   * @param {Function} callback
   */
  constructor(request, options, callback) {
    this.request = request;
    this._options = options;
    this._rowCallback = options && options.rowCallback;
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
   * @param {Number} defaultReadTimeout
   * @param {String} address
   * @param {Function} onTimeout The callback to be invoked when it times out.
   * @param {Function} onResponse The callback to be invoked if a response is obtained after it timed out.
   */
  setRequestTimeout(defaultReadTimeout, address, onTimeout, onResponse) {
    if (this._state !== state.init) {
      // No need to set the timeout
      return;
    }
    const millis = (this._options && this._options.readTimeout !== undefined) ?
      this._options.readTimeout : defaultReadTimeout;
    if (!(millis > 0)) {
      // Read timeout disabled
      return;
    }
    const self = this;
    this._timeout = setTimeout(function requestTimedOut() {
      onTimeout();
      const message = util.format('The host %s did not reply before timeout %d ms', address, millis);
      self._markAsTimedOut(new errors.OperationTimedOutError(message), onResponse);
    }, millis);
  }

  setResultRow(row, meta, rowLength, flags) {
    this._markAsCompleted();
    if (!this._rowCallback) {
      return this.setResult(new errors.DriverInternalError('RowCallback not found for streaming frame handler'));
    }
    this._rowCallback(this._rowIndex++, row, rowLength);
    if (this._rowIndex === rowLength) {
      this._swapCallbackAndInvoke(null, { rowLength: rowLength, meta: meta, flags: flags });
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
    this._swapCallbackAndInvoke(err, null, onResponse);
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
   */
  setResult(err, result) {
    this._markAsCompleted();
    this._swapCallbackAndInvoke(err, result);
  }

  _swapCallbackAndInvoke(err, result, newCallback) {
    const callback = this._callback;
    this._callback = newCallback || utils.noop;
    callback(err, result);
  }
}

module.exports = OperationState;