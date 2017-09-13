'use strict';

var util = require('util');
var utils = require('./utils');
var errors = require('./errors');
var requests = require('./requests');
var ExecuteRequest = requests.ExecuteRequest;
var QueryRequest = requests.QueryRequest;

var state = {
  init: 0,
  completed: 1,
  timedOut: 2,
  cancelled: 3
};

/**
 * Maintains the state information of a request inside a Connection.
 * @param {Request} request
 * @param {QueryOptions} options
 * @param {Function} callback
 */
function OperationState(request, options, callback) {
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
OperationState.prototype.cancel = function () {
  if (this._state !== state.init) {
    return;
  }
  if (this._timeout !== null) {
    clearTimeout(this._timeout);
  }
  this._state = state.cancelled;
  this._callback = utils.noop;
};

/**
 * Determines if the response is going to be yielded by row.
 * @return {boolean}
 */
OperationState.prototype.isByRow = function () {
  return this._rowCallback && (this.request instanceof ExecuteRequest || this.request instanceof QueryRequest);
};

/**
 * Creates the timeout for the request.
 * @param {Number} defaultReadTimeout
 * @param {String} address
 * @param {Function} onTimeout The callback to be invoked when it times out.
 * @param {Function} onResponse The callback to be invoked if a response is obtained after it timed out.
 */
OperationState.prototype.setRequestTimeout = function (defaultReadTimeout, address, onTimeout, onResponse) {
  if (this._state !== state.init) {
    // No need to set the timeout
    return;
  }
  var millis = (this._options && this._options.readTimeout !== undefined) ?
    this._options.readTimeout : defaultReadTimeout;
  if (!(millis > 0)) {
    // Read timeout disabled
    return;
  }
  var self = this;
  this._timeout = setTimeout(function requestTimedOut() {
    onTimeout();
    var message = util.format('The host %s did not reply before timeout %d ms', address, millis);
    self._markAsTimedOut(new errors.OperationTimedOutError(message), onResponse);
  }, millis);
};

OperationState.prototype.setResultRow = function (row, meta, rowLength, flags) {
  this._markAsCompleted();
  if (!this._rowCallback) {
    return this.setResult(new errors.DriverInternalError('RowCallback not found for streaming frame handler'));
  }
  this._rowCallback(this._rowIndex++, row, rowLength);
  if (this._rowIndex === rowLength) {
    this._swapCallbackAndInvoke(null, { rowLength: rowLength, meta: meta, flags: flags });
  }
};

/**
 * Marks the current operation as timed out.
 * @param {Error} err
 * @param {Function} onResponse
 * @private
 */
OperationState.prototype._markAsTimedOut = function (err, onResponse) {
  if (this._state !== state.init) {
    return;
  }
  this._state = state.timedOut;
  this._swapCallbackAndInvoke(err, null, onResponse);
};

OperationState.prototype._markAsCompleted = function () {
  if (this._state !== state.init) {
    return;
  }
  if (this._timeout !== null) {
    clearTimeout(this._timeout);
  }
  this._state = state.completed;
};

/**
 * Sets the result of this operation, declaring that no further input will be processed for this operation.
 * @param {Error} err
 * @param {Object} [result]
 */
OperationState.prototype.setResult = function (err, result) {
  this._markAsCompleted();
  this._swapCallbackAndInvoke(err, result);
};

OperationState.prototype._swapCallbackAndInvoke = function (err, result, newCallback) {
  var callback = this._callback;
  this._callback = newCallback || utils.noop;
  callback(err, result);
};

module.exports = OperationState;