'use strict';

/**
 * Represents an execution of a Request against a single
 * @param {RequestHandler} parent
 * @constructor
 */
function RequestExecution(parent) {
  this._parent = parent;
  /** @type {OperationState} */
  this._operation = null;
}

RequestExecution.prototype.start = function (hostIterator) {
  //TODO: Define who deals with LBP get next host
  //TODO: Define who creates the newQueryPlan() (only should run once), probably on handler.send()
  //this._operation = connection.sendStream();
};

/**
 * Allows the handler to cancel the current request.
 * When the request has been already written, we can unset the callback and forget about it.
 */
RequestExecution.prototype.cancel = function () {
  //TODO OK
  this._operation.cancel();
};

RequestExecution.prototype._getNextConnection = function (callback) {
  //TODO
};

/**
 * Synchronously iterates through the query plan.
 * @returns {Host}
 * @private
 */
RequestExecution.prototype._getNextHost = function () {
  //TODO: Move from _iterateThroughHosts
};

RequestExecution.prototype._getPooledConnection = function () {
  //TODO: Move from RequestHandler#_getPooledConnection()
};

RequestExecution.prototype._handleError = function (err, callback) {

};

module.exports = RequestExecution;