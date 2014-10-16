var util = require('util');

var types = require('./types.js');
/** @module errors */
/**
 * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
 * @param {Object} innerErrors An object map containing the error per host tried
 * @param {String} [message]
 * @constructor
 */
function NoHostAvailableError(innerErrors, message) {
  this.innerErrors = innerErrors;
  this.message = message;
  if (!message) {
    this.message = 'All host(s) tried for query failed.';
    if (innerErrors) {
      var hostList = Object.keys(innerErrors);
      if (hostList.length > 0) {
        var host = hostList[0];
        this.message += util.format(' First host tried, %s: %s. See innerErrors.', host, innerErrors[host]);
      }
    }
  }
}

util.inherits(NoHostAvailableError, Error);

/**
 * Represents an error message from the server
 * @param {Number} code Cassandra exception code
 * @param {String} message
 * @constructor
 */
function ResponseError(code, message) {
  ResponseError.super_.call(this, message, this.constructor);
  this.code = code;
  this.info = 'Represents an error message from the server';
}

util.inherits(ResponseError, types.DriverError);

/**
 * Represents a bug inside the driver or in a Cassandra host.
 * @param {String} message
 * @constructor
 */
function DriverInternalError(message) {
  DriverInternalError.super_.call(this, message, this.constructor);
  this.info = 'Represents a bug inside the driver or in a Cassandra host.';
}

util.inherits(DriverInternalError, types.DriverError);

/**
 * Represents an error when trying to authenticate with auth-enabled host
 * @param {String} message
 * @constructor
 */
function AuthenticationError(message) {
  AuthenticationError.super_.call(this, message, this.constructor);
  this.info = 'Represents an authentication error from the driver or from a Cassandra node.';
}
util.inherits(AuthenticationError, types.DriverError);

exports.AuthenticationError = AuthenticationError;
exports.DriverInternalError = DriverInternalError;
exports.NoHostAvailableError = NoHostAvailableError;
exports.ResponseError = ResponseError;