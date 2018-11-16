"use strict";
const util = require('util');

/**
 * Contains the error classes exposed by the driver.
 * @module errors
 */

/**
 * Base Error
 * @private
 */
function DriverError (message) {
  Error.call(this, message);
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.info = 'Cassandra Driver Error';
  // Explicitly set the message property as the Error.call() doesn't set the property on v8
  this.message = message;
}

util.inherits(DriverError, Error);

/**
 * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
 * @param {Object} innerErrors An object map containing the error per host tried
 * @param {String} [message]
 * @constructor
 */
function NoHostAvailableError(innerErrors, message) {
  DriverError.call(this, message);
  this.innerErrors = innerErrors;
  this.info = 'Represents an error when a query cannot be performed because no host is available or could be reached by the driver.';
  if (!message) {
    this.message = 'All host(s) tried for query failed.';
    if (innerErrors) {
      const hostList = Object.keys(innerErrors);
      if (hostList.length > 0) {
        const host = hostList[0];
        this.message += util.format(' First host tried, %s: %s. See innerErrors.', host, innerErrors[host]);
      }
    }
  }
}

util.inherits(NoHostAvailableError, DriverError);

/**
 * Represents an error message from the server
 * @param {Number} code Cassandra exception code
 * @param {String} message
 * @constructor
 */
function ResponseError(code, message) {
  DriverError.call(this, message);
  /**
   * The error code as defined in [responseErrorCodes]{@link module:types~responseErrorCodes}.
   * @type {Number}
   */
  this.code = code;
  this.info = 'Represents an error message from the server';
}

util.inherits(ResponseError, DriverError);

/**
 * Represents a bug inside the driver or in a Cassandra host.
 * @param {String} message
 * @constructor
 */
function DriverInternalError(message) {
  DriverError.call(this, message);
  this.info = 'Represents a bug inside the driver or in a Cassandra host.';
}

util.inherits(DriverInternalError, DriverError);

/**
 * Represents an error when trying to authenticate with auth-enabled host
 * @param {String} message
 * @constructor
 */
function AuthenticationError(message) {
  DriverError.call(this, message);
  this.info = 'Represents an authentication error from the driver or from a Cassandra node.';
}

util.inherits(AuthenticationError, DriverError);

/**
 * Represents an error that is raised when one of the arguments provided to a method is not valid
 * @param {String} message
 * @constructor
 */
function ArgumentError(message) {
  DriverError.call(this, message);
  this.info = 'Represents an error that is raised when one of the arguments provided to a method is not valid.';
}

util.inherits(ArgumentError, DriverError);

/**
 * Represents a client-side error that is raised when the client didn't hear back from the server within
 * {@link ClientOptions.socketOptions.readTimeout}.
 * @param {String} message The error message.
 * @param {String} [host] Address of the server host that caused the operation to time out.
 * @constructor
 */
function OperationTimedOutError(message, host) {
  DriverError.call(this, message, this.constructor);
  this.info = 'Represents a client-side error that is raised when the client did not hear back from the server ' +
    'within socketOptions.readTimeout';

  /**
   * When defined, it gets the address of the host that caused the operation to time out.
   * @type {String|undefined}
   */
  this.host = host;
}

util.inherits(OperationTimedOutError, DriverError);

/**
 * Represents an error that is raised when a feature is not supported in the driver or in the current Cassandra version.
 * @param message
 * @constructor
 */
function NotSupportedError(message) {
  DriverError.call(this, message, this.constructor);
  this.info = 'Represents a feature that is not supported in the driver or in the Cassandra version.';
}

util.inherits(NotSupportedError, DriverError);

/**
 * Represents a client-side error indicating that all connections to a certain host have reached
 * the maximum amount of in-flight requests supported.
 * @param {String} address
 * @param {Number} maxRequestsPerConnection
 * @param {Number} connectionLength
 * @constructor
 */
function BusyConnectionError(address, maxRequestsPerConnection, connectionLength) {
  const message = util.format('All connections to host %s are busy, %d requests are in-flight on %s',
    address, maxRequestsPerConnection, connectionLength === 1 ? 'a single connection': 'each connection');
  DriverError.call(this, message, this.constructor);
  this.info = 'Represents a client-side error indicating that all connections to a certain host have reached ' +
    'the maximum amount of in-flight requests supported (pooling.maxRequestsPerConnection)';
}

util.inherits(BusyConnectionError, DriverError);

exports.ArgumentError = ArgumentError;
exports.AuthenticationError = AuthenticationError;
exports.BusyConnectionError = BusyConnectionError;
exports.DriverError = DriverError;
exports.OperationTimedOutError = OperationTimedOutError;
exports.DriverInternalError = DriverInternalError;
exports.NoHostAvailableError = NoHostAvailableError;
exports.NotSupportedError = NotSupportedError;
exports.ResponseError = ResponseError;