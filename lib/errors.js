var util = require('util');
/** @module errors */
/**
 * Base Error
 * @private
 */
function DriverError (message, constructor) {
  if (constructor) {
    this.name = constructor.name;
    this.stack = (new Error(message)).stack;
  }
  this.message = message || 'Error';
  this.info = 'Cassandra Driver Error';
}
util.inherits(DriverError, Error);
/**
 * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
 * @param {Object} innerErrors An object map containing the error per host tried
 * @param {String} [message]
 * @constructor
 */
function NoHostAvailableError(innerErrors, message) {
  this.innerErrors = innerErrors;
  this.info = 'Represents an error when a query cannot be performed because no host is available or could be reached by the driver.';
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

util.inherits(NoHostAvailableError, DriverError);

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

util.inherits(ResponseError, DriverError);

/**
 * Represents a bug inside the driver or in a Cassandra host.
 * @param {String} message
 * @constructor
 */
function DriverInternalError(message) {
  DriverInternalError.super_.call(this, message, this.constructor);
  this.info = 'Represents a bug inside the driver or in a Cassandra host.';
}

util.inherits(DriverInternalError, DriverError);

/**
 * Represents an error when trying to authenticate with auth-enabled host
 * @param {String} message
 * @constructor
 */
function AuthenticationError(message) {
  AuthenticationError.super_.call(this, message, this.constructor);
  this.info = 'Represents an authentication error from the driver or from a Cassandra node.';
}

util.inherits(AuthenticationError, DriverError);

/**
 * Represents an error that is raised when one of the arguments provided to a method is not valid
 * @param {String} message
 * @constructor
 */
function ArgumentError(message) {
  ArgumentError.super_.call(this, message, this.constructor);
  this.info = 'Represents an error that is raised when one of the arguments provided to a method is not valid.';
}

util.inherits(ArgumentError, DriverError);

/**
 * Represents a client-side error that is raised when the client didn't hear back from the server within
 * {@link ClientOptions.socketOptions.readTimeout}.
 * @constructor
 */
function OperationTimedOutError(message) {
  OperationTimedOutError.super_.call(this, message, this.constructor);
  this.info = 'Represents a client-side error that is raised when the client did not hear back from the server ' +
    'within socketOptions.readTimeout';
}

util.inherits(OperationTimedOutError, DriverError);

exports.ArgumentError = ArgumentError;
exports.AuthenticationError = AuthenticationError;
exports.DriverError = DriverError;
exports.OperationTimedOutError = OperationTimedOutError;
exports.DriverInternalError = DriverInternalError;
exports.NoHostAvailableError = NoHostAvailableError;
exports.ResponseError = ResponseError;