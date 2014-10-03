var util = require('util');
var os = require('os');

var types = require('./types.js');

function NoHostAvailableError(innerErrors, message) {
  this.innerErrors = innerErrors;
  this.message = message;
  if (!message) {
    this.message = 'All host(s) tried for query failed.';
    if (innerErrors) {
      var host = Object.keys(innerErrors)[0];
      this.message += os.EOL + 'First error, host ' + host + ': ' + innerErrors[host];
    }
  }
}

util.inherits(NoHostAvailableError, Error);

function ResponseError(code, message) {
  ResponseError.super_.call(this, message, this.constructor);
  this.code = code;
  this.info = 'Represents a error message from the server';
}

util.inherits(ResponseError, types.DriverError);

function DriverInternalError(message) {
  DriverInternalError.super_.call(this, message, this.constructor);
  this.info = 'Represents a bug inside the driver or in a Cassandra host.';
}

util.inherits(DriverInternalError, types.DriverError);

function AuthenticationError(message) {
  AuthenticationError.super_.call(this, message, this.constructor);
  this.info = 'Represents an authentication error from the driver or from a Cassandra node.';
}
util.inherits(AuthenticationError, types.DriverError);

exports.AuthenticationError = AuthenticationError;
exports.DriverInternalError = DriverInternalError;
exports.NoHostAvailableError = NoHostAvailableError;
exports.ResponseError = ResponseError;