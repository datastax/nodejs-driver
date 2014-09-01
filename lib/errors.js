var util = require('util');
var types = require('./types.js');

function NoHostAvailableError(innerErrors) {
  this.message = "All host(s) tried for query failed";
  this.innerErrors = innerErrors;
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

exports.DriverInternalError = DriverInternalError;
exports.NoHostAvailableError = NoHostAvailableError;
exports.ResponseError = ResponseError;