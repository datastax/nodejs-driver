var util = require('util');
var types = require('./types.js');

function NoHostAvailableError() {
  this.message = "All host(s) tried for query failed";
}

util.inherits(NoHostAvailableError, Error);

function ResponseError(code, message) {
  ResponseError.super_.call(this, message, this.constructor);
  this.code = code;
  this.info = 'Represents a error message from the server';
}

util.inherits(ResponseError, types.DriverError);

exports.NoHostAvailableError = NoHostAvailableError;
exports.ResponseError = ResponseError;