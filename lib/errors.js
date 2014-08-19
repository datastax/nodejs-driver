var util = require('util');

function NoHostAvailableError() {
  this.message = "All host(s) tried for query failed";
}

util.inherits(NoHostAvailableError, Error);

exports.NoHostAvailableError = NoHostAvailableError;