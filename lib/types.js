var opcodes = {
  error: 0x00,
  startup: 0x01,
  ready: 0x02,
  authenticate: 0x03,
  credentials: 0x04,
  options: 0x05,
  supported: 0x06,
  query: 0x07,
  result: 0x08,
  prepare: 0x09,
  execute: 0x0a,
  register: 0x0b,
  event: 0x0c
};

var consistencies = {
  any: 0,
  one: 1,
  two: 2,
  three: 3,
  quorum: 4,
  all: 5,
  local_quorum: 6,
  each_quorum: 7
};
var responseErrorCodes = {
  serverError: 0x0000,
  protocolError: 0x000A,
  badCredentials: 0x0100,
  unavailableException: 0x1000,
  overloaded: 0x1001,
  isBootstrapping: 0x1002,
  truncateError: 0x1003,
  writeTimeout: 0x1100,
  readTimeout: 0x1200,
  syntaxError: 0x2000,
  unauthorized: 0x2100,
  invalid: 0x2200,
  configError: 0x2300,
  alreadyExists: 0x2400,
  unprepared: 0x2500
};

function QueryLiteral(value) {
  this.value = value;
}
QueryLiteral.prototype.toString = function () {
  return this.value.toString();
}

exports.opcodes = opcodes;
exports.consistencies = consistencies;
exports.responseErrorCodes = responseErrorCodes;
exports.QueryLiteral = QueryLiteral;