var util = require('util');
var stream = require('stream');
var async = require('async');
var utils = require('./utils.js');
var uuidGenerator = require('node-uuid');

//instances

/**
 * Consistency levels
 */
var consistencies = {
  any:          0x00,
  one:          0x01,
  two:          0x02,
  three:        0x03,
  quorum:       0x04,
  all:          0x05,
  localQuorum:  0x06,
  eachQuorum:   0x07,
  localOne:     0x10,
  getDefault: function () {
    return this.quorum;
  }
};

/**
 * CQL data types
 */
var dataTypes = {
  custom:     0x0000,
  ascii:      0x0001,
  bigint:     0x0002,
  blob:       0x0003,
  boolean:    0x0004,
  counter:    0x0005,
  decimal:    0x0006,
  double:     0x0007,
  float:      0x0008,
  int:        0x0009,
  text:       0x000a,
  timestamp:  0x000b,
  uuid:       0x000c,
  varchar:    0x000d,
  varint:     0x000e,
  timeuuid:   0x000f,
  inet:       0x0010,
  list:       0x0020,
  map:        0x0021,
  set:        0x0022,
  getByName:  function(name) {
    var typeInfo = { name: name.toLowerCase() };
    var listMatches = /^(list|set)<(\w+)>$/.exec(typeInfo.name);
    if (listMatches) {
      typeInfo.name = listMatches[1];
      typeInfo.subtype = listMatches[2];
    }
    typeInfo.type = this[typeInfo.name];
    if (typeof typeInfo.type !== 'number') {
      throw new TypeError('Datatype with name ' + name + ' not valid', null);
    }
    return typeInfo;
  }
};

/**
 * An integer byte that distinguish the actual message from and to Cassandra
 */
var opcodes = {
  error:          0x00,
  startup:        0x01,
  ready:          0x02,
  authenticate:   0x03,
  options:        0x05,
  supported:      0x06,
  query:          0x07,
  result:         0x08,
  prepare:        0x09,
  execute:        0x0a,
  register:       0x0b,
  event:          0x0c,
  batch:          0x0d,
  auth_challenge: 0x0e,
  auth_response:  0x0f,
  auth_success:   0x10,

  /**
   * Determines if the code is a valid opcode
   */
  isInRange: function (code) {
    return code > this.error && code > this.event;
  }
};

/**
 * Parses a string query and stringifies the parameters
 */
var queryParser = {
  /**
   * Replaced the query place holders with the stringified value
   * @param {String} query
   * @param {Array} params
   * @param {Function} stringifier
   */
  parse: function (query, params, stringifier) {
    if (!query || !query.length || !params) {
      return query;
    }
    if (!stringifier) {
      stringifier = function (a) {return a.toString()};
    }
    var parts = [];
    var isLiteral = false;
    var lastIndex = 0;
    var paramsCounter = 0;
    for (var i = 0; i < query.length; i++) {
      var char = query.charAt(i);
      if (char === "'" && query.charAt(i-1) !== '\\') {
        //opening or closing quotes in a literal value of the query
        isLiteral = !isLiteral;
      }
      if (!isLiteral && char === '?') {
        //is a placeholder
        parts.push(query.substring(lastIndex, i));
        parts.push(stringifier(params[paramsCounter++]));
        lastIndex = i+1;
      }
    }
    parts.push(query.substring(lastIndex));
    return parts.join('');
  }
};

/**
 * Server error codes returned by Cassandra
 */
var responseErrorCodes = {
  serverError:            0x0000,
  protocolError:          0x000A,
  badCredentials:         0x0100,
  unavailableException:   0x1000,
  overloaded:             0x1001,
  isBootstrapping:        0x1002,
  truncateError:          0x1003,
  writeTimeout:           0x1100,
  readTimeout:            0x1200,
  syntaxError:            0x2000,
  unauthorized:           0x2100,
  invalid:                0x2200,
  configError:            0x2300,
  alreadyExists:          0x2400,
  unprepared:             0x2500
};

/**
 * Type of result included in a response
 */
var resultKind = {
  voidResult:      0x0001,
  rows:            0x0002,
  setKeyspace:     0x0003,
  prepared:        0x0004,
  schemaChange:    0x0005
};

/**
 * Generates and returns a RFC4122 v1 (timestamp based) UUID.
 * Uses node-uuid module as generator.
 */
function timeuuid(options, buffer, offset) {
  return uuidGenerator.v1(options, buffer, offset);
}

/**
 * Generate and return a RFC4122 v4 UUID.
 * Uses node-uuid module as generator.
 */
function uuid(options, buffer, offset) {
  return uuidGenerator.v4(options, buffer, offset);
}

//classes

/**
 * Represents a frame header that could be used to read from a Buffer or to write to a Buffer
 */
function FrameHeader(values) {
  if (values) {
    if (values instanceof Buffer) {
      this.fromBuffer(values);
    }
    else {
      for (var prop in values) {
        this[prop] = values[prop];
      }
    }
  }
}

/**
 * The length of the header of the protocol
 */
FrameHeader.size = 8;
FrameHeader.prototype.version = 2;
FrameHeader.prototype.flags = 0x0;
FrameHeader.prototype.streamId = null;
FrameHeader.prototype.opcode = null;
FrameHeader.prototype.bodyLength = 0;

FrameHeader.prototype.fromBuffer = function (buf) {
  if (buf.length < FrameHeader.size) {
    //there is not enough data to read the header
    return;
  }
  this.bufferLength = buf.length;
  this.isResponse = buf[0] & 0x80;
  this.version = buf[0] & 0x7F;
  this.flags = buf.readUInt8(1);
  this.streamId = buf.readInt8(2);
  this.opcode = buf.readUInt8(3);
  this.bodyLength = buf.readUInt32BE(4);
};

FrameHeader.prototype.toBuffer = function () {
  var buf = new Buffer(FrameHeader.size);
  buf.writeUInt8(0 + this.version, 0);
  buf.writeUInt8(this.flags, 1);
  buf.writeUInt8(this.streamId, 2);
  buf.writeUInt8(this.opcode, 3);
  buf.writeUInt32BE(this.bodyLength, 4);
  return buf;
};

/**
 * Long constructor, wrapper of the internal library used.
 */
var Long = require('long');
/**
 * Returns a long representation.
 * Used internally for deserialization
 */
Long.fromBuffer = function (value) {
  if (!(value instanceof Buffer)) {
    throw new TypeError('Expected Buffer', value, Buffer);
  }
  return new Long(value.readInt32BE(4), value.readInt32BE(0, 4));
};

/**
 * Returns a big-endian buffer representation of the Long instance
 * @param {Long} value
 */
Long.toBuffer = function (value) {
  if (!(value instanceof Long)) {
    throw new TypeError('Expected Long', value, Long);
  }
  var buffer = new Buffer(8);
  buffer.writeUInt32BE(value.getHighBitsUnsigned(), 0);
  buffer.writeUInt32BE(value.getLowBitsUnsigned(), 4);
  return buffer;
};

/**
 * Wraps a value to be included as literal in a query
 */
function QueryLiteral (value) {
  this.value = value;
}

QueryLiteral.prototype.toString = function () {
  return this.value.toString();
};

/**
 * Queues callbacks while the condition tests true. Similar behaviour as async.whilst.
 */
function QueueWhile(test, delayRetry) {
  this.queue = async.queue(function (task, queueCallback) {
    async.whilst(
      test,
      function(cb) {
        //Retry in a while
        if (delayRetry) {
          setTimeout(cb, delayRetry);
        }
        else {
          setImmediate(cb);
        }
      },
      function() {
        queueCallback(null, null);
      }
    );
  }, 1);
}

QueueWhile.prototype.push = function (callback) {
  this.queue.push({}, callback);
};

/**
 * Readable stream using to yield data from a result or a field
 */
function ResultStream(opt) {
  stream.Readable.call(this, opt);
  this.buffer = [];
  this.paused = true;
}

util.inherits(ResultStream, stream.Readable);

ResultStream.prototype._read = function() {
  this.paused = false;
  if (this.buffer.length === 0) {
    this._readableState.reading = false;
  }
  while (!this.paused && this.buffer.length > 0) {
    this.paused = this.push(this.buffer.shift());
  }
};

ResultStream.prototype.add = function (chunk) {
  this.buffer.push(chunk);
  this.read(0);
};

/**
 * Represents a result row
 */
function Row(columns) {
  this.columns = columns;
}

/**
 * Returns the cell value.
 * Created for backward compatibility: use row[columnName] instead.
 * @param {String|Number} columnName Name or index of the column
 */
Row.prototype.get = function (columnName) {
  if (typeof columnName === 'number') {
    if (this.columns && this.columns[columnName]) {
      columnName = this.columns[columnName].name;
    }
    else {
      throw new Error('Column not found');
    }
  }
  return this[columnName];
};

//error classes

/**
 * Base Error
 */
function DriverError (message, constructor) {
  if (constructor) {
    Error.captureStackTrace(this, constructor);
    this.name = constructor.name;
  }
  this.message = message || 'Error';
  this.info = 'Cassandra Driver Error';
}
util.inherits(DriverError, Error);


function QueryParserError(e) {
  QueryParserError.super_.call(this, e.message, this.constructor);
  this.internalError = e;
}
util.inherits(QueryParserError, DriverError);

function TimeoutError (message) {
  TimeoutError.super_.call(this, message, this.constructor);
  this.info = 'Represents an error that happens when the maximum amount of time for an operation passed.';
}
util.inherits(TimeoutError, DriverError);

function TypeError (message, value, expectedType, actualType, reference) {
  if (!message) {
    message = 'Type not supported for operation';
  }
  TimeoutError.super_.call(this, message, this.constructor);
  this.value = value;
  this.info = 'Represents an error that happens when trying to convert from one type to another.';
  if (expectedType) {
    this.expectedType = expectedType;
  }
  if (this.actualType) {
    this.actualType = actualType;
  }
  if (this.reference) {
    this.reference = reference;
  }
}
util.inherits(TypeError, DriverError);

exports.opcodes = opcodes;
exports.consistencies = consistencies;
exports.dataTypes = dataTypes;
exports.queryParser = queryParser;
exports.responseErrorCodes = responseErrorCodes;
exports.resultKind = resultKind;
exports.timeuuid = timeuuid;
exports.uuid = uuid;
exports.FrameHeader = FrameHeader;
exports.Long = Long;
exports.QueryLiteral = QueryLiteral;
exports.QueueWhile = QueueWhile;
exports.ResultStream = ResultStream;
exports.Row = Row;
exports.DriverError = DriverError;
exports.TimeoutError = TimeoutError;