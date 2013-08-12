var util = require('util');
var utils = require('./utils.js');
var Int64 = require('node-int64');
var uuid = require('node-uuid');

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
  event: 0x0c,
  //Represents the maximum number supported by the protocol
  maxCode: 0x0c
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
  set:        0x0022
};

/**
 * Server error codes returned by Cassandra
 */
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

/**
 * Encodes and decodes from a type to Cassandra bytes
 */
var typeEncoder = (function(){
  /**
   * Decodes Cassandra bytes into Javascript values.
   */
  function decode(bytes, type) {
    if (bytes === null) {
      return null;
    }
    switch(type[0]) {
      case dataTypes.custom:
      case dataTypes.decimal:
      case dataTypes.inet:
      case dataTypes.varint:
        //return buffer and move on :)
        return utils.copyBuffer(bytes);
        break;
      case dataTypes.ascii:
        return bytes.toString('ascii');
      case dataTypes.bigint:
      case dataTypes.counter:
      case dataTypes.timestamp:
        return decodeBigNumber(utils.copyBuffer(bytes));
      case dataTypes.blob:
        return utils.copyBuffer(bytes);
      case dataTypes.boolean:
        return !!bytes.readUInt8(0);
      case dataTypes.double:
        return bytes.readDoubleBE(0);
      case dataTypes.float:
        return bytes.readFloatBE(0);
      case dataTypes.int:
        return bytes.readInt32BE(0);
      case dataTypes.uuid:
      case dataTypes.timeuuid:
        return uuid.unparse(bytes);
      case dataTypes.text:
      case dataTypes.varchar:
        return bytes.toString('utf8');
      case dataTypes.list:
      case dataTypes.set:
        var list = decodeList(bytes, type[1][0]);
        return list;
      case dataTypes.map:
        var map = decodeMap(bytes, type[1][0][0], type[1][1][0]);
        return map;
    }

    throw new Error('Unknown data type: ' + type[0]);
  }
  
  function decodeBigNumber (bytes) {
    var value = new Int64(bytes);
    return value;
  }

  /*
   * Reads a list from bytes
   */
  function decodeList (bytes, type) {
    var offset = 0;
    //a short containing the total items
    var totalItems = bytes.readUInt16BE(offset);
    offset += 2;
    var list = [];
    for(var i = 0; i < totalItems; i++) {
      //bytes length of the item
      var length = bytes.readUInt16BE(offset);
      offset += 2;
      //slice it
      list.push(decode(bytes.slice(offset, offset+length), [type]));
      offset += length;
    }
    return list;
  }

  /*
   * Reads a map (key / value) from bytes
   */
  function decodeMap (bytes, type1, type2) {
    var offset = 0;
    //a short containing the total items
    var totalItems = bytes.readUInt16BE(offset);
    offset += 2;
    var map = {};
    for(var i = 0; i < totalItems; i++) {
      var keyLength = bytes.readUInt16BE(offset);
      offset += 2;
      var key = decode(bytes.slice(offset, offset+keyLength), [type1]);
      offset += keyLength;
      var valueLength = bytes.readUInt16BE(offset);
      offset += 2;
      var value = decode(bytes.slice(offset, offset+valueLength), [type2]);
      map[key] = value;
      offset += valueLength;
    }
    return map;
  }
  
  function encode (item) {
    if (item === null) {
      return null;
    }
    var value = item;
    var type = null;
    if (item.hint) {
      type = item.hint;
      value = item.value;
    }
    if (value === null) {
      return null;
    }
    if (!type) {
      //TODO: Handle all types
      if (typeof value === 'number') {
        //TODO: Floats / Int64
        type = dataTypes.int;
      }
      else if (typeof value === 'string') {
        type = dataTypes.text;
      }
      else if (value instanceof Buffer) {
        type = dataTypes.blob;
      }
      // Assume all other objects are some kind of collection
      else if (value instanceof Object) {
        type = dataTypes[utils.guessCollectionType(value)];
      }
    }
    switch (type) {
      case dataTypes.int:
        return encodeInt(value);
      case dataTypes.text:
      case dataTypes.uuid:
      case dataTypes.varchar:
        return encodeString(value);
      case dataTypes.custom:
      case dataTypes.decimal:
      case dataTypes.inet:
      case dataTypes.varint:
      case dataTypes.timeuuid:
      case dataTypes.blob:
        return encodeBlob(value, type);
      case dataTypes.map:
      case dataTypes.list:
      case dataTypes.set:
        return encodeCollection(value, type);
      default:
        throw new Error('Type not supported');
    }
  }
  
  function encodeInt (value) {
    if (typeof value !== 'number') {
      throw new TypeError(null, value, 'number');
    }
    var buf = new Buffer(4);
    buf.writeInt32BE(value, 0);
    return buf;
  }
  
  function encodeString (value) {
    if (typeof value !== 'string') {
      throw new TypeError(null, value, 'string');
    }
    var buf = new Buffer(value, 'utf8');
    return buf;
  }
  
  function encodeBlob (value, type) {
    if (!(value instanceof Buffer)) {
      throw new TypeError(null, value, Buffer, null, type);
    }
    return value;
  }

  function encodeCollection(value, type){
    switch (type){
      case dataTypes.map:
         return new Buffer(utils.queryParser.stringifyMap(value), 'utf8');
        break;
      case dataTypes.set:
        return new Buffer(utils.queryParser.stringifySet(value), 'utf8');
        break;
      case dataTypes.list:
        return new Buffer(utils.queryParser.stringifyList(value), 'utf8');
        break;
      default:
        throw new Error('Invalid type provided');
    }
  }

  return {
    decode: decode,
    encode: encode};
})();

function QueryLiteral (value) {
  this.value = value;
}

QueryLiteral.prototype.toString = function () {
  return this.value.toString();
}

/**
 * Base Error
 */
function DriverError (message, constructor) {
  Error.captureStackTrace(this, constructor || this);
  this.name = constructor.name;
  this.message = message || 'Error';
  this.info = 'Cassandra Driver Error';
}
util.inherits(DriverError, Error);

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
  this.expectedType = expectedType;
  this.actualType = actualType;
  this.reference = reference;
  this.info = 'Represents an error that happens when trying to convert from one type to another.';
}
util.inherits(TypeError, DriverError);


exports.opcodes = opcodes;
exports.consistencies = consistencies;
exports.responseErrorCodes = responseErrorCodes;
exports.dataTypes = dataTypes;
exports.typeEncoder = typeEncoder;
exports.QueryLiteral = QueryLiteral;
exports.DriverError = DriverError;
exports.TimeoutError = TimeoutError;
