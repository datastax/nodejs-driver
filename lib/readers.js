/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js 
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils.js');
var types = require('./types.js');

/**
 * Information on the formatting of the returned rows
 */
var resultFlag = {
  globalTablesSpec:   0x0001,
  hasMorePages:       0x0002,
  noMetadata:         0x0004
};

/**
 * Buffer forward reader of CQL binary frames
 */
function FrameReader(header, body) {
  this.header = header;
  this.opcode = header.opcode;
  this.offset = 0;
  this.buf = body;
}

FrameReader.prototype.remainingLength = function () {
  return this.buf.length - this.offset;
};

FrameReader.prototype.getBuffer = function () {
  return this.buf;
};

/**
 * Slices the underlining buffer
 */
FrameReader.prototype.slice = function (begin, end) {
  if (typeof end === 'undefined') {
    end = this.buf.length;
  }
  return this.buf.slice(begin, end);
};

/**
 * Modifies the underlying buffer, it concatenates the given buffer with the original (internalBuffer = concat(bytes, internalBuffer)
 */
FrameReader.prototype.unshift = function (bytes) {
  if (this.offset > 0) {
    throw new Error('Can not modify the underlying buffer if already read');
  }
  this.buf = Buffer.concat([bytes, this.buf], bytes.length + this.buf.length);
};

/**
 * Reads any number of bytes and moves the offset.
 * if length not provided or it's larger than the remaining bytes, reads to end.
 */
FrameReader.prototype.read = function (length) {
  var end = this.buf.length;
  if (typeof length !== 'undefined' && this.offset + length < this.buf.length) {
    end = this.offset + length;
  }
  var bytes = this.slice(this.offset, end);
  this.offset = end;
  return bytes;
};

/**
 * Moves the reader cursor to the end
 */
FrameReader.prototype.toEnd = function () {
  this.offset = this.buf.length;
};

FrameReader.prototype.readInt = function() {
  var result = this.buf.readInt32BE(this.offset);
  this.offset += 4;
  return result;
};

FrameReader.prototype.readShort = function () {
  var result = this.buf.readUInt16BE(this.offset);
  this.offset += 2;
  return result;
};

FrameReader.prototype.readByte = function () {
  var result = this.buf.readUInt8(this.offset);
  this.offset += 1;
  return result;
};

FrameReader.prototype.readString = function () {
  var length = this.readShort();
  this.checkOffset(length);
  var result = this.buf.toString('utf8', this.offset, this.offset+length);
  this.offset += length;
  return result;
};

/**
 * Checks that the new length to read is within the range of the buffer length. Throws a RangeError if not.
 */
FrameReader.prototype.checkOffset = function (newLength) {
  if (this.offset + newLength > this.buf.length) {
    throw new RangeError('Trying to access beyond buffer length');
  }
};

FrameReader.prototype.readUUID = function () {
  var octets = [];
  for (var i = 0; i < 16; i++) {
      octets.push(this.readByte());
  }

  var str = "";

  octets.forEach(function(octet) {
      str += octet.toString(16);
  });

  return str.slice(0, 8) + '-' + str.slice(8, 12) + '-' + str.slice(12, 16) + '-' + str.slice(16, 20) + '-' + str.slice(20);
};

FrameReader.prototype.readStringList = function () {
  var num = this.readShort();

  var list = [];

  for (var i = 0; i < num; i++) {
      list.push(this.readString());
  }

  return list;
};
/**
 * Reads the amount of bytes that the field has and returns them (slicing them).
 */
FrameReader.prototype.readBytes = function () {
  var length = this.readInt();
  if (length < 0) {
    return null;
  }
  this.checkOffset(length);

  return this.read(length);
};

FrameReader.prototype.readShortBytes = function () {
  var length = this.readShort();
  if (length < 0) {
    return null;
  }
  this.checkOffset(length);
  return this.read(length);
};

/* returns an array with two elements */
FrameReader.prototype.readOption = function () {
  var id = this.readShort();

  switch(id) {
    case 0x0000: 
        return [id, this.readString()];
    case 0x0001:
    case 0x0002:
    case 0x0003:
    case 0x0004:
    case 0x0005:
    case 0x0006:
    case 0x0007:
    case 0x0008:
    case 0x0009:
    case 0x000A:
    case 0x000B:
    case 0x000C:
    case 0x000D:
    case 0x000E:
    case 0x000F:
    case 0x0010:
        return [id, null];
    case 0x0020:
        return [id, this.readOption()];
    case 0x0021:
        return [id, [this.readOption(), this.readOption()]];
    case 0x0022:
        return [id, this.readOption()];
  }

  return [id, null];
};

/* returns an array of arrays */
FrameReader.prototype.readOptionList = function () {
  var num = this.readShort();
  var options = [];
  for(var i = 0; i < num; i++) {
      options.push(this.readOption());
  }
  return options;
};

FrameReader.prototype.readInet = function () {
  //TODO
};

FrameReader.prototype.readStringMap = function () {
  var num = this.readShort();
  var map = {};
  for(var i = 0; i < num; i++) {
      var key = this.readString();
      var value = this.readString();
      map[key] = value;
  }
  return map;
};

FrameReader.prototype.readStringMultimap = function () {
  var num = this.readShort();
  var map = {};
  for(var i = 0; i < num; i++) {
      var key = this.readString();
      map[key] = this.readStringList();
  }
  return map;
};

/**
 * Reads the metadata from a row or a prepared result response
 * @returns {Object}
 */
FrameReader.prototype.readMetadata = function() {
  var meta = {};
  //as used in Rows and Prepared responses
  var flags = this.readInt();

  var columnCount = this.readInt();
  if (flags & resultFlag.hasMorePages) {
    meta.pageState = this.readBytes();
  }
  if (flags & resultFlag.globalTablesSpec) {
    meta.global_tables_spec = true;
    meta.keyspace = this.readString();
    meta.table = this.readString();
  }

  meta.columns = [];

  for(var i = 0; i < columnCount; i++) {
    var spec = {};
    if(!meta.global_tables_spec) {
      spec.ksname = this.readString();
      spec.tablename = this.readString();
    }

    spec.name = this.readString();
    spec.type = this.readOption();
    meta.columns.push(spec);
    //Store the column index by name, to be able to find the column by name
    meta.columns['_col_' + spec.name] = i;
  }

  return meta;
};

/**
 * Try to read a value, in case there is a RangeError (out of bounds) it returns null
 * @param method
 */
FrameReader.prototype.tryRead = function (method) {
  var value;
  try{
    value = method();
  }
  catch (e) {
    if (e instanceof RangeError) {
      return null;
    }
    throw e;
  }

};

/**
 * Reads the error from the frame
 * @throws {RangeError}
 * @returns {ResponseError}
 */
FrameReader.prototype.readError = function () {
  var code = this.readInt();
  var message = this.readString();
  //determine if the server is unhealthy
  //if true, the client should not retry for a while
  var isServerUnhealthy = false;
  switch (code) {
    case types.responseErrorCodes.serverError:
    case types.responseErrorCodes.overloaded:
    case types.responseErrorCodes.isBootstrapping:
      isServerUnhealthy = true;
      break;
  }
  return new ResponseError(code, message, isServerUnhealthy);
};

function readEvent(data, emitter) {
  var reader = new FrameReader(data);
  var event = reader.readString();
  if(event === 'TOPOLOGY_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readInet());
  }
  else if (event === 'STATUS_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readInet());
  }
  else if (event === 'SCHEMA_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readString(), reader.readString());
  }
  else {
    throw new Error('Unknown EVENT type: ' + event);
  }
}

function ResponseError(code, message, isServerUnhealthy) {
  ResponseError.super_.call(this, message, this.constructor);
  this.code = code;
  this.isServerUnhealthy = isServerUnhealthy;
  this.info = 'Represents a error message from the server';
}

util.inherits(ResponseError, types.DriverError);

exports.readEvent = readEvent;
exports.FrameReader = FrameReader;