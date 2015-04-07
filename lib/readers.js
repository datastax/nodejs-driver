/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils');
var types = require('./types');
var errors = require('./errors');

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
 * @param length
 * @returns {Buffer}
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

/**
 * Reads a BE Int and moves the offset
 * @returns {Number}
 */
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
    var err = new RangeError('Trying to access beyond buffer length');
    err.missingBytes = this.offset + newLength - this.buf.length;
    throw err;
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

/**
 * @returns {Array} An array of 2 elements
 */
FrameReader.prototype.readOption = function () {
  var id = this.readShort();

  switch (id) {
    case types.dataTypes.custom:
      return [id, this.readString()];
    case types.dataTypes.list:
    case types.dataTypes.set:
      return [id, this.readOption()];
    case types.dataTypes.map:
      return [id, [this.readOption(), this.readOption()]];
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

/**
 * Reads an Ip address and port
 * @returns {{address: exports.InetAddress, port: Number}}
 */
FrameReader.prototype.readInet = function () {
  var length = this.readByte();
  var address = this.read(length);
  return {address: new types.InetAddress(address), port: this.readInt()};
};

FrameReader.prototype.readStringMap = function () {
  var length = this.readShort();
  var map = {};
  for(var i = 0; i < length; i++) {
    var key = this.readString();
    map[key] = this.readString();
  }
  return map;
};

FrameReader.prototype.readStringMultimap = function () {
  var length = this.readShort();
  var map = {};
  for(var i = 0; i < length; i++) {
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
  //noinspection JSBitwiseOperatorUsage
  if (flags & resultFlag.hasMorePages) {
    meta.pageState = this.readBytes();
  }
  //noinspection JSBitwiseOperatorUsage
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
 * Reads the error from the frame
 * @throws {RangeError}
 * @returns {exports.ResponseError}
 */
FrameReader.prototype.readError = function () {
  var code = this.readInt();
  var message = this.readString();
  var err = new errors.ResponseError(code, message);
  //read extra info
  switch (code) {
    case types.responseErrorCodes.unavailableException:
      err.consistencies = this.readShort();
      err.required = this.readInt();
      err.alive = this.readInt();
      break;
    case types.responseErrorCodes.readTimeout:
      err.consistencies = this.readShort();
      err.received = this.readInt();
      err.blockFor = this.readInt();
      err.isDataPresent = this.readByte();
      break;
    case types.responseErrorCodes.writeTimeout:
      err.consistencies = this.readShort();
      err.received = this.readInt();
      err.blockFor = this.readInt();
      err.writeType = this.readString();
      break;
    case types.responseErrorCodes.unprepared:
      err.queryId = this.readShortBytes();
      break;
  }
  return err;
};

/**
 * Reads an event from Cassandra and returns the detail
 * @returns {{eventType: String, inet: {address: Buffer, port: Number}}, *}
 */
FrameReader.prototype.readEvent = function () {
  var eventType = this.readString();
  switch (eventType) {
    case types.protocolEvents.topologyChange:
      return {
        added: this.readString() === 'NEW_NODE',
        inet: this.readInet(),
        eventType: eventType};
    case types.protocolEvents.statusChange:
      return {
        up: this.readString() === 'UP',
        inet: this.readInet(),
        eventType: eventType};
    case types.protocolEvents.schemaChange:
      return this.parseSchemaChange();
  }
  //Forward compatibility
  return { eventType: eventType};
};

FrameReader.prototype.parseSchemaChange = function () {
  if (this.header.version < 3) {
    //v1/v2: 3 strings, the table value can be empty
    return {
      eventType: types.protocolEvents.schemaChange,
      schemaChangeType: this.readString(),
      keyspace: this.readString(),
      table: this.readString()
    };
  }
  //v3: 3 or 4 strings: change_type, target, keyspace and (table or type)
  var result = {
    eventType: types.protocolEvents.schemaChange,
    schemaChangeType: this.readString(),
    target: this.readString(),
    keyspace: this.readString(),
    table: null,
    type: null
  };
  switch (result.target) {
    case 'TABLE':
      result.table = this.readString();
      break;
    case 'TYPE':
      result.type = this.readString();
      break;
  }
  return result;
};

exports.FrameReader = FrameReader;
