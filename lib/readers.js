'use strict';

const util = require('util');
const utils = require('./utils');
const types = require('./types');
const errors = require('./errors');

/**
 * Information on the formatting of the returned rows
 */
const resultFlag = {
  globalTablesSpec:   0x0001,
  hasMorePages:       0x0002,
  noMetadata:         0x0004
};

/**
 * Buffer forward reader of CQL binary frames
 * @param {FrameHeader} header
 * @param {Buffer} body
 * @param {Number} [offset]
 */
function FrameReader(header, body, offset) {
  this.header = header;
  this.opcode = header.opcode;
  this.offset = offset || 0;
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
 * @param {Number} begin
 * @param {Number} [end]
 * @returns {Buffer}
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
  let end = this.buf.length;
  if (typeof length !== 'undefined' && this.offset + length < this.buf.length) {
    end = this.offset + length;
  }
  const bytes = this.slice(this.offset, end);
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
  const result = this.buf.readInt32BE(this.offset);
  this.offset += 4;
  return result;
};

/** @returns {Number} */
FrameReader.prototype.readShort = function () {
  const result = this.buf.readUInt16BE(this.offset);
  this.offset += 2;
  return result;
};

FrameReader.prototype.readByte = function () {
  const result = this.buf.readUInt8(this.offset);
  this.offset += 1;
  return result;
};

FrameReader.prototype.readString = function () {
  const length = this.readShort();
  this.checkOffset(length);
  const result = this.buf.toString('utf8', this.offset, this.offset+length);
  this.offset += length;
  return result;
};

/**
 * Checks that the new length to read is within the range of the buffer length. Throws a RangeError if not.
 * @param {Number} newLength
 */
FrameReader.prototype.checkOffset = function (newLength) {
  if (this.offset + newLength > this.buf.length) {
    const err = new RangeError('Trying to access beyond buffer length');
    err.expectedLength = newLength;
    throw err;
  }
};

/**
 * Reads a protocol string list
 * @returns {Array}
 */
FrameReader.prototype.readStringList = function () {
  const length = this.readShort();
  const list = new Array(length);
  for (let i = 0; i < length; i++) {
    list[i] = this.readString();
  }
  return list;
};

/**
 * Reads the amount of bytes that the field has and returns them (slicing them).
 * @returns {Buffer}
 */
FrameReader.prototype.readBytes = function () {
  const length = this.readInt();
  if (length < 0) {
    return null;
  }
  this.checkOffset(length);
  return this.read(length);
};

FrameReader.prototype.readShortBytes = function () {
  const length = this.readShort();
  if (length < 0) {
    return null;
  }
  this.checkOffset(length);
  return this.read(length);
};

/**
 * Reads an associative array of strings as keys and bytes as values
 * @returns {Object}
 */
FrameReader.prototype.readBytesMap = function () {
  //A [short] n, followed by n pair <k><v> where <k> is a
  //[string] and <v> is a [bytes].
  const length = this.readShort();
  if (length < 0) {
    return null;
  }
  const map = {};
  for (let i = 0; i < length; i++) {
    map[this.readString()] = this.readBytes();
  }
  return map;
};

/**
 * Reads a data type definition
 * @returns {{code: Number, info: Object|null}} An array of 2 elements
 */
FrameReader.prototype.readType = function () {
  let i;
  const type = {
    code: this.readShort(),
    type: null
  };
  switch (type.code) {
    case types.dataTypes.custom:
      type.info = this.readString();
      break;
    case types.dataTypes.list:
    case types.dataTypes.set:
      type.info = this.readType();
      break;
    case types.dataTypes.map:
      type.info = [this.readType(), this.readType()];
      break;
    case types.dataTypes.udt:
      type.info = {
        keyspace: this.readString(),
        name: this.readString(),
        fields: new Array(this.readShort())
      };
      for (i = 0; i < type.info.fields.length; i++) {
        type.info.fields[i] = {
          name: this.readString(),
          type: this.readType()
        };
      }
      break;
    case types.dataTypes.tuple:
      type.info = new Array(this.readShort());
      for (i = 0; i < type.info.length; i++) {
        type.info[i] = this.readType();
      }
      break;
  }
  return type;
};

/**
 * Reads an Ip address and port
 * @returns {{address: exports.InetAddress, port: Number}}
 */
FrameReader.prototype.readInet = function () {
  const length = this.readByte();
  const address = this.read(length);
  return {address: new types.InetAddress(address), port: this.readInt()};
};

/**
 * Reads the body bytes corresponding to the flags
 * @returns {{traceId: Uuid, warnings: Array, customPayload}}
 * @throws {RangeError}
 */
FrameReader.prototype.readFlagsInfo = function () {
  if (this.header.flags === 0) {
    return utils.emptyObject;
  }
  const result = {};
  if (this.header.flags & types.frameFlags.tracing) {
    this.checkOffset(16);
    result.traceId = new types.Uuid(utils.copyBuffer(this.read(16)));
  }
  if (this.header.flags & types.frameFlags.warning) {
    result.warnings = this.readStringList();
  }
  if (this.header.flags & types.frameFlags.customPayload) {
    result.customPayload = this.readBytesMap();
  }
  return result;
};

/**
 * Reads the metadata from a row or a prepared result response
 * @param {Number} kind
 * @returns {Object}
 * @throws {RangeError}
 */
FrameReader.prototype.readMetadata = function (kind) {
  let i;
  //Determines if its a prepared metadata
  const isPrepared = (kind === types.resultKind.prepared);
  const meta = {};
  //as used in Rows and Prepared responses
  const flags = this.readInt();

  const columnLength = this.readInt();
  if (types.protocolVersion.supportsPreparedPartitionKey(this.header.version) && isPrepared) {
    //read the pk columns
    meta.partitionKeys = new Array(this.readInt());
    for (i = 0; i < meta.partitionKeys.length; i++) {
      meta.partitionKeys[i] = this.readShort();
    }
  }
  if (flags & resultFlag.hasMorePages) {
    meta.pageState = utils.copyBuffer(this.readBytes());
  }
  if (flags & resultFlag.globalTablesSpec) {
    meta.global_tables_spec = true;
    meta.keyspace = this.readString();
    meta.table = this.readString();
  }
  meta.columns = new Array(columnLength);
  meta.columnsByName = utils.emptyObject;
  if (isPrepared) {
    //for prepared metadata, we will need a index of the columns (param) by name
    meta.columnsByName = {};
  }
  for (i = 0; i < columnLength; i++) {
    const col = {};
    if(!meta.global_tables_spec) {
      col.ksname = this.readString();
      col.tablename = this.readString();
    }
    col.name = this.readString();
    col.type = this.readType();
    meta.columns[i] = col;
    if (isPrepared) {
      meta.columnsByName[col.name] = i;
    }
  }

  return meta;
};

// templates for derived error messages.
const _writeTimeoutQueryMessage = 'Server timeout during write query at consistency %s (%d peer(s) acknowledged the write over %d required)';
const _writeTimeoutBatchLogMessage = 'Server timeout during batchlog write at consistency %s (%d peer(s) acknowledged the write over %d required)';
const _writeFailureMessage = 'Server failure during write query at consistency %s (%d responses were required but only %d replicas responded, %d failed)';
const _unavailableMessage = 'Not enough replicas available for query at consistency %s (%d required but only %d alive)';
const _readTimeoutMessage = 'Server timeout during read query at consistency %s (%s)';
const _readFailureMessage = 'Server failure during read query at consistency %s (%d responses were required but only %d replicas responded, %d failed)';

/**
 * Reads the error from the frame
 * @throws {RangeError}
 * @returns {ResponseError}
 */
FrameReader.prototype.readError = function () {
  const code = this.readInt();
  const message = this.readString();
  const err = new errors.ResponseError(code, message);
  //read extra info
  switch (code) {
    case types.responseErrorCodes.unavailableException:
      err.consistencies = this.readShort();
      err.required = this.readInt();
      err.alive = this.readInt();
      err.message = util.format(_unavailableMessage, types.consistencyToString[err.consistencies], err.required, err.alive);
      break;
    case types.responseErrorCodes.readTimeout:
    case types.responseErrorCodes.readFailure:
      err.consistencies = this.readShort();
      err.received = this.readInt();
      err.blockFor = this.readInt();
      if (code === types.responseErrorCodes.readFailure) {
        err.failures = this.readInt();
      }
      err.isDataPresent = this.readByte();
      if (code === types.responseErrorCodes.readTimeout) {
        let details;
        if (err.received < err.blockFor) {
          details = util.format('%d replica(s) responded over %d required', err.received, err.blockFor);
        } else if (!err.isDataPresent) {
          details = 'the replica queried for the data didn\'t respond';
        } else {
          details = 'timeout while waiting for repair of inconsistent replica';
        }
        err.message = util.format(_readTimeoutMessage, types.consistencyToString[err.consistencies], details);
      } else {
        err.message = util.format(_readFailureMessage, types.consistencyToString[err.consistencies],
          err.blockFor, err.received, err.failures);
      }
      break;
    case types.responseErrorCodes.writeTimeout:
    case types.responseErrorCodes.writeFailure:
      err.consistencies = this.readShort();
      err.received = this.readInt();
      err.blockFor = this.readInt();
      if (code === types.responseErrorCodes.writeFailure) {
        err.failures = this.readInt();
      }
      err.writeType = this.readString();

      if (code === types.responseErrorCodes.writeTimeout) {
        const template = err.writeType === 'BATCH_LOG' ? _writeTimeoutBatchLogMessage : _writeTimeoutQueryMessage;
        err.message = util.format(template, types.consistencyToString[err.consistencies], err.received, err.blockFor);
      } else {
        err.message = util.format(_writeFailureMessage, types.consistencyToString[err.consistencies],
          err.blockFor, err.received, err.failures);
      }
      break;
    case types.responseErrorCodes.unprepared:
      err.queryId = utils.copyBuffer(this.readShortBytes());
      break;
    case types.responseErrorCodes.functionFailure:
      err.keyspace = this.readString();
      err.functionName = this.readString();
      err.argTypes = this.readStringList();
      break;
    case types.responseErrorCodes.alreadyExists: {
      err.keyspace = this.readString();
      const table = this.readString();
      if(table.length > 0) {
        err.table = table;
      }
      break;
    }
  }
  return err;
};

/**
 * Reads an event from Cassandra and returns the detail
 * @returns {{eventType: String, inet: {address: Buffer, port: Number}}, *}
 */
FrameReader.prototype.readEvent = function () {
  const eventType = this.readString();
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
  let result;
  if (!types.protocolVersion.supportsSchemaChangeFullMetadata(this.header.version)) {
    //v1/v2: 3 strings, the table value can be empty
    result = {
      eventType: types.protocolEvents.schemaChange,
      schemaChangeType: this.readString(),
      keyspace: this.readString(),
      table: this.readString()
    };
    result.isKeyspace = !result.table;
    return result;
  }
  //v3+: 3 or 4 strings: change_type, target, keyspace and (table, type, functionName or aggregate)
  result = {
    eventType: types.protocolEvents.schemaChange,
    schemaChangeType: this.readString(),
    target: this.readString(),
    keyspace: this.readString(),
    table: null,
    udt: null,
    signature: null
  };
  result.isKeyspace = result.target === 'KEYSPACE';
  switch (result.target) {
    case 'TABLE':
      result.table = this.readString();
      break;
    case 'TYPE':
      result.udt = this.readString();
      break;
    case 'FUNCTION':
      result.functionName = this.readString();
      result.signature = this.readStringList();
      break;
    case 'AGGREGATE':
      result.aggregate = this.readString();
      result.signature = this.readStringList();
  }
  return result;
};

exports.FrameReader = FrameReader;
