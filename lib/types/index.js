/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const util = require('util');

const errors = require('../errors');
const TimeUuid = require('./time-uuid');
const Uuid = require('./uuid');
const protocolVersion = require('./protocol-version');
const utils = require('../utils');

/** @module types */
/**
 * Long constructor, wrapper of the internal library used: {@link https://github.com/dcodeIO/long.js Long.js}.
 * @constructor
 */
const Long = require('long');

/**
 * Consistency levels
 * @type {Object}
 * @property {Number} any Writing: A write must be written to at least one node. If all replica nodes for the given row key are down, the write can still succeed after a hinted handoff has been written. If all replica nodes are down at write time, an ANY write is not readable until the replica nodes for that row have recovered.
 * @property {Number} one Returns a response from the closest replica, as determined by the snitch.
 * @property {Number} two Returns the most recent data from two of the closest replicas.
 * @property {Number} three Returns the most recent data from three of the closest replicas.
 * @property {Number} quorum Reading: Returns the record with the most recent timestamp after a quorum of replicas has responded regardless of data center. Writing: A write must be written to the commit log and memory table on a quorum of replica nodes.
 * @property {Number} all Reading: Returns the record with the most recent timestamp after all replicas have responded. The read operation will fail if a replica does not respond. Writing: A write must be written to the commit log and memory table on all replica nodes in the cluster for that row.
 * @property {Number} localQuorum Reading: Returns the record with the most recent timestamp once a quorum of replicas in the current data center as the coordinator node has reported. Writing: A write must be written to the commit log and memory table on a quorum of replica nodes in the same data center as the coordinator node. Avoids latency of inter-data center communication.
 * @property {Number} eachQuorum Reading: Returns the record once a quorum of replicas in each data center of the cluster has responded. Writing: Strong consistency. A write must be written to the commit log and memtable on a quorum of replica nodes in all data centers.
 * @property {Number} serial Achieves linearizable consistency for lightweight transactions by preventing unconditional updates.
 * @property {Number} localSerial Same as serial but confined to the data center. A write must be written conditionally to the commit log and memtable on a quorum of replica nodes in the same data center.
 * @property {Number} localOne Similar to One but only within the DC the coordinator is in.
 */
const consistencies = {
  any:          0x00,
  one:          0x01,
  two:          0x02,
  three:        0x03,
  quorum:       0x04,
  all:          0x05,
  localQuorum:  0x06,
  eachQuorum:   0x07,
  serial:       0x08,
  localSerial:  0x09,
  localOne:     0x0a
};

/**
 * Mapping of consistency level codes to their string representation.
 * @type {Object}
 */
const consistencyToString = {};
consistencyToString[consistencies.any] = 'ANY';
consistencyToString[consistencies.one] = 'ONE';
consistencyToString[consistencies.two] = 'TWO';
consistencyToString[consistencies.three] = 'THREE';
consistencyToString[consistencies.quorum] = 'QUORUM';
consistencyToString[consistencies.all] = 'ALL';
consistencyToString[consistencies.localQuorum] = 'LOCAL_QUORUM';
consistencyToString[consistencies.eachQuorum] = 'EACH_QUORUM';
consistencyToString[consistencies.serial] = 'SERIAL';
consistencyToString[consistencies.localSerial] = 'LOCAL_SERIAL';
consistencyToString[consistencies.localOne] = 'LOCAL_ONE';

/**
 * CQL data types
 * @type {Object}
 * @property {Number} custom A custom type.
 * @property {Number} ascii ASCII character string.
 * @property {Number} bigint 64-bit signed long.
 * @property {Number} blob Arbitrary bytes (no validation).
 * @property {Number} boolean true or false.
 * @property {Number} counter Counter column (64-bit signed value).
 * @property {Number} decimal Variable-precision decimal.
 * @property {Number} double 64-bit IEEE-754 floating point.
 * @property {Number} float 32-bit IEEE-754 floating point.
 * @property {Number} int 32-bit signed integer.
 * @property {Number} text UTF8 encoded string.
 * @property {Number} timestamp A timestamp.
 * @property {Number} uuid Type 1 or type 4 UUID.
 * @property {Number} varchar UTF8 encoded string.
 * @property {Number} varint Arbitrary-precision integer.
 * @property {Number} timeuuid  Type 1 UUID.
 * @property {Number} inet An IP address. It can be either 4 bytes long (IPv4) or 16 bytes long (IPv6).
 * @property {Number} date A date without a time-zone in the ISO-8601 calendar system.
 * @property {Number} time A value representing the time portion of the day.
 * @property {Number} smallint 16-bit two's complement integer.
 * @property {Number} tinyint 8-bit two's complement integer.
 * @property {Number} list A collection of elements.
 * @property {Number} map Key/value pairs.
 * @property {Number} set A collection that contains no duplicate elements.
 * @property {Number} udt User-defined type.
 * @property {Number} tuple A sequence of values.
 */
const dataTypes = {
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
  date:       0x0011,
  time:       0x0012,
  smallint:   0x0013,
  tinyint:    0x0014,
  duration:   0x0015,
  list:       0x0020,
  map:        0x0021,
  set:        0x0022,
  udt:        0x0030,
  tuple:      0x0031,
  /**
   * Returns the typeInfo of a given type name
   * @param {string} name
   */
  getByName:  function(name) {
    name = name.toLowerCase();
    if (name.indexOf('<') > 0) {
      const listMatches = /^(list|set)<(.+)>$/.exec(name);
      if (listMatches) {
        return { code: this[listMatches[1]], info: this.getByName(listMatches[2])};
      }
      const mapMatches = /^(map)< *(.+) *, *(.+)>$/.exec(name);
      if (mapMatches) {
        return { code: this[mapMatches[1]], info: [this.getByName(mapMatches[2]), this.getByName(mapMatches[3])]};
      }
      const udtMatches = /^(udt)<(.+)>$/.exec(name);
      if (udtMatches) {
        //udt name as raw string
        return { code: this[udtMatches[1]], info: udtMatches[2]};
      }
      const tupleMatches = /^(tuple)<(.+)>$/.exec(name);
      if (tupleMatches) {
        //tuple info as an array of types
        return { code: this[tupleMatches[1]], info: tupleMatches[2].split(',').map(function (x) {
          return this.getByName(x.trim());
        }, this)};
      }
      const vectorMatches = /^vector<\s*(.+)\s*,\s*(\d+)\s*>$/.exec(name);
      if(vectorMatches){
        return {
          code: this.custom,
          customTypeName: 'vector',
          info: [this.getByName(vectorMatches[1]), parseInt(vectorMatches[2], 10)]
        };
      }
    }
    const typeInfo = { code: this[name]};
    if (typeof typeInfo.code !== 'number') {
      throw new TypeError('Data type with name ' + name + ' not valid');
    }
    return typeInfo;
  }
};

/**
 * Map of Data types by code
 * @internal
 * @private
 * @type {Record<number, string>}
 */
const _dataTypesByCode = (function () {
  /**@type {Record<number, string>} */
  const result = {};
  for (const key in dataTypes) {
    if (!dataTypes.hasOwnProperty(key)) {
      continue;
    }
    const val = dataTypes[key];
    if (typeof val !== 'number') {
      continue;
    }
    result[val] = key;
  }
  return result;
})();

/**
 * Represents the distance of Cassandra node as assigned by a LoadBalancingPolicy relatively to the driver instance.
 * @type {Object}
 * @property {Number} local A local node.
 * @property {Number} remote A remote node.
 * @property {Number} ignored A node that is meant to be ignored.
 */
const distance = {
  local:    0,
  remote:   1,
  ignored:  2
};

/**
 * An integer byte that distinguish the actual message from and to Cassandra
 * @internal
 * @ignore
 */
const opcodes = {
  error:          0x00,
  startup:        0x01,
  ready:          0x02,
  authenticate:   0x03,
  credentials:    0x04,
  options:        0x05,
  supported:      0x06,
  query:          0x07,
  result:         0x08,
  prepare:        0x09,
  execute:        0x0a,
  register:       0x0b,
  event:          0x0c,
  batch:          0x0d,
  authChallenge:  0x0e,
  authResponse:   0x0f,
  authSuccess:    0x10,
  cancel:         0xff,

  /**
   * Determines if the code is a valid opcode
   */
  isInRange: function (code) {
    return code > this.error && code > this.event;
  }
};

/**
 * Event types from Cassandra
 * @type {{topologyChange: string, statusChange: string, schemaChange: string}}
 * @internal
 * @ignore
 */
const protocolEvents = {
  topologyChange: 'TOPOLOGY_CHANGE',
  statusChange: 'STATUS_CHANGE',
  schemaChange: 'SCHEMA_CHANGE'
};

/**
 * Server error codes returned by Cassandra
 * @type {Object}
 * @property {Number} serverError Something unexpected happened.
 * @property {Number} protocolError Some client message triggered a protocol violation.
 * @property {Number} badCredentials Authentication was required and failed.
 * @property {Number} unavailableException Raised when coordinator knows there is not enough replicas alive to perform a query with the requested consistency level.
 * @property {Number} overloaded The request cannot be processed because the coordinator is overloaded.
 * @property {Number} isBootstrapping The request was a read request but the coordinator node is bootstrapping.
 * @property {Number} truncateError Error encountered during a truncate request.
 * @property {Number} writeTimeout Timeout encountered on write query on coordinator waiting for response(s) from replicas.
 * @property {Number} readTimeout Timeout encountered on read query on coordinator waitign for response(s) from replicas.
 * @property {Number} readFailure A non-timeout error encountered during a read request.
 * @property {Number} functionFailure A (user defined) function encountered during execution.
 * @property {Number} writeFailure A non-timeout error encountered during a write request.
 * @property {Number} syntaxError The submitted query has a syntax error.
 * @property {Number} unauthorized The logged user doesn't have the right to perform the query.
 * @property {Number} invalid The query is syntactically correct but invalid.
 * @property {Number} configError The query is invalid because of some configuration issue.
 * @property {Number} alreadyExists The query attempted to create a schema element (i.e. keyspace, table) that already exists.
 * @property {Number} unprepared Can be thrown while a prepared statement tries to be executed if the provided statement is not known by the coordinator.
 */
const responseErrorCodes = {
  serverError:            0x0000,
  protocolError:          0x000A,
  badCredentials:         0x0100,
  unavailableException:   0x1000,
  overloaded:             0x1001,
  isBootstrapping:        0x1002,
  truncateError:          0x1003,
  writeTimeout:           0x1100,
  readTimeout:            0x1200,
  readFailure:            0x1300,
  functionFailure:        0x1400,
  writeFailure:           0x1500,
  syntaxError:            0x2000,
  unauthorized:           0x2100,
  invalid:                0x2200,
  configError:            0x2300,
  alreadyExists:          0x2400,
  unprepared:             0x2500,
  clientWriteFailure:     0x8000,
};

/**
 * Type of result included in a response
 * @internal
 * @ignore
 */
const resultKind = {
  voidResult:      0x0001,
  rows:            0x0002,
  setKeyspace:     0x0003,
  prepared:        0x0004,
  schemaChange:    0x0005
};

/**
 * Message frame flags
 * @internal
 * @ignore
 */
const frameFlags = {
  compression:    0x01,
  tracing:        0x02,
  customPayload:  0x04,
  warning:        0x08
};

/**
 * Unset representation.
 * <p>
 *   Use this field if you want to set a parameter to <code>unset</code>. Valid for Cassandra 2.2 and above.
 * </p>
 */
const unset = Object.freeze({'unset': true});

/**
 * A long representing the value 1000
 * @const
 * @private
 */
const _longOneThousand = Long.fromInt(1000);

/**
 * Counter used to generate up to 1000 different timestamp values with the same Date
 * @private
 */
let _timestampTicks = 0;

/**
 * <p><strong>Backward compatibility only, use [TimeUuid]{@link module:types~TimeUuid} instead</strong>.</p>
 * Generates and returns a RFC4122 v1 (timestamp based) UUID in a string representation.
 * @param {{msecs, node, clockseq, nsecs}} [options]
 * @param {Buffer} [buffer]
 * @param {Number} [offset]
 * @deprecated Use [TimeUuid]{@link module:types~TimeUuid} instead
 */
function timeuuid(options, buffer, offset) {
  let date;
  let ticks;
  let nodeId;
  let clockId;
  if (options) {
    if (typeof options.msecs === 'number') {
      date = new Date(options.msecs);
    }
    if (options.msecs instanceof Date) {
      date = options.msecs;
    }
    if (Array.isArray(options.node)) {
      nodeId = utils.allocBufferFromArray(options.node);
    }
    if (typeof options.clockseq === 'number') {
      clockId = utils.allocBufferUnsafe(2);
      clockId.writeUInt16BE(options.clockseq, 0);
    }
    if (typeof options.nsecs === 'number') {
      ticks = options.nsecs;
    }
  }
  const uuid = new TimeUuid(date, ticks, nodeId, clockId);
  if (buffer instanceof Buffer) {
    //copy the values into the buffer
    uuid.getBuffer().copy(buffer, offset || 0);
    return buffer;
  }
  return uuid.toString();
}

/**
 * <p><strong>Backward compatibility only, use [Uuid]{@link module:types~Uuid} class instead</strong>.</p>
 * Generate and return a RFC4122 v4 UUID in a string representation.
 * @deprecated Use [Uuid]{@link module:types~Uuid} class instead
 */
function uuid(options, buffer, offset) {
  let uuid;
  if (options) {
    if (Array.isArray(options.random)) {
      uuid = new Uuid(utils.allocBufferFromArray(options.random));
    }
  }
  if (!uuid) {
    uuid = Uuid.random();
  }
  if (buffer instanceof Buffer) {
    //copy the values into the buffer
    uuid.getBuffer().copy(buffer, offset || 0);
    return buffer;
  }
  return uuid.toString();
}

/**
 * Gets the data type name for a given type definition, it may not work for udt or custom type
 * @internal
 * @ignore
 * @throws {ArgumentError}
 */
function getDataTypeNameByCode(item) {
  if (!item || typeof item.code !== 'number') {
    throw new errors.ArgumentError('Invalid signature type definition');
  }
  const typeName = _dataTypesByCode[item.code];
  if (!typeName) {
    throw new errors.ArgumentError(util.format('Type with code %d not found', item.code));
  }
  if (!('info' in item) || !item.info) {
    return typeName;
  }
  // special case for vector
  if (item.code === dataTypes.custom && 'customTypeName' in item && item.customTypeName === 'vector') {
    return 'vector<' + getDataTypeNameByCode(item.info[0]) + ', ' + item.info[1] + '>';
  }
  if (Array.isArray(item.info)) {
    return (typeName +
      '<' +
      item.info.map(function (t) {
        return getDataTypeNameByCode(t);
      }).join(', ') +
      '>');
  }
  if (typeof item.info.code === 'number') {
    return typeName + '<' + getDataTypeNameByCode(item.info) + '>';
  }
  if (item.code === dataTypes.udt) {
    return (/**@type {UdtColumnInfo}*/item).info.name;
  }
  return typeName;
}

//classes

/**
 * Represents a frame header that could be used to read from a Buffer or to write to a Buffer
 * @ignore
 * @param {Number} version Protocol version
 * @param {Number} flags
 * @param {Number} streamId
 * @param {Number} opcode
 * @param {Number} bodyLength
 * @constructor
 */
function FrameHeader(version, flags, streamId, opcode, bodyLength) {
  this.version = version;
  this.flags = flags;
  this.streamId = streamId;
  this.opcode = opcode;
  this.bodyLength = bodyLength;
}

/**
 * The length of the header of the frame based on the protocol version
 * @returns {Number}
 */
FrameHeader.size = function (version) {
  if (protocolVersion.uses2BytesStreamIds(version)) {
    return 9;
  }
  return 8;
};

/**
 * Gets the protocol version based on the first byte of the header
 * @param {Buffer} buffer
 * @returns {Number}
 */
FrameHeader.getProtocolVersion = function (buffer) {
  return buffer[0] & 0x7F;
};

/**
 * @param {Buffer} buf
 * @param {Number} [offset]
 * @returns {FrameHeader}
 */
FrameHeader.fromBuffer = function (buf, offset) {
  let streamId = 0;
  if (!offset) {
    offset = 0;
  }
  const version = buf[offset++] & 0x7F;
  const flags = buf.readUInt8(offset++);
  if (!protocolVersion.uses2BytesStreamIds(version)) {
    streamId = buf.readInt8(offset++);
  }
  else {
    streamId = buf.readInt16BE(offset);
    offset += 2;
  }
  return new FrameHeader(version, flags, streamId, buf.readUInt8(offset++), buf.readUInt32BE(offset));
};

/** @returns {Buffer} */
FrameHeader.prototype.toBuffer = function () {
  const buf = utils.allocBufferUnsafe(FrameHeader.size(this.version));
  buf.writeUInt8(this.version, 0);
  buf.writeUInt8(this.flags, 1);
  let offset = 3;
  if (!protocolVersion.uses2BytesStreamIds(this.version)) {
    buf.writeInt8(this.streamId, 2);
  }
  else {
    buf.writeInt16BE(this.streamId, 2);
    offset = 4;
  }
  buf.writeUInt8(this.opcode, offset++);
  buf.writeUInt32BE(this.bodyLength, offset);
  return buf;
};
/**
 * Returns a long representation.
 * Used internally for deserialization
 */
Long.fromBuffer = function (value) {
  if (!(value instanceof Buffer)) {
    throw new TypeError('Expected Buffer, obtained ' + util.inspect(value));
  }
  return new Long(value.readInt32BE(4), value.readInt32BE(0));
};

/**
 * Returns a big-endian buffer representation of the Long instance
 * @param {Long} value
 */
Long.toBuffer = function (value) {
  if (!(value instanceof Long)) {
    throw new TypeError('Expected Long, obtained ' + util.inspect(value));
  }
  const buffer = utils.allocBufferUnsafe(8);
  buffer.writeUInt32BE(value.getHighBitsUnsigned(), 0);
  buffer.writeUInt32BE(value.getLowBitsUnsigned(), 4);
  return buffer;
};

/**
 * Provide the name of the constructor and the string representation
 * @returns {string}
 */
Long.prototype.inspect = function () {
  return 'Long: ' + this.toString();
};

/**
 * Returns the string representation.
 * Method used by the native JSON.stringify() to serialize this instance
 */
Long.prototype.toJSON = function () {
  return this.toString();
};

/**
 * Generates a value representing the timestamp for the query in microseconds based on the date and the microseconds provided
 * @param {Date} [date] The date to generate the value, if not provided it will use the current date.
 * @param {Number} [microseconds] A number from 0 to 999 used to build the microseconds part of the date.
 * @returns {Long}
 */
function generateTimestamp(date, microseconds) {
  if (!date) {
    date = new Date();
  }
  let longMicro = Long.ZERO;
  if (typeof microseconds === 'number' && microseconds >= 0 && microseconds < 1000) {
    longMicro = Long.fromInt(microseconds);
  }
  else {
    if (_timestampTicks > 999) {
      _timestampTicks = 0;
    }
    longMicro = Long.fromInt(_timestampTicks);
    _timestampTicks++;
  }
  return Long
    .fromNumber(date.getTime())
    .multiply(_longOneThousand)
    .add(longMicro);
}

//error classes

/** @private */
function QueryParserError(e) {
  QueryParserError.super_.call(this, e.message, this.constructor);
  this.internalError = e;
}
util.inherits(QueryParserError, errors.DriverError);

/** @private */
function TimeoutError (message) {
  TimeoutError.super_.call(this, message, this.constructor);
  this.info = 'Represents an error that happens when the maximum amount of time for an operation passed.';
}
util.inherits(TimeoutError, errors.DriverError);

exports.opcodes = opcodes;
exports.consistencies = consistencies;
exports.consistencyToString = consistencyToString;
exports.dataTypes = dataTypes;
exports.getDataTypeNameByCode = getDataTypeNameByCode;
exports.distance = distance;
exports.frameFlags = frameFlags;
exports.protocolEvents = protocolEvents;
exports.protocolVersion = protocolVersion;
exports.responseErrorCodes = responseErrorCodes;
exports.resultKind = resultKind;
exports.timeuuid = timeuuid;
exports.uuid = uuid;
exports.BigDecimal = require('./big-decimal');
exports.Duration = require('./duration');
exports.FrameHeader = FrameHeader;
exports.InetAddress = require('./inet-address');
exports.Integer = require('./integer');
exports.LocalDate = require('./local-date');
exports.LocalTime = require('./local-time');
exports.Long = Long;
exports.ResultSet = require('./result-set');
exports.ResultStream = require('./result-stream');
exports.Row = require('./row');
//export DriverError for backward-compatibility
exports.DriverError = errors.DriverError;
exports.TimeoutError = TimeoutError;
exports.TimeUuid = TimeUuid;
exports.Tuple = require('./tuple');
exports.Vector = require('./vector');
exports.Uuid = Uuid;
exports.unset = unset;
exports.generateTimestamp = generateTimestamp;
