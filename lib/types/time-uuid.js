var util = require('util');
var crypto = require('crypto');

var Uuid = require('./uuid');
/** @module types */

var _nsecs = 0;
/**
 * Oct 15, 1582 in milliseconds
 * @const
 */
var _unixToGregorian = 12219292800000;
/**
 * 10,000 ticks in a millisecond
 * @const
 */
var _ticksInMs = 10000;
/**
 * Creates a new instance of Uuid based on the parameters provided according to rfc4122.
 * If any of the arguments is not provided, it will be randomly generated, except for the date that will use the current date.
 * @class
 * @classdesc Represents an immutable version 1 universally unique identifier (UUID). A UUID represents a 128-bit value.
 * @extends module:types~Uuid
 * @param {Date} [value] The datetime for the instance, if not provided, it will use the current Date.
 * @param {Number} [nsecs] A number from 0 to 10000 representing the 100-nanoseconds units for this instance to fill in the information not available in the Date,
 * as Ecmascript Dates have only milliseconds precision.
 * @param {String|Buffer} [nodeId] A 6-length Buffer or string of 6 ascii characters representing the node identifier, ie: 'host01'.
 * @param {String|Buffer} [clockId] A 2-length Buffer or string of 6 ascii characters representing the clock identifier.
 * @constructor
 */
function TimeUuid(value, nsecs, nodeId, clockId) {
  var buffer;
  if (value instanceof Buffer) {
    if (value.length !== 16) {
      throw new Error('Buffer for v1 uuid not valid');
    }
    buffer = value;
  }
  else {
    buffer = generateBuffer(value, nsecs, nodeId, clockId);
  }
  Uuid.call(this, buffer);
}

util.inherits(TimeUuid, Uuid);

function writeTime(buffer, time, nsecs) {
  //There is risk of overflow time * 10000, but not in (time & 0xffffffff) * 1000
  //Translate to ticks
  var timeLow = (time  & 0xffffffff) * _ticksInMs;
  timeLow = (timeLow + nsecs) & 0xffffffff;
  buffer.writeUInt32BE(timeLow, 0, true);

  //To avoid overflow, first extract only the most significant bytes that need to be included
  var timeHigh = (time / 0x100000000);
  //Translate to ticks
  timeHigh = ( timeHigh * _ticksInMs) & 0xffffffff;
  //buffer.writeUInt32LE(timeHigh, 4, true);
  buffer.writeUInt16BE(timeHigh & 0xffff, 4, true);
  buffer.writeUInt16BE(timeHigh >>> 16 & 0xffff, 6, true);
}

/**
 * Returns a buffer of length 2 representing the clock identifier
 * @param {String|Buffer} clockId
 * @returns {Buffer}
 */
function getClockId(clockId) {
  var buffer = clockId;
  if (typeof clockId === 'string') {
    buffer = new Buffer(clockId, 'ascii');
  }
  if (!(buffer instanceof Buffer)) {
    //Generate
    buffer = getRandomBytes(2);
  }
  else if (buffer.length != 2) {
    throw new Error('Clock identifier must have 2 bytes');
  }
  return buffer;
}

/**
 * Returns a buffer of length 6 representing the clock identifier
 * @param {String|Buffer} nodeId
 * @returns {Buffer}
 */
function getNodeId(nodeId) {
  var buffer = nodeId;
  if (typeof nodeId === 'string') {
    buffer = new Buffer(nodeId, 'ascii');
  }
  if (!(buffer instanceof Buffer)) {
    //Generate
    buffer = getRandomBytes(6);
  }
  else if (buffer.length != 6) {
    throw new Error('Node identifier must have 6 bytes');
  }
  return buffer;
}

function getNsecs(nsecs) {
  if (typeof nsecs !== 'number'|| nsecs >= _ticksInMs) {
    _nsecs++;
    if (_nsecs >= _ticksInMs) {
      _nsecs = 0;
    }
    nsecs = _nsecs;
  }
  return nsecs;
}

/**
 * @returns {Number} Returns the time representation of the date expressed in milliseconds since gregorian epoch.
 */
function getTime(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) {
    date = new Date();
  }
  return date.getTime() + _unixToGregorian;
}

function getRandomBytes(length) {
  return crypto.randomBytes(length);
}

/**
 * Generates a 16-length Buffer instance
 * @private
 * @param {Date} date
 * @param {Number} nsecs
 * @param {String|Buffer} nodeId
 * @param {String|Buffer} clockId
 * @returns {Buffer}
 */
function generateBuffer(date, nsecs, nodeId, clockId) {
  var time = getTime(date);
  nsecs = getNsecs(nsecs);
  nodeId = getNodeId(nodeId);
  clockId = getClockId(clockId);
  var buffer = new Buffer(16);
  //Positions 0-7 Timestamp
  writeTime(buffer, time, nsecs);
  //Position 8-9 Clock
  clockId.copy(buffer, 8, 0);
  //Positions 10-15 Node
  nodeId.copy(buffer, 10, 0);
  //Version Byte: Time based
  //0001xxxx
  //turn off first 4 bits
  buffer[6] = buffer[6] & 0x0f;
  //turn on fifth bit
  buffer[6] = buffer[6] | 0x10;

  //IETF Variant Byte: 1.0.x
  //10xxxxxx
  //turn off first 2 bits
  buffer[8] = buffer[8] & 0x3f;
  //turn on first bit
  buffer[8] = buffer[8] | 0x80;
  return buffer;
}

module.exports = TimeUuid;