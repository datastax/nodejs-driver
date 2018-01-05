/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const endianness = {
  '0': 'BE',
  '1': 'LE'
};

function Geometry() {

}

Geometry.types = {
  Point2D: 1,
  LineString: 2,
  Polygon: 3
};

/**
 * @protected
 * @param {Number} code
 * @returns {String}
 * @ignore
 */
Geometry.getEndianness = function (code) {
  const value = endianness[code.toString()];
  if (typeof value === 'undefined') {
    throw new TypeError('Invalid endianness with code ' + code);
  }
  return value;
};

/**
 * Reads an int32 from binary representation based on endianness.
 * @protected
 * @param {Buffer} buffer
 * @param {String} endianness
 * @param {Number} offset
 * @returns Number
 * @ignore
 */
Geometry.readInt32 = function (buffer, endianness, offset) {
  if (endianness === 'BE') {
    return buffer.readInt32BE(offset, true);
  }
  return buffer.readInt32LE(offset, true);
};

/**
 * Reads an 64-bit double from binary representation based on endianness.
 * @protected
 * @param {Buffer} buffer
 * @param {String} endianness
 * @param {Number} offset
 * @returns Number
 * @ignore
 */
Geometry.readDouble = function (buffer, endianness, offset) {
  if (endianness === 'BE') {
    return buffer.readDoubleBE(offset, true);
  }
  return buffer.readDoubleLE(offset, true);
};

/**
 * Writes an 32-bit integer to binary representation based on OS endianness.
 * @protected
 * @param {Number} val
 * @param {Buffer} buffer
 * @param {Number} offset
 * @ignore
 */
Geometry.prototype.writeInt32 = function (val, buffer, offset) {
  if (this.useBESerialization()) {
    return buffer.writeInt32BE(val, offset, true);
  }
  return buffer.writeInt32LE(val, offset, true);
};

/**
 * Writes an 64-bit double to binary representation based on OS endianness.
 * @protected
 * @param {Number} val
 * @param {Buffer} buffer
 * @param {Number} offset
 * @ignore
 */
Geometry.prototype.writeDouble = function (val, buffer, offset) {
  if (this.useBESerialization()) {
    return buffer.writeDoubleBE(val, offset, true);
  }
  return buffer.writeDoubleLE(val, offset, true);
};

/**
 * Writes an 8-bit int that represents the OS endianness.
 * @protected
 * @param {Buffer} buffer
 * @param {Number} offset
 * @ignore
 */
Geometry.prototype.writeEndianness = function (buffer, offset) {
  if (this.useBESerialization()) {
    return buffer.writeInt8(0, offset, true);
  }
  return buffer.writeInt8(1, offset, true);
};

/**
 * Returns true if the serialization must be done in big-endian format.
 * Designed to allow injection of OS endianness.
 * @abstract
 * @ignore
 */
Geometry.prototype.useBESerialization = function () {
  throw new Error('Not Implemented');
};

module.exports = Geometry;