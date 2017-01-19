/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var Geometry = require('./geometry');

/**
 * Creates a new {@link Point} instance.
 * @classdesc
 * A Point is a zero-dimensional object that represents a specific (X,Y)
 * location in a two-dimensional XY-Plane. In case of Geographic Coordinate
 * Systems, the X coordinate is the longitude and the Y is the latitude.
 * @param {Number} x The X coordinate.
 * @param {Number} y The Y coordinate.
 * @extends {Geometry}
 * @alias module:geometry~Point
 * @constructor
 */
function Point(x, y) {
  if (typeof x !== 'number' || typeof y !== 'number') {
    throw new TypeError('X and Y must be numbers');
  }
  if (isNaN(x) || isNaN(y)) {
    throw new TypeError('X and Y must be numbers');
  }
  /**
   * Returns the X coordinate of this 2D point.
   * @type {Number}
   */
  this.x = x;
  /**
   * Returns the Y coordinate of this 2D point.
   * @type {Number}
   */
  this.y = y;
}

//noinspection JSCheckFunctionSignatures
util.inherits(Point, Geometry);

/**
 * Creates a {@link Point} instance from
 * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
 * representation of a 2D point.
 * @param {Buffer} buffer
 * @returns {Point}
 */
Point.fromBuffer = function (buffer) {
  if (!buffer || buffer.length !== 21) {
    throw new TypeError('2D Point buffer should contain 21 bytes');
  }
  var endianness = Geometry.getEndianness(buffer.readInt8(0, true));
  if (Geometry.readInt32(buffer, endianness, 1) !== Geometry.types.Point2D) {
    throw new TypeError('Binary representation was not a point');
  }
  return new Point(Geometry.readDouble(buffer, endianness, 5), Geometry.readDouble(buffer, endianness, 13));
};

/**
 * Creates a {@link Point} instance from
 * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
 * representation of a 2D point.
 * @param {String} textValue
 * @returns {Point}
 */
Point.fromString = function (textValue) {
  var wktRegex = /^POINT\s?\(([-0-9.]+) ([-0-9.]+)\)$/g;
  var matches = wktRegex.exec(textValue);
  if (!matches || matches.length !== 3) {
    throw new TypeError('2D Point WTK should contain 2 coordinates');
  }
  return new Point(parseFloat(matches[1]), parseFloat(matches[2]));
};

/**
 * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
 * representation of this instance.
 * @returns {Buffer}
 */
Point.prototype.toBuffer = function () {
  var buffer = new Buffer(21);
  this.writeEndianness(buffer, 0);
  this.writeInt32(Geometry.types.Point2D, buffer, 1);
  this.writeDouble(this.x, buffer, 5);
  this.writeDouble(this.y, buffer, 13);
  return buffer;
};

/**
 * Returns true if the values of the point are the same, otherwise it returns false.
 * @param {Point} other
 * @returns {Boolean}
 */
Point.prototype.equals = function (other) {
  if (!(other instanceof Point)) {
    return false;
  }
  return (this.x === other.x && this.y === other.y);
};

/**
 * Returns Well-known text (WKT) representation of the geometry object.
 * @returns {String}
 */
Point.prototype.toString = function () {
  return util.format('POINT (%d %d)', this.x, this.y);
};

Point.prototype.useBESerialization = function () {
  return false;
};

/**
 * Returns a JSON representation of this geo-spatial type.
 */
Point.prototype.toJSON = function () {
  return { type: 'Point', coordinates: [ this.x, this.y ]};
};

module.exports = Point;