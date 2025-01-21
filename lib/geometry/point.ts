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
const utils = require('../utils');
const Geometry = require('./geometry');

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
  const endianness = Geometry.getEndianness(buffer.readInt8(0, true));
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
  const wktRegex = /^POINT\s?\(([-0-9.]+) ([-0-9.]+)\)$/g;
  const matches = wktRegex.exec(textValue);
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
  const buffer = utils.allocBufferUnsafe(21);
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