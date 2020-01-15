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
const Point = require('./point');

/**
 * Creates a new {@link LineString} instance.
 * @classdesc
 * A LineString is a one-dimensional object representing a sequence of points and the line segments connecting them.
 * @param {...Point}[point] A sequence of [Point]{@link module:geometry~Point} items as arguments.
 * @example
 * new LineString(new Point(10.99, 20.02), new Point(14, 26), new Point(34, 1.2));
 * @constructor
 * @alias module:geometry~LineString
 * @extends {Geometry}
 */
function LineString(point) {
  let points = Array.prototype.slice.call(arguments);
  if (points.length === 1 && Array.isArray(points) && Array.isArray(points[0])) {
    //The first argument is an array of the points
    points = points[0];
  }
  if (points.length === 1) {
    throw new TypeError('LineString can be either empty or contain 2 or more points');
  }
  /**
   * Returns a frozen Array of points that represent the line.
   * @type {Array.<Point>}
   */
  this.points = Object.freeze(points);
}

//noinspection JSCheckFunctionSignatures
util.inherits(LineString, Geometry);

/**
 * Creates a {@link LineString} instance from
 * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
 * representation of a line.
 * @param {Buffer} buffer
 * @returns {LineString}
 */
LineString.fromBuffer = function (buffer) {
  if (!buffer || buffer.length < 9) {
    throw new TypeError('A linestring buffer should contain at least 9 bytes');
  }
  const endianness = Geometry.getEndianness(buffer.readInt8(0, true));
  let offset = 1;
  if (Geometry.readInt32(buffer, endianness, offset) !== Geometry.types.LineString) {
    throw new TypeError('Binary representation was not a LineString');
  }
  offset += 4;
  const length = Geometry.readInt32(buffer, endianness, offset);
  offset += 4;
  if (buffer.length !== offset + length * 16) {
    throw new TypeError(util.format('Length of the buffer does not match %d !== %d', buffer.length, offset + length * 8));
  }
  const points = new Array(length);
  for (let i = 0; i < length; i++) {
    points[i] = new Point(
      Geometry.readDouble(buffer, endianness, offset),
      Geometry.readDouble(buffer, endianness, offset + 8));
    offset += 16;
  }
  //noinspection JSCheckFunctionSignatures
  return new LineString(points);
};

/**
 * Creates a {@link LineString} instance from
 * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
 * representation of a line.
 * @param {String} textValue
 * @returns {LineString}
 */
LineString.fromString = function (textValue) {
  const wktRegex = /^LINESTRING ?\(([-0-9. ,]+)\)+$/g;
  const matches = wktRegex.exec(textValue);
  if (!matches || matches.length !== 2) {
    throw new TypeError('Invalid WKT: ' + textValue);
  }
  const points = LineString.parseSegments(matches[1]);
  return new LineString(points);
};

/**
 * Internal method that parses a series of WKT points.
 * @param {String} textValue
 * @returns {Array<Point>}
 * @internal
 * @ignore
 */
LineString.parseSegments = function (textValue) {
  const points = [];
  const pointParts = textValue.split(',');
  for (let i = 0; i < pointParts.length; i++) {
    const p = pointParts[i].trim();
    if (p.length === 0) {
      throw new TypeError('Invalid WKT segment: ' + textValue);
    }
    const xyText = p.split(' ').filter(function (element) {
      return (element.trim().length > 0);
    });
    if (xyText.length !== 2) {
      throw new TypeError('Invalid WKT segment: ' + textValue);
    }
    points.push(new Point(parseFloat(xyText[0]), parseFloat(xyText[1])));
  }
  return points;
};

/**
 * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
 * representation of this instance.
 * @returns {Buffer}
 */
LineString.prototype.toBuffer = function () {
  const buffer = utils.allocBufferUnsafe(9 + this.points.length * 16);
  this.writeEndianness(buffer, 0);
  let offset = 1;
  this.writeInt32(Geometry.types.LineString, buffer, offset);
  offset += 4;
  this.writeInt32(this.points.length, buffer, offset);
  offset += 4;
  this.points.forEach(function (p) {
    this.writeDouble(p.x, buffer, offset);
    this.writeDouble(p.y, buffer, offset + 8);
    offset += 16;
  }, this);
  return buffer;
};

/**
 * Returns true if the values of the linestrings are the same, otherwise it returns false.
 * @param {LineString} other
 * @returns {Boolean}
 */
LineString.prototype.equals = function (other) {
  if (!(other instanceof LineString)) {
    return false;
  }
  if (this.points.length !== other.points.length) {
    return false;
  }
  for (let i = 0; i < this.points.length; i++) {
    if (!this.points[i].equals(other.points[i])) {
      return false;
    }
  }
  return true;
};

/**
 * Returns Well-known text (WKT) representation of the geometry object.
 * @returns {String}
 */
LineString.prototype.toString = function () {
  if (this.points.length === 0) {
    return 'LINESTRING EMPTY';
  }
  return 'LINESTRING ('
    + this.points.map(function (p) {
      return p.x + ' ' + p.y;
    }).join(', ')
    + ')';
};

LineString.prototype.useBESerialization = function () {
  return false;
};

/**
 * Returns a JSON representation of this geo-spatial type.
 */
LineString.prototype.toJSON = function () {
  return { type: 'LineString', coordinates: this.points.map(function (p) {
    return [p.x, p.y];
  })};
};

module.exports = LineString;