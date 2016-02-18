'use strict';
var util = require('util');
var Geometry = require('./geometry');
var Point = require('./point');
var os = require('os');

/**
 * Creates a new {@link Circle} instance.
 * @classdesc
 * Represents the circle simple shape in Euclidean geometry composed by a center and a distance to the center (radius).
 * @param {Point} center Center of the shape.
 * @param {Number} radius Distance to the center.
 * @constructor
 */
function Circle(center, radius) {
  if (!(center instanceof Point) || typeof radius !== 'number') {
    throw new TypeError('You must provide a center and a radius');
  }
  if (radius <= 0) {
    throw new TypeError('Radius must be greater than zero');
  }
  /**
   * Returns the center of the circle. The centre of a circle is the point equidistant from the points on the edge.
   * @type {Point}
   */
  this.center = center;
  /**
   * Returns the scalar value of the distance to the center.
   * @type {Number}
   */
  this.radius = radius;
}

util.inherits(Circle, Geometry);

/**
 * Creates a new {@link Circle} instance from a non-standard binary representation:
 * <ol>
 * <li>byte 1: the byte order (0 for big endian, 1 for little endian)</li>
 * <li>bytes 2-5: an int representing the circle type</li>
 * <li>bytes 6-13: a double representing the circle's center X coordinate</li>
 * <li>bytes 14-21: a double representing the circle's center Y coordinate</li>
 * <li>bytes 22-29: a double representing the circle's radius</li>
 * </ol>
 * @param {Buffer} buffer
 * @returns {Circle}
 */
Circle.fromBuffer = function (buffer) {
  if (!buffer || buffer.length !== 29) {
    throw new TypeError('Circle buffer should contain 29 bytes');
  }
  var endianness = Geometry.getEndianness(buffer.readInt8(0, true));
  if (Geometry.readInt32(buffer, endianness, 1) !== Geometry.types.Circle) {
    throw new TypeError('Binary representation was not a Circle');
  }
  var center = new Point(Geometry.readDouble(buffer, endianness, 5), Geometry.readDouble(buffer, endianness, 13));
  return new Circle(center, Geometry.readDouble(buffer, endianness, 21));
};

/**
 * Returns the binary representation of the shape.
 * @returns {Buffer}
 */
Circle.prototype.toBuffer = function () {
  var buffer = new Buffer(29);
  this.writeEndianness(buffer, 0);
  this.writeInt32(Geometry.types.Circle, buffer, 1);
  this.writeDouble(this.center.x, buffer, 5);
  this.writeDouble(this.center.y, buffer, 13);
  this.writeDouble(this.radius, buffer, 21);
  return buffer;
};

/**
 * Returns true if the values of the point are the same, otherwise it returns false.
 * @param {Circle} other
 * @returns {Boolean}
 */
Circle.prototype.equals = function (other) {
  if (!(other instanceof Circle)) {
    return false;
  }
  return (this.radius === other.radius && this.center.equals(other.center));
};

/**
 * Returns a string representation of the geometry object in the form of <code>CIRCLE ((X Y) RADIUS)</code>.
 * @returns {String}
 */
Circle.prototype.toString = function () {
  return util.format('CIRCLE ((%d %d) %d)', this.center.x, this.center.y, this.radius);
};

Circle.prototype.isOSBE = function () {
  return os.endianness() === 'BE';
};

/**
 * Returns a JSON representation of this geo-spatial type.
 */
Circle.prototype.toJSON = function () {
  return { type: 'Circle', coordinates: [ this.center.x, this.center.y ], radius: this.radius };
};


module.exports = Circle;