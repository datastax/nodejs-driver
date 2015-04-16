"use strict";
/** @module types */
/**
 * Creates a new sequence of immutable objects with the parameters provided.
 * @class
 * @classdesc A tuple is a sequence of immutable objects.
 * Tuples are sequences, just like [Arrays]{@link Array}. The only difference is that tuples can't be changed.
 * <p>
 *   As tuples can be used as a Map keys, the {@link Tuple#toString toString()} method calls toString of each element,
 *   to try to get a unique string key.
 * </p>
 * @param [arguments] The sequence elements as arguments.
 * @constructor
 */
function Tuple() {
  var elements = Array.prototype.slice.call(arguments);
  if (elements.length === 0) {
    throw new TypeError('Tuple must contain at least one value');
  }
  Object.defineProperty(this, 'elements', { value: elements, enumerable: false, writable: false });
  /**
   * Returns the number of the elements.
   * @name length
   * @type Number
   * @memberof module:types~Tuple#
   */
  Object.defineProperty(this, 'length', { value: elements.length, enumerable: false, writable: false });
}

/**
 * Returns the value located at the index.
 * @param {Number} index Element index
 */
Tuple.prototype.get = function (index) {
  return this.elements[index || 0];
};

/**
 * Returns the string representation of the sequence surrounded by parenthesis, ie: (1, 2).
 * @returns {string}
 */
Tuple.prototype.toString = function () {
  return ('(' +
    this.elements.reduce(function (prev, x, i) {
      return prev + (i > 0 ? ',' : '') + x.toString();
    }, '') +
    ')');
};

/**
 * Returns the string representation of the sequence surrounded by square brackets, ie: ["1", "2"].
 * @returns {string}
 */
Tuple.prototype.toJSON = function () {
  return ('[' +
    this.elements.reduce(function (prev, x, i) {
      return prev + (i > 0 ? ',"' : '"') + x.toString() + '"';
    }, '') +
    ']');
};

/**
 * Gets the elements as an array
 * @returns {Array}
 */
Tuple.prototype.values = function () {
  return this.elements.slice(0);
};

module.exports = Tuple;