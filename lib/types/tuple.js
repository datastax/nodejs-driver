/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

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

  /**
   * Immutable elements of Tuple object.
   * @type Array
   */
  this.elements = Array.prototype.slice.call(arguments);

  if (this.elements.length === 0) {
    throw new TypeError('Tuple must contain at least one value');
  }

  /**
   * Returns the number of the elements.
   * @type Number
   */
  this.length = this.elements.length;
}

/**
 * Creates a new instance of a tuple based on the Array
 * @param {Array} elements
 * @returns {Tuple}
 */
Tuple.fromArray = function (elements) {
  // No spread operator in Node.js 4 :/
  // return new Tuple(...elements);
  // Apply the elements Array as parameters
  return new (Function.prototype.bind.apply(Tuple, [ null ].concat(elements)));
};

/**
 * Returns the value located at the index.
 * @param {Number} index Element index
 */
Tuple.prototype.get = function (index) {
  return this.elements[index || 0];
};

/**
 * Returns the string representation of the sequence surrounded by parenthesis, ie: (1, 2).
 * <p>
 *   The returned value attempts to be a unique string representation of its values.
 * </p>
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
 * Returns the Array representation of the sequence.
 * @returns {Array}
 */
Tuple.prototype.toJSON = function () {
  return this.elements;
};

/**
 * Gets the elements as an array
 * @returns {Array}
 */
Tuple.prototype.values = function () {
  return this.elements.slice(0);
};

module.exports = Tuple;