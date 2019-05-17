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

"use strict";
const util = require('util');
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
  let elements = Array.prototype.slice.call(arguments);
  if (elements.length === 0) {
    throw new TypeError('Tuple must contain at least one value');
  }
  if (elements.length === 1 && util.isArray(elements)) {
    //The first argument is an array of the elements, use a copy of the array
    elements = elements[0];
  }
 
  /** 
   * Immutable elements of Tuple object.
   * @type Array
   */
  this.elements = elements;

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
  //Use a copy of an array
  return new Tuple(elements.slice(0));
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