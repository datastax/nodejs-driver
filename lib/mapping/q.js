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

const errors = require('../errors');

/**
 * Represents a CQL query operator, like >=, IN, <, ...
 * @ignore
 */
class QueryOperator {
  /**
   * Creates a new instance of <code>QueryOperator</code>.
   * @param {String} key
   * @param value
   * @param [hasChildValues]
   * @param [isInOperator]
   */
  constructor(key, value, hasChildValues, isInOperator) {
    /**
     * The CQL key representing the operator
     * @type {string}
     */
    this.key = key;

    /**
     * The value to be used as parameter.
     */
    this.value = value;

    /**
     * Determines whether a query operator can have child values or operators (AND, OR)
     */
    this.hasChildValues = hasChildValues;

    /**
     * Determines whether this instance represents CQL "IN" operator.
     */
    this.isInOperator = isInOperator;
  }
}

/**
 * Represents a CQL assignment operation, like col = col + x.
 * @ignore
 */
class QueryAssignment {
  constructor(sign, value, inverted) {
    /**
     * Gets the sign of the assignment operation.
     */
    this.sign = sign;

    /**
     * Gets the value to be assigned.
     */
    this.value = value;

    /**
     * Determines whether the assignment should be inverted (prepends), e.g: col = x + col
     * @type {boolean}
     */
    this.inverted = !!inverted;
  }
}

/**
 * Contains functions that represents operators in a query.
 * @alias module:mapping~q
 * @type {Object}
 * @property {function} in_ Represents the CQL operator "IN".
 * @property {function} gt Represents the CQL operator greater than ">".
 * @property {function} gte Represents the CQL operator greater than or equals to ">=" .
 * @property {function} lt Represents the CQL operator less than "<" .
 * @property {function} lte Represents the CQL operator less than or equals to "<=" .
 * @property {function} notEq Represents the CQL operator not equals to "!=" .
 * @property {function} and When applied to a property, it represents two CQL conditions on the same column separated
 * by the logical AND operator, e.g: "col1 >= x col < y"
 * @property {function} incr Represents the CQL increment assignment used for counters, e.g: "col = col + x"
 * @property {function} decr Represents the CQL decrement assignment used for counters, e.g: "col = col - x"
 * @property {function} append Represents the CQL append assignment used for collections, e.g: "col = col + x"
 * @property {function} prepend Represents the CQL prepend assignment used for lists, e.g: "col = x + col"
 * @property {function} remove Represents the CQL remove assignment used for collections, e.g: "col = col - x"
 */
const q = {
  in_: function in_(arr) {
    if (!Array.isArray(arr)) {
      throw new errors.ArgumentError('IN operator supports only Array values');
    }
    return new QueryOperator('IN', arr, false, true);
  },

  gt: function gt(value) {
    return new QueryOperator('>', value);
  },

  gte: function gte(value) {
    return new QueryOperator('>=', value);
  },

  lt: function lt(value) {
    return new QueryOperator('<', value);
  },

  lte: function lte(value) {
    return new QueryOperator('<=', value);
  },

  notEq: function notEq(value) {
    return new QueryOperator('!=', value);
  },

  and: function (condition1, condition2) {
    return new QueryOperator('AND', [ condition1, condition2 ], true);
  },

  incr: function incr(value) {
    return new QueryAssignment('+', value);
  },

  decr: function decr(value) {
    return new QueryAssignment('-', value);
  },

  append: function append(value) {
    return new QueryAssignment('+', value);
  },

  prepend: function prepend(value) {
    return new QueryAssignment('+', value, true);
  },

  remove: function remove(value) {
    return new QueryAssignment('-', value);
  }
};

exports.q = q;
exports.QueryAssignment = QueryAssignment;
exports.QueryOperator = QueryOperator;