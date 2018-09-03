'use strict';

const errors = require('../errors');

/**
 * Represents a CQL query operator, like >=, IN, <, ...
 */
class QueryOperator {
  /**
   * Creates a new instance of <code>QueryOperator</code>.
   * @param {String} key
   * @param value
   * @param [hasChildValues]
   */
  constructor(key, value, hasChildValues) {
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
  }
}

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

const q = {
  /**
   * Represents the CQL operator "IN"
   * @param {Array} arr
   */
  in_: function in_(arr) {
    if (!Array.isArray(arr)) {
      throw new errors.ArgumentError('IN operator supports only Array values');
    }
    return new QueryOperator('IN', arr);
  },

  /**
   * Represents the CQL operator greater than ">" .
   * @param value
   */
  gt: function gt(value) {
    return new QueryOperator('>', value);
  },

  /**
   * Represents the CQL operator greater than or equals to ">=" .
   * @param value
   */
  gte: function gte(value) {
    return new QueryOperator('>=', value);
  },

  /**
   * Represents the CQL operator less than "<" .
   * @param value
   */
  lt: function lt(value) {
    return new QueryOperator('<', value);
  },

  /**
   * Represents the CQL operator less than or equals to "<=" .
   * @param value
   */
  lte: function lte(value) {
    return new QueryOperator('<=', value);
  },

  /**
   * Represents the CQL operator not equals to "!=" .
   * @param value
   */
  notEq: function notEq(value) {
    return new QueryOperator('!=', value);
  },

  /**
   * When applied to a property, it represents two CQL conditions on the same column separated by the logical
   * AND operator, e.g: "col1 >= x col < y"
   * @param condition1
   * @param condition2
   */
  and: function (condition1, condition2) {
    return new QueryOperator('AND', [ condition1, condition2 ], true);
  },

  /**
   * Represents the CQL increment assignment used for counters, e.g: "col = col + x"
   * @param {Long|Number} value
   */
  incr: function incr(value) {
    return new QueryAssignment('+', value);
  },

  /**
   * Represents the CQL decrement assignment used for counters, e.g: "col = col - x"
   * @param {Long|Number} value
   */
  decr: function decr(value) {
    return new QueryAssignment('-', value);
  },

  /**
   * Represents the CQL append assignment used for collections, e.g: "col = col + x"
   * @param {Array} value
   */
  append: function append(value) {
    return new QueryAssignment('+', value);
  },

  /**
   * Represents the CQL prepend assignment used for lists, e.g: "col = x + col"
   * @param {Array} value
   */
  prepend: function prepend(value) {
    return new QueryAssignment('+', value, true);
  },

  /**
   * Represents the CQL remove assignment used for collections, e.g: "col = col - x"
   * @param {Array} value
   */
  remove: function remove(value) {
    return new QueryAssignment('-', value);
  }
};

exports.q = q;
exports.QueryAssignment = QueryAssignment;
exports.QueryOperator = QueryOperator;