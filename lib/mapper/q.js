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
   */
  constructor(key, value) {
    /**
     * The CQL key representing the operator
     * @type {string}
     */
    this.key = key;

    /**
     * The value to be used as parameter.
     */
    this.value = value;
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
  }
};

exports.QueryOperator = QueryOperator;
exports.q = q;