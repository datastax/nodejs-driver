'use strict';

class QueryOperator {
  constructor() {
    //TODO: Change to actual property values
    /**
     * The CQL key representing the operator
     * @type {string}
     */
    this.key = 'IN';

    /**
     * An identifier of the operator.
     * @type {string}
     */
    this.hashCode = this.key;

    /**
     * The value to be used as parameter.
     */
    this.value = null;
  }
}

const q = {

};

exports.QueryOperator = QueryOperator;
exports.q = q;