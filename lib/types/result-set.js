var util = require('util');
var utils = require('../utils');
/** @module types */

/**
 * Creates a new instance of ResultSet
 * @class
 * @classdesc Represents the result of a query
 * @param {Object} response
 * @param {String} host
 * @param {Object} triedHosts
 * @param {Number} consistency
 * @constructor
 */
function ResultSet(response, host, triedHosts, consistency) {
  /**
   * Information on the execution of a successful query:
   * @member {Object}
   * @property {Number} achievedConsistency The consistency level that has been actually achieved by the query.
   * @property {String} queriedHost The Cassandra host that coordinated this query.
   * @property {Object} triedHosts Gets the associative array of host that were queried before getting a valid response,
   * being the last host the one that replied correctly.
   */
  this.info = {
    queriedHost: host,
    triedHosts: triedHosts,
    achievedConsistency: consistency
  };
  /**
   * Gets an array rows returned by the query, in case the result was buffered.
   * @member {Array.<Row>}
   */
  this.rows = response.rows;
  /**
   * Gets the row length of the result, regardless if the result has been buffered or not
   * @member {Number}
   */
  this.rowLength = this.rows ? this.rows.length : response.rowLength;
  //Define meta as a private (not enumerable) property, for backward compatibility
  Object.defineProperty(this, 'meta', { value: response.meta, enumerable: false, writable: false });
  /**
   * A string token representing the current page state of query. It can be used in the following executions to
   * continue paging and retrieve the remained of the result for the query.
   * @name pageState
   * @type String
   * @memberof module:types~ResultSet#
   * @default null
   */
  Object.defineProperty(this, 'pageState', {get: this.getPageState.bind(this), enumerable: true});
  /**
   * Gets the columns returned in this ResultSet.
   * @name columns
   * @type Array
   * @memberof module:types~ResultSet#
   */
  Object.defineProperty(this, 'columns', {get: this.getColumns.bind(this), enumerable: true});
  //private properties
  Object.defineProperty(this, 'id', { value: response.id, enumerable: false});
  Object.defineProperty(this, '_columns', { value: undefined, enumerable: false, writable: true});
}

/**
 * Returns the first row or null if the result rows are empty
 */
ResultSet.prototype.first = function () {
  if (this.rows && this.rows.length) {
    return this.rows[0];
  }
  return null;
};

ResultSet.prototype.getPageState = function () {
  if (this.meta && this.meta.pageState) {
    return this.meta.pageState.toString('hex');
  }
  return null;
};

ResultSet.prototype.getColumns = function () {
  if (typeof this._columns !== 'undefined') {
    return this._columns;
  }
  if (!this.meta) {
    return null;
  }
  this._columns = utils.parseColumnDefinitions(this.meta.columns);
  return this._columns;
};

module.exports = ResultSet;