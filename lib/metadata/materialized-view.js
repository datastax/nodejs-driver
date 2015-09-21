"use strict";
var util = require('util');
var DataCollection = require('./data-collection');

/** @module metadata */

/**
 * Creates a new MaterializedView.
 * @param {String} name Name of the View.
 * @class
 * @classdesc Describes a CQL materialized view.
 * @augments module:metadata~DataCollection
 * @constructor
 */
function MaterializedView(name) {
  DataCollection.call(this, name);
  /**
   * Name of the table
   * @type {String}
   */
  this.tableName = null;
  /**
   * View where clause
   * @type {String}
   */
  this.whereClause = null;
}

//noinspection JSCheckFunctionSignatures
util.inherits(MaterializedView, DataCollection);

module.exports = MaterializedView;