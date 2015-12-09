"use strict";
var util = require('util');
var DataCollection = require('./data-collection');
//noinspection JSValidateJSDoc
/**
 * Creates a new MaterializedView.
 * @param {String} name Name of the View.
 * @classdesc Describes a CQL materialized view.
 * @alias module:metadata~MaterializedView
 * @augments {module:metadata~DataCollection}
 * @constructor
 */
function MaterializedView(name) {
  DataCollection.call(this, name);
  /**
   * Name of the table.
   * @type {String}
   */
  this.tableName = null;
  /**
   * View where clause.
   * @type {String}
   */
  this.whereClause = null;
  /**
   * Determines if all the table columns where are included in the view.
   * @type {boolean}
   */
  this.includeAllColumns = false;
}

//noinspection JSCheckFunctionSignatures
util.inherits(MaterializedView, DataCollection);

module.exports = MaterializedView;