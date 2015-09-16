"use strict";
var util = require('util');
var events = require('events');
/** @module metadata */

/**
 * Creates a new MaterializedView.
 * @param {String} keyspaceName Name of the keyspace.
 * @param {TableMetadata} table Table metadata.
 * @class
 * @classdesc Describes a CQL materialized view.
 * @constructor
 */
function MaterializedView(keyspaceName, table) {
  /**
   * Gets the name of the keyspace.
   * @type {String}
   */
  this.keyspaceName = keyspaceName;
  /**
   * Gets the base table metadata
   * @type {TableMetadata}
   */
  this.table = table;
  /**
   * Gets the name of the materialized view.
   * @type {String}
   */
  this.name = null;
  /**
   * Array describing the clustering columns.
   * @type {Array}
   */
  this.clusteringColumns = [];
  /**
   * Array describing the clustering columns.
   * @type {Array}
   */
  this.includedColumns = [];
  /**
   * Array describing the target columns.
   * @type {Array}
   */
  this.targetColumns = [];
}

/**
 * Parses the materialized view row and returns a MaterializedView instance.
 * @param {String} keyspaceName
 * @param {TableMetadata} table
 * @param {Row} row
 * @returns {MaterializedView}
 * @internal
 * @ignore
 */
MaterializedView.parse = function (keyspaceName, table, row) {
  var view = new MaterializedView(keyspaceName, table);
  view.name = row['view_name'];
  var clusteringColumnNames = row['clustering_columns'];
  var includedColumnNames = row['included_columns'];
  var targetColumnNames = row['target_columns'];
  var i;
  for (i = 0; i < clusteringColumnNames.length; i++) {
    view.clusteringColumns.push(table.columnsByName[clusteringColumnNames[i]]);
  }
  for (i = 0; i < includedColumnNames.length; i++) {
    view.includedColumns.push(table.columnsByName[includedColumnNames[i]]);
  }
  for (i = 0; i < targetColumnNames.length; i++) {
    view.targetColumns.push(table.columnsByName[targetColumnNames[i]]);
  }
  return view;
};

module.exports = MaterializedView;