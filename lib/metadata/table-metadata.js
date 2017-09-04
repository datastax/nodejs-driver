"use strict";
var util = require('util');
var DataCollection = require('./data-collection');
//noinspection JSValidateJSDoc
/**
 * Creates a new instance of TableMetadata
 * @classdesc Describes a table
 * @param {String} name Name of the Table
 * @augments {module:metadata~DataCollection}
 * @alias module:metadata~TableMetadata
 * @constructor
 */
function TableMetadata(name) {
  DataCollection.call(this, name);
  /**
   * Applies only to counter tables.
   * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
   * the client for a write request. For counter tables, this should always be set to true.
   * @type {Boolean}
   */
  this.replicateOnWrite = true;
  /**
   * Returns the memtable flush period (in milliseconds) option for this table.
   * <p>
   * Note: this option is available only on Cassandra 2.x and will return 0 (no periodic
   * flush) when connected to 1.2 nodes.
   * </p>
   * @type {Number}
   */
  this.memtableFlushPeriod = 0;
  /**
   * Returns the index interval option for this table.
   * <p>
   * Note: this option is only available in Cassandra 2.0. It is deprecated in Cassandra 2.1 and above, and will
   * therefore return {@code null} for 2.1 nodes.
   * </p>
   * @type {Number|null}
   */
  this.indexInterval = null;
  /**
   * Determines  whether the table uses the COMPACT STORAGE option.
   * @type {Boolean}
   */
  this.isCompact = false;
  /**
   *
   * @type {Array.<Index>}
   */
  this.indexes = null;

  /**
   * Determines whether the Change Data Capture (CDC) flag is set for the table.
   * @type {Boolean|null}
   */
  this.cdc = null;
}

//noinspection JSCheckFunctionSignatures
util.inherits(TableMetadata, DataCollection);

module.exports = TableMetadata;