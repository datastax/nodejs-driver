"use strict";
var util = require('util');
var DataCollection = require('./data-collection');
/** @module metadata */
/**
 * Creates a new instance of TableMetadata
 * @param {String} name Name of the Table
 * @class
 * @classdesc Describes a table
 * @augments module:metadata~DataCollection
 * @constructor
 */
function TableMetadata(name) {
  DataCollection.call(this, name);
  /**
   * Applies only to counter tables.
   * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
   * the client for a write request. For counter tables, this should always be set to true.
   * @type {boolean}
   */
  this.replicateOnWrite = true;
  /**
   * Determines  whether the table uses the COMPACT STORAGE option.
   * @type {boolean}
   */
  this.isCompact = false;
}

//noinspection JSCheckFunctionSignatures
util.inherits(TableMetadata, DataCollection);

module.exports = TableMetadata;