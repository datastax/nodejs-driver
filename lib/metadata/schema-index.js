"use strict";
var util = require('util');
var utils = require('../utils');
var types = require('../types');

/** @private */
var kind = {
  custom: 0,
  keys: 1,
  composites: 2
};
/**
 * Creates a new Index instance.
 * @classdesc Describes a CQL index.
 * @param {String} name
 * @param {String} target
 * @param {Number} kind
 * @param {Object} options
 * @alias module:metadata~Index
 * @constructor
 */
function Index(name, target, kind, options) {
  /**
   * Name of the index.
   * @type {String}
   */
  this.name = name;
  /**
   * Target of the index.
   * @type {String}
   */
  this.target = target;
  /**
   * A numeric value representing index kind (0: custom, 1: keys, 2: composite);
   * @type {Number}
   */
  this.kind = kind;
  /**
   * An associative array containing the index options
   * @type {Object}
   */
  this.options = options;
}

/**
 * Determines if the index is of composites kind
 * @returns {Boolean}
 */
Index.prototype.isCompositesKind = function () {
  return this.kind === kind.composites;
};

/**
 * Determines if the index is of keys kind
 * @returns {Boolean}
 */
Index.prototype.isKeysKind = function () {
  return this.kind === kind.keys;
};

/**
 * Determines if the index is of custom kind
 * @returns {Boolean}
 */
Index.prototype.isCustomKind = function () {
  return this.kind === kind.custom;
};

/**
 * Parses Index information from rows in the 'system_schema.indexes' table
 * @param {Array.<Row>} indexRows
 * @returns {Array.<Index>}
 */
Index.fromRows = function (indexRows) {
  if (!indexRows || indexRows.length === 0) {
    return utils.emptyArray;
  }
  return indexRows.map(function (row) {
    var options = row['options'];
    return new Index(row['index_name'], options['target'], getKindByName(row['kind']), options);
  });
};

/**
 * Parses Index information from rows in the legacy 'system.schema_columns' table
 * @param {Array.<Row>} columnRows
 * @param {Object.<String, {name, type}>} columnsByName
 * @returns {Array.<Index>}
 */
Index.fromColumnRows = function (columnRows, columnsByName) {
  var result = [];
  for (var i = 0; i < columnRows.length; i++) {
    var row = columnRows[i];
    var indexName = row['index_name'];
    if (!indexName) {
      continue;
    }
    var c = columnsByName[row['column_name']];
    var target;
    var options = JSON.parse(row['index_options']);
    if (options !== null && options['index_keys'] !== undefined) {
      target = util.format("keys(%s)", c.name);
    }
    else if (options !== null && options['index_keys_and_values'] !== undefined) {
      target = util.format("entries(%s)", c.name);
    }
    else if (c.type.options.frozen && (c.type.code === types.dataTypes.map || c.type.code === types.dataTypes.list ||
      c.type.code === types.dataTypes.set)) {
      target = util.format("full(%s)", c.name);
    }
    else {
      target = c.name;
    }
    result.push(new Index(indexName, target, getKindByName(row['index_type']), options));
  }
  return result;
};

/**
 * Gets the number representing the kind based on the name
 * @param {String} name
 * @returns {Number}
 * @private
 */
function getKindByName(name) {
  if (!name) {
    return kind.custom;
  }
  return kind[name.toLowerCase()];
}

module.exports = Index;