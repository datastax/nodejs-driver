"use strict";
var util = require('util');
/** @module types */
/**
 * Represents a result row
 * @param {Encoder} encoder
 * @param {{columns: Array}} meta
 * @param {Array.<Buffer>} values
 * @constructor
 */
function Row(encoder, meta, values) {
  if (!encoder) {
    throw new Error('Encoder not defined');
  }
  if (!meta) {
    throw new Error('Metadata not defined');
  }
  if (!meta.columns) {
    throw new Error('Columns not defined');
  }
  if (!values) {
    throw new Error('Values not defined');
  }
  //Private non-enumerable properties, with double underscore to avoid interfering with column names
  Object.defineProperty(this, '__encoder', { value: encoder, enumerable: false, writable: false});
  Object.defineProperty(this, '__meta', { value: meta, enumerable: false, writable: false});
  Object.defineProperty(this, '__columns', { value: meta.columns, enumerable: false, writable: false});
  Object.defineProperty(this, '__values', { value: values, enumerable: false, writable: false});
  for (var i = 0; i < meta.columns.length; i++) {
    Object.defineProperty(this, meta.columns[i].name, { get: this._getValueFunc(i), enumerable: true });
  }
}

/**
 * Returns the cell value.
 * @param {String|Number} columnName Name or index of the column
 */
Row.prototype.get = function (columnName) {
  if (typeof columnName === 'number') {
    return this._getValueFunc(columnName)();
  }
  return this[columnName];
};

/**
 * @param {Number} index
 * @returns {Function}
 * @private
 */
Row.prototype._getValueFunc = function (index) {
  var self = this;
  return (function getValue() {
    return self.__encoder.decode(self.__values[index], self.__columns[index].type);
  });
};

/**
 * Returns an array of the values of the row
 * @returns {Array}
 */
Row.prototype.values = function () {
  var valuesArray = [];
  this.forEach(function (val) {
    valuesArray.push(val);
  });
  return valuesArray;
};

/**
 * Returns an array of the column names of the row
 * @returns {Array}
 */
Row.prototype.keys = function () {
  var keysArray = [];
  this.forEach(function (val, key) {
    keysArray.push(key);
  });
  return keysArray;
};

/**
 * Executes the callback for each field in the row, containing the value as first parameter followed by the columnName
 * @param {Function} callback
 */
Row.prototype.forEach = function (callback) {
  for (var columnName in this) {
    if (!this.hasOwnProperty(columnName)) {
      continue;
    }
    callback(this[columnName], columnName);
  }
};

/**
 * Evaluates each property
 * @returns {string}
 */
Row.prototype.inspect = function () {
  //In V8, string concatenation is faster than array join
  var result = '{ ';
  this.forEach(function (val, key) {
    if (result.length > 2) {
      result += ', ' + key + ': ' + util.inspect(val);
      return;
    }
    result += key + ': ' + util.inspect(val);
  });
  return result + ' }';
};

module.exports = Row;