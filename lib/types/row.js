"use strict";
/** @module types */
/**
 * Represents a result row
 * @param {Array} columns
 * @constructor
 */
function Row(columns) {
  if (!columns) {
    throw new Error('Columns not defined');
  }
  //Private non-enumerable properties, with double underscore to avoid interfering with column names
  Object.defineProperty(this, '__columns', { value: columns, enumerable: false, writable: false});
}

/**
 * Returns the cell value.
 * @param {String|Number} columnName Name or index of the column
 */
Row.prototype.get = function (columnName) {
  if (typeof columnName === 'number') {
    //its an index
    return this[this.__columns[columnName].name];
  }
  return this[columnName];
};

/**
 * Returns an array of the values of the row
 * @returns {Array}
 */
Row.prototype.values = function () {
  const valuesArray = [];
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
  const keysArray = [];
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
  for (const columnName in this) {
    if (!this.hasOwnProperty(columnName)) {
      continue;
    }
    callback(this[columnName], columnName);
  }
};

module.exports = Row;