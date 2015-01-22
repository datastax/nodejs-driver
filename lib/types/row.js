/** @module types */
/**
 * Represents a result row
 * @constructor
 */
function Row(columns) {
  Object.defineProperty(this, "__columns", { value: columns, enumerable: false});
}

/**
 * Returns the cell value.
 * @param {String|Number} columnName Name or index of the column
 */
Row.prototype.get = function (columnName) {
  if (typeof columnName === 'number') {
    if (this.__columns && this.__columns[columnName]) {
      columnName = this.__columns[columnName].name;
    }
    else {
      throw new Error('Column not found');
    }
  }
  return this[columnName];
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

Row.prototype.forEach = function (callback) {
  for (var columnName in this) {
    if (!this.hasOwnProperty(columnName)) {
      continue;
    }
    if (columnName === '__columns') {
      continue;
    }
    callback(this[columnName], columnName);
  }
};

module.exports = Row;