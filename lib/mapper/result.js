'use strict';

class Result {
  /**
   * @param {ResultSet} rs
   * @param {TableMappingInfo} info
   * @param {Function} rowAdapter
   */
  constructor(rs, info, rowAdapter) {
    this._rs = rs;
    this._info = info;
    this._rowAdapter = rowAdapter;
  }

  first() {
    if (!this._rs.rowLength) {
      return null;
    }
    return this._rowAdapter(this._rs.rows[0], this._info);
  }

  [Symbol.iterator]() {
    throw new Error('Not implemented');
  }

  toArray() {
    // Use a simple loop that is faster than a map
    const arr = new Array(this._rs.rows.length);
    for (let i = 0; i < this._rs.rows.length; i++) {
      arr[i] = this._rowAdapter(this._rs.rows[i], this._info);
    }
    return arr;
  }
}

module.exports = Result;