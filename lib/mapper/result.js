'use strict';

const utils = require('../utils');

/**
 * Represents a Result in the Mapper.
 */
class Result {
  /**
   * Creates a new instance of Result.
   * @ignore
   * @param {ResultSet} rs
   * @param {TableMappingInfo} info
   * @param {Function} rowAdapter
   */
  constructor(rs, info, rowAdapter) {
    this._rs = rs;
    this._info = info;
    this._rowAdapter = rowAdapter;

    /**
     * When there is a single cell containing the result of the a LWT operation, hide the result from the user.
     * @private
     */
    this._isEmptyLwt = (rs.columns !== null
      && rs.columns.length === 1 && this._rs.rowLength === 1 && rs.columns[0].name === '[applied]');

    /**
     * Gets the amount of the documents contained in this Result instance.
     * <p>
     *   When the results are paged, it returns the length of the current paged results not the total amount of
     *   rows in the table matching the query.
     * </p>.
     * @type {Number}
     */
    this.length = this._isEmptyLwt ? 0 : (rs.rowLength || 0);
  }

  /**
   * When this instance is the result of a conditional update query, it returns whether it was successful.
   * Otherwise, it returns <code>true</code>.
   * <p>
   *   For consistency, this method always returns <code>true</code> for non-conditional queries (although there is
   *   no reason to call the method in that case). This is also the case for conditional DDL statements
   *   (CREATE KEYSPACE... IF NOT EXISTS, CREATE TABLE... IF NOT EXISTS), for which the server doesn't return
   *   information whether it was applied or not.
   * </p>
   */
  wasApplied() {
    return this._rs.wasApplied();
  }

  /**
   * Gets the first document in this result or null when the result is empty.
   */
  first() {
    if (!this._rs.rowLength || this._isEmptyLwt) {
      return null;
    }
    return this._rowAdapter(this._rs.rows[0], this._info);
  }

  /**
   * Returns a new Iterator object that contains the document values.
   */
  *[Symbol.iterator]() {
    if (this._isEmptyLwt) {
      // Empty iterator
      return;
    }

    for (let i = 0; i < this._rs.rows.length; i++) {
      yield this._rowAdapter(this._rs.rows[i], this._info);
    }
  }

  /**
   * Converts the current instance to an Array of documents.
   * @return {Array<Object>}
   */
  toArray() {
    if (this._isEmptyLwt) {
      return utils.emptyArray;
    }

    // Use a simple loop that is faster than a map
    const arr = new Array(this._rs.rows.length);
    for (let i = 0; i < this._rs.rows.length; i++) {
      arr[i] = this._rowAdapter(this._rs.rows[i], this._info);
    }
    return arr;
  }

  inspect() {
    return this.toArray();
  }
}

module.exports = Result;