'use strict';

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
    //TODO: Hide the empty row
    return this._rs.wasApplied();
  }

  /**
   * Gets the first document in this result or null when the result is empty.
   */
  first() {
    if (!this._rs.rowLength) {
      return null;
    }
    return this._rowAdapter(this._rs.rows[0], this._info);
  }

  /**
   * Returns a new Iterator object that contains the document values.
   */
  *[Symbol.iterator]() {
    for (let i = 0; i < this._rs.rows.length; i++) {
      yield this._rowAdapter(this._rs.rows[i], this._info);
    }
  }

  /**
   * Converts the current instance to an Array of documents.
   * @return {Array<Object>}
   */
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