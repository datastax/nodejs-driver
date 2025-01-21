/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const util = require('util');
const utils = require('../utils');
const inspectMethod = util.inspect.custom || 'inspect';

/**
 * Represents the result of an execution as an iterable of objects in the Mapper.
 * @alias module:mapping~Result
 */
class Result {
  /**
   * Creates a new instance of Result.
   * @param {ResultSet} rs
   * @param {ModelMappingInfo} info
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
     * </p>
     * @type {Number}
     */
    this.length = this._isEmptyLwt ? 0 : (rs.rowLength || 0);

    /**
     * A string token representing the current page state of query.
     * <p>
     *   When provided, it can be used in the following executions to continue paging and retrieve the remained of the
     *   result for the query.
     * </p>
     * @type {String}
     * @default null
     */
    this.pageState = rs.pageState;
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
    if (this._isEmptyLwt || !this._rs.rows) {
      return utils.emptyArray;
    }

    return this._rs.rows.map(row => this._rowAdapter(row, this._info));
  }

  /**
   * Executes a provided function once per result element.
   * @param {Function} callback Function to execute for each element, taking two arguments: currentValue and index.
   * @param {Object} [thisArg] Value to use as <code>this</code> when executing callback.
   */
  forEach(callback, thisArg) {
    let index = 0;
    thisArg = thisArg || this;
    for (const doc of this) {
      callback.call(thisArg, doc, index++);
    }
  }

  [inspectMethod]() {
    return this.toArray();
  }
}

module.exports = Result;