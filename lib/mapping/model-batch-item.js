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

/**
 * Represents a query or a set of queries used to perform a mutation in a batch.
 * @alias module:mapping~ModelBatchItem
 */
class ModelBatchItem {
  /**
   * @param {Promise<Array>} queries
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {ModelMappingInfo} mappingInfo
   */
  constructor(queries, doc, docInfo, mappingInfo) {
    this._queries = queries;
    this._doc = doc;
    this._docInfo = docInfo;
    this._mappingInfo = mappingInfo;
  }

  /**
   * Pushes the queries and parameters represented by this instance to the provided array.
   * @internal
   * @ignore
   * @param {Array} arr
   * @return {Promise<{isIdempotent, isCounter}>}
   */
  pushQueries(arr) {
    let isIdempotent = true;
    let isCounter;
    return this._queries.then(queries => {
      queries.forEach(q => {
        // It's idempotent if all the queries contained are idempotent
        isIdempotent = isIdempotent && q.isIdempotent;

        // Either all queries are counter mutation or we let it fail at server level
        isCounter = q.isCounter;

        arr.push({ query: q.query, params: q.paramsGetter(this._doc, this._docInfo) });
      });

      return { isIdempotent, isCounter };
    });
  }

  /**
   * Gets the mapping information for this batch item.
   * @internal
   * @ignore
   */
  getMappingInfo() {
    return this._mappingInfo;
  }
}

module.exports = ModelBatchItem;