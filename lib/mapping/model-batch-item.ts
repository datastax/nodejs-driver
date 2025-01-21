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

const Cache = require('./cache');

/**
 * Represents a query or a set of queries used to perform a mutation in a batch.
 * @alias module:mapping~ModelBatchItem
 */
class ModelBatchItem {
  /**
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {MappingHandler} handler
   * @param {Tree} cache
   */
  constructor(doc, docInfo, handler, cache) {
    this.doc = doc;
    this.docInfo = docInfo;
    this.handler = handler;
    this.cache = cache;
  }

  /**
   * @ignore
   * @returns <Promise<Array>>
   */
  getQueries() {
    const docKeys = Object.keys(this.doc);
    const cacheItem = this.cache.getOrCreate(this.getCacheKey(docKeys), () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this.createQueries(docKeys);
    }

    return cacheItem.queries;
  }

  /**
   * Gets the cache key for this item.
   * @abstract
   * @param {Array} docKeys
   * @returns {Iterator}
   */
  getCacheKey(docKeys) {
    throw new Error('getCacheKey must be implemented');
  }

  /**
   * Gets the Promise to create the queries.
   * @abstract
   * @param {Array} docKeys
   * @returns {Promise<Array>}
   */
  createQueries(docKeys) {
    throw new Error('getCacheKey must be implemented');
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

    return this.getQueries().then(queries => {
      queries.forEach(q => {
        // It's idempotent if all the queries contained are idempotent
        isIdempotent = isIdempotent && q.isIdempotent;

        // Either all queries are counter mutation or we let it fail at server level
        isCounter = q.isCounter;

        arr.push({ query: q.query, params: q.paramsGetter(this.doc, this.docInfo, this.getMappingInfo()) });
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
    return this.handler.info;
  }
}

/**
 * Represents a single or a set of INSERT queries in a batch.
 * @ignore
 * @internal
 */
class InsertModelBatchItem extends ModelBatchItem {
  /**
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {MappingHandler} handler
   * @param {Tree} cache
   */
  constructor(doc, docInfo, handler, cache) {
    super(doc, docInfo, handler, cache);
  }

  /** @override */
  getCacheKey(docKeys) {
    return Cache.getInsertKey(docKeys, this.docInfo);
  }

  /** @override */
  createQueries(docKeys) {
    return this.handler.createInsertQueries(docKeys, this.doc, this.docInfo);
  }
}

/**
 * Represents a single or a set of UPDATE queries in a batch.
 * @ignore
 * @internal
 */
class UpdateModelBatchItem extends ModelBatchItem {
  /**
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {MappingHandler} handler
   * @param {Tree} cache
   */
  constructor(doc, docInfo, handler, cache) {
    super(doc, docInfo, handler, cache);
  }

  /** @override */
  getCacheKey(docKeys) {
    return Cache.getUpdateKey(docKeys, this.doc, this.docInfo);
  }

  /** @override */
  createQueries(docKeys) {
    return this.handler.createUpdateQueries(docKeys, this.doc, this.docInfo);
  }
}

/**
 * Represents a single or a set of DELETE queries in a batch.
 * @ignore
 * @internal
 */
class RemoveModelBatchItem extends ModelBatchItem {
  /**
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {MappingHandler} handler
   * @param {Tree} cache
   */
  constructor(doc, docInfo, handler, cache) {
    super(doc, docInfo, handler, cache);
  }

  /** @override */
  getCacheKey(docKeys) {
    return Cache.getRemoveKey(docKeys, this.doc, this.docInfo);
  }

  /** @override */
  createQueries(docKeys) {
    return this.handler.createDeleteQueries(docKeys, this.doc, this.docInfo);
  }
}

module.exports = { ModelBatchItem, InsertModelBatchItem, UpdateModelBatchItem, RemoveModelBatchItem };