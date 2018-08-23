'use strict';

const Cache = require('./cache');
const Tree = require('./tree');
const ModelBatchItem = require('./model-batch-item');

/**
 * Provides utility methods to group multiple doc mutations on a single batch.
 */
class ModelBatchMapper {
  /**
   * Creates a new instance of model batch mapper.
   * @param {MappingHandler} handler
   * @ignore
   */
  constructor(handler) {
    this._handler = handler;
    this._cache = {
      insert: new Tree(),
      update: new Tree(),
      remove: new Tree()
    };
  }

  /**
   * Gets a <code>ModelBatchItem</code> containing the queries for this INSERT mutation to be used in a batch execution.
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}|null} [docInfo]
   * @returns {ModelBatchItem}
   */
  insert(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getInsertKey(docKeys, docInfo);
    const cacheItem = this._cache.insert.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createInsertQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo);
  }

  /**
   * Gets a <code>ModelBatchItem</code> containing the queries for this UPDATE mutation to be used in a batch execution.
   * @param {Object} doc
   * @param {{when, ifExists, ttl, fields}|null} [docInfo]
   * @returns {ModelBatchItem}
   */
  update(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getUpdateKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.update.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createUpdateQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo);
  }

  /**
   * Gets a <code>ModelBatchItem</code> containing the queries for this DELETE mutation to be used in a batch execution.
   * @param {Object} doc
   * @param {{when, ifExists, fields}|null} [docInfo]
   * @returns {ModelBatchItem}
   */
  remove(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getRemoveKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.remove.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createDeleteQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo);
  }
}

module.exports = ModelBatchMapper;