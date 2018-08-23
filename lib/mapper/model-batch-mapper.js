'use strict';

const Cache = require('./cache');
const Tree = require('./tree');
const ModelBatchItem = require('./model-batch-item');

/**
 * Provides utility methods to group multiple doc mutations on a single batch.
 */
class ModelBatchMapper {
  /**
   *
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
   * Gets a <code>ModelBatchItem</code> containing the queries for this UPDATE mutation to be used in a batch execution.
   * @param doc
   * @param docInfo
   * @returns {Promise.<Array.<{query, paramsGetter}>>|*}
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
}

module.exports = ModelBatchMapper;