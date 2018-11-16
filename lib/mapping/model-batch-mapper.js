'use strict';

const Cache = require('./cache');
const Tree = require('./tree');
const ModelBatchItem = require('./model-batch-item');

/**
 * Provides utility methods to group multiple mutations on a single batch.
 * @alias module:mapping~ModelBatchMapper
 */
class ModelBatchMapper {
  /**
   * Creates a new instance of model batch mapper.
   * <p>
   *   An instance of this class is exposed as a singleton in the <code>batching</code> field of the
   *   [ModelMapper]{@link module:mapping~ModelMapper}. Note that new instances should not be create with this
   *   constructor.
   * </p>
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
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the INSERT mutation to be
   * used in a batch execution.
   * @param {Object} doc An object containing the properties to insert.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * INSERT cql statements generated. If specified, it must include the columns to insert and the primary keys.
   * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
   * @param {Boolean} [docInfo.ifNotExists] When set, it only inserts if the row does not exist prior to the insertion.
   * <p>Please note that using IF NOT EXISTS will incur a non negligible performance cost so this should be used
   * sparingly.</p>
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  insert(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getInsertKey(docKeys, docInfo);
    const cacheItem = this._cache.insert.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createInsertQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo, this._handler.info);
  }

  /**
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the UPDATE mutation to be
   * used in a batch execution.
   * @param {Object} doc An object containing the properties to update.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * UPDATE cql statements generated. If specified, it must include the columns to update and the primary keys.
   * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
   * @param {Boolean} [docInfo.ifExists] When set, it only updates if the row already exists on the server.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the UPDATE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  update(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getUpdateKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.update.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createUpdateQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo, this._handler.info);
  }

  /**
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem}  containing the queries for the DELETE mutation to be
   * used in a batch execution.
   * @param {Object} doc A document containing the primary keys values of the document to delete.
   * @param {Object} [docInfo] An object containing the additional doc information.
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * When the CQL query is generated, this would be used to generate the `IF` clause.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Boolean} [docInfo.ifExists] When set, it only issues the DELETE command if the row already exists on the
   * server.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
   * @param {Boolean} [docInfo.deleteOnlyColumns] Determines that, when more document properties are specified
   * besides the primary keys, the generated DELETE statement should be used to delete some column values but leave
   * the row. When this is enabled and more properties are specified, a DELETE statement will have the following form:
   * "DELETE col1, col2 FROM table1 WHERE pk1 = ? AND pk2 = ?"
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  remove(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getRemoveKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.remove.getOrCreate(cacheKey, () => ({ queries: null }));

    if (cacheItem.queries === null) {
      cacheItem.queries = this._handler.createDeleteQueries(docKeys, doc, docInfo);
    }

    return new ModelBatchItem(cacheItem.queries, doc, docInfo, this._handler.info);
  }
}

module.exports = ModelBatchMapper;