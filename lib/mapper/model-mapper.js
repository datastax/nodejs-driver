'use strict';

const ModelBatchMapper = require('./model-batch-mapper');

/**
 * Represents an object mapper for a specific model.
 */
class ModelMapper {
  constructor(name, handler) {
    this._name = name;
    this._handler = handler;
    /**
     * Contains utility methods to group multiple doc mutations on a single batch.
     * @type {ModelBatchMapper}
     */
    this.batching = new ModelBatchMapper(this._handler);
  }

  /**
   * @param {Object} doc
   * @param {{fields, orderBy, limit}} [docInfo]
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @return {Promise}
   */
  get(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getSelectExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions))
      .then(result => result.first());
  }

  /**
   * @param {Object} doc
   * @param {{fields, orderBy, limit}|null} [docInfo]
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Number} [executionOptions.fetchSize] Amount of rows to retrieve per page.
   * @param {Number} [executionOptions.pageState] Buffer or string token representing the paging state.
   * <p>When provided, the query will be executed starting from a given paging state.</p>
   * @return {Promise<Result>}
   */
  find(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getSelectExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}|null} [docInfo]
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   By default all generated INSERT statements are considered idempotent, except in the case of lightweight
   *   transactions. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>}
   */
  insert(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getInsertExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * @param {Object} doc
   * @param {{when, ifExists, ttl, fields}|null} [docInfo]
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   The mapper uses the generated queries to determine the default value. When an UPDATE is generated with a
   *   counter column or appending/prepending to a list column, the execution is marked as not idempotent.
   * </p>
   * <p>
   *   Additionally, the mapper uses the safest approach for queries with lightweight transactions (Compare and
   *   Set) by considering them as non-idempotent. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>}
   */
  update(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getUpdateExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Deletes a document.
   * @param {Object} doc A document containing the primary keys values of the document to delete.
   * @param {Object|null} [docInfo] An object containing the additional doc information.
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * When the CQL query is generated, this would be used to generate the `IF` clause.
   * @param {Boolean} [docInfo.ifExists]
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
   * @param {Boolean} [docInfo.deleteOnlyColumns] Determines that, when more document properties are specified
   * besides the primary keys, the generated DELETE statement should be used to delete some column values but leave
   * the row. When this is enabled and more properties are specified, a DELETE statement will have the following form:
   * "DELETE col1, col2 FROM table1 WHERE pk1 = ? AND pk2 = ?"
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   By default all generated DELETE statements are considered idempotent, except in the case of lightweight
   *   transactions. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>}
   */
  remove(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getDeleteExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Uses the provided query and param getter function to execute a query and map the results.
   * Gets a function that takes the document, executes the query and returns the mapped results.
   * @param {String} query
   * @param {Function} paramsHandler
   * @param {Object|String} [executionOptions] When provided, the options for all executions generated with this
   * method will use the provided options and it will not consider the executionOptions per call.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Number} [executionOptions.fetchSize] Amount of rows to retrieve per page.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times
   * without changing the result beyond the initial application.
   * @param {Number} [executionOptions.pageState] Buffer or string token representing the paging state.
   * <p>When provided, the query will be executed starting from a given paging state.</p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Function} Returns a function that takes the document and execution options as parameters and returns a
   * Promise.
   */
  mapWithQuery(query, paramsHandler, executionOptions) {
    return this._handler.getExecutorFromQuery(query, paramsHandler, executionOptions);
  }
}

module.exports = ModelMapper;