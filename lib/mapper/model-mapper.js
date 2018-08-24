'use strict';

const ModelBatchMapper = require('./model-batch-mapper');

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
   * @param {{fields, groupBy, orderBy, limit}} [docInfo]
   * @param {String|{executionProfile, executeAs}|null} [executionOptions]
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
   * @param {{fields, groupBy, orderBy, limit}|null} [docInfo]
   * @param {String|{executionProfile, executeAs, fetchSize, pageState}} [executionOptions]
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
   * @param {String|{executionProfile, executeAs, timestamp}} [executionOptions] An object containing the execution
   * options or the name of the execution profile.
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
   * @param {String|{executionProfile, executeAs, timestamp}} [executionOptions]
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
   * @param {String|{executionProfile, executeAs, timestamp}} [executionOptions] The execution options.
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
   * @param {String|{executionProfile, executeAs, fetchSize, pageState, timestamp}} [executionOptions] When
   * provided, the options for all executions generated with this method will use the provided options and it will
   * not consider the executionOptions per call.
   * @return {Function} Returns a function that takes the document and execution options as parameters and returns a
   * Promise.
   */
  mapWithQuery(query, paramsHandler, executionOptions) {
    return this._handler.getExecutorFromQuery(query, paramsHandler, executionOptions);
  }
}

module.exports = ModelMapper;