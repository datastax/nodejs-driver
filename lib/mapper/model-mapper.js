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
   * @param {Object} doc A document containing the primary keys values of the document to delete.
   * @param {Object|null} [docInfo] An object containing the additional doc information.
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * When the CQL query is generated, this would be used to generate the `IF` clause.
   * @param {Boolean} [docInfo.ifExists]
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
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
   * @param {String} query
   * @param {Function} paramsHandler
   * @return {Function}
   */
  mapWithQuery(query, paramsHandler) {
    // The executor is part of the closure
    const executor = this._handler.getExecutorFromQuery(query, paramsHandler);

    return (function executeWithQuery(doc, executionOptions) {
      return executor(doc, executionOptions);
    });
  }
}

module.exports = ModelMapper;