'use strict';

class ModelMapper {
  constructor(name, handler) {
    this._name = name;
    this._handler = handler;
  }

  /**
   * @param {Object} doc
   * @param {{fields, groupBy, orderBy, limit}} docInfo
   * @param {String|{executionProfile, executeAs, fetchSize, pageState}} executionOptions
   * @return {Promise}
   */
  get(doc, docInfo, executionOptions) {
    return this._handler.getSelectExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions))
      .then(result => result.first());
  }

  /**
   * @param {Object} doc
   * @param {{fields, groupBy, orderBy, limit}} docInfo
   * @param {String|{executionProfile, executeAs, fetchSize, pageState}}executionOptions
   * @return {Promise<Result>}
   */
  find(doc, docInfo, executionOptions) {
    return this._handler.getSelectExecutor(doc, docInfo)
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