'use strict';

/**
 * Represents a query or a set of queries used to perform a document mutation in a batch.
 */
class ModelBatchItem {
  constructor(queries, doc, docInfo) {
    /**
     * @type {Promise<Array<{query, paramsGetter}>>}
     */
    this._queries = queries;
    this._doc = doc;
    this._docInfo = docInfo;
  }

  /**
   * Pushes the queries and parameters represented by this instance to the provided array.
   * @internal
   * @ignore
   * @param {Array} arr
   * @return {Promise}
   */
  pushQueries(arr) {
    return this._queries.then(queries => {
      queries.forEach(q => {
        arr.push({ query: q.query, params: q.paramsGetter(this._doc, this._docInfo) });
      });
    });
  }
}

module.exports = ModelBatchItem;