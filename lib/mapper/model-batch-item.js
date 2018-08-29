'use strict';

/**
 * Represents a query or a set of queries used to perform a document mutation in a batch.
 */
class ModelBatchItem {
  /**
   * @ignore
   * @param {Promise<Array<{query, paramsGetter}>>} queries
   * @param {Object} doc
   * @param {Object} docInfo
   * @param {ModelMappingInfo} mappingInfo
   */
  constructor(queries, doc, docInfo, mappingInfo) {
    this._queries = queries;
    this._doc = doc;
    this._docInfo = docInfo;
    this._mappingInfo = mappingInfo;
    this.isIdempotent = undefined;
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
      this.isIdempotent = true;
      queries.forEach(q => {
        this.isIdempotent = this.isIdempotent && q.isIdempotent;
        arr.push({ query: q.query, params: q.paramsGetter(this._doc, this._docInfo) });
      });
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