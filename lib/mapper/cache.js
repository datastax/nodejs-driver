'use strict';

const QueryOperator = require('./q').QueryOperator;

/**
 * Provides utility methods for obtaining a caching keys based on the specifics of the Mapper methods.
 */
class Cache {
  /**
   * Gets an iterator of keys to uniquely identify a document shape.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{fields, limit, orderBy}} docInfo
   * @returns {Iterator}
   */
  static *getSelectKey(docKeys, doc, docInfo) {
    // Use the docKeys
    for (let i = 0; i < docKeys.length; i++) {
      const key = docKeys[i];
      yield key;
      const value = doc[key];
      if (value !== null && value !== undefined && value instanceof QueryOperator) {
        yield value.hashCode;
      }
    }

    if (docInfo) {
      // Use fields
      if (docInfo.fields && docInfo.fields.length > 0) {
        // act as a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }
      // Use limit
      if (typeof docInfo.limit === 'number') {
        yield '|l|';
      }
      // Use orderBy
      if (docInfo.orderBy && docInfo.orderBy.length > 0) {
        yield '|o|';
        yield* docInfo.orderBy;
      }
    }
  }
}

module.exports = Cache;