'use strict';

const QueryOperator = require('./q').QueryOperator;

/**
 * Provides utility methods for obtaining a caching keys based on the specifics of the Mapper methods.
 */
class Cache {
  /**
   * Gets an iterator of keys to uniquely identify a document shape for a select query.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{fields, limit, orderBy}} docInfo
   * @returns {Iterator}
   */
  static *getSelectKey(docKeys, doc, docInfo) {
    yield* Cache._yieldKeyAndOperators(docKeys, doc);

    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }

      if (typeof docInfo.limit === 'number') {
        yield '|l|';
      }

      if (docInfo.orderBy && docInfo.orderBy.length > 0) {
        yield '|o|';
        yield* docInfo.orderBy;
      }
    }
  }

  /**
   * Gets an iterator of keys to uniquely identify a document shape for an insert query.
   * @param {Array<String>} docKeys
   * @param {{ifNotExists, ttl, fields}} docInfo
   */
  static *getInsertKey(docKeys, docInfo) {
    // No operator supported on INSERT values
    yield* docKeys;

    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }

      if (typeof docInfo.ttl === 'number') {
        yield '|t|';
      }

      if (docInfo.ifNotExists) {
        yield '|e|';
      }
    }
  }

  /**
   * Gets an iterator of keys to uniquely identify a document shape for an UPDATE query.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{ifExists, when, ttl, fields}} docInfo
   */
  static *getUpdateKey(docKeys, doc, docInfo) {
    yield* Cache._yieldKeyAndOperators(docKeys, doc);

    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }

      if (typeof docInfo.ttl === 'number') {
        yield '|t|';
      }

      if (docInfo.ifExists) {
        yield '|e|';
      }

      if (docInfo.when) {
        yield* Cache._yieldKeyAndOperators(Object.keys(docInfo.when), docInfo.when);
      }
    }
  }

  /**
   * Gets an iterator of keys to uniquely identify a document shape for a DELETE query.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{ifExists, when, ttl, fields}} docInfo
   */
  static *getRemoveKey(docKeys, doc, docInfo) {
    yield* Cache._yieldKeyAndOperators(docKeys, doc);

    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }

      if (docInfo.ifExists) {
        yield '|e|';
      }
    }
  }

  static *_yieldKeyAndOperators(keys, obj) {
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      yield key;
      const value = obj[key];
      if (value !== null && value !== undefined && value instanceof QueryOperator) {
        yield value.key;
      }
    }
  }
}

module.exports = Cache;