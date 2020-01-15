/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const qModule = require('./q');
const QueryOperator = qModule.QueryOperator;
const QueryAssignment = qModule.QueryAssignment;

/**
 * Provides utility methods for obtaining a caching keys based on the specifics of the Mapper methods.
 * @ignore
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

    yield* Cache._getSelectDocInfo(docInfo);
  }
  /**
   * Gets an iterator of keys to uniquely identify a shape for a select all query.
   * @param {{fields, limit, orderBy}} docInfo
   * @returns {Iterator}
   */
  static *getSelectAllKey(docInfo) {
    yield 'root';

    yield* Cache._getSelectDocInfo(docInfo);
  }

  /**
   * Gets the parts of the key for a select query related to the docInfo.
   * @param {{fields, limit, orderBy}} docInfo
   * @private
   */
  static *_getSelectDocInfo(docInfo) {
    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use a separator from properties
        yield '|f|';
        yield* docInfo.fields;
      }

      if (typeof docInfo.limit === 'number') {
        yield '|l|';
      }

      if (docInfo.orderBy) {
        yield '|o|';

        // orderBy is uses property names as keys and 'asc'/'desc' as values
        const keys = Object.keys(docInfo.orderBy);
        for (let i = 0; i < keys.length; i++) {
          const key = keys[i];
          yield key;
          yield docInfo.orderBy[key];
        }
      }
    }
  }

  /**
   * Gets an iterator of keys to uniquely identify a document shape for an insert query.
   * @param {Array<String>} docKeys
   * @param {{ifNotExists, ttl, fields}} docInfo
   * @returns {Iterator}
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
    yield* Cache._yieldKeyAndAllQs(docKeys, doc);

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
   * @param {{ifExists, when, fields, deleteOnlyColumns}} docInfo
   * @returns {Iterator}
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

      if (docInfo.deleteOnlyColumns) {
        yield '|dc|';
      }

      if (docInfo.when) {
        yield* Cache._yieldKeyAndOperators(Object.keys(docInfo.when), docInfo.when);
      }
    }
  }

  static *_yieldKeyAndOperators(keys, obj) {
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      yield key;
      yield* Cache._yieldOperators(obj[key]);
    }
  }

  static *_yieldOperators(value) {
    if (value !== null && value !== undefined && value instanceof QueryOperator) {
      yield value.key;
      if (value.hasChildValues) {
        yield* Cache._yieldOperators(value.value[0]);
        yield '|/|';
        yield* Cache._yieldOperators(value.value[1]);
      }
    }
  }

  static *_yieldKeyAndAllQs(keys, obj) {
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      yield key;
      const value = obj[key];
      if (value !== null && value !== undefined) {
        if (value instanceof QueryOperator) {
          yield* Cache._yieldOperators(value);
        }
        else if (value instanceof QueryAssignment) {
          yield value.sign;
          yield value.inverted;
        }
      }
    }
  }
}

module.exports = Cache;