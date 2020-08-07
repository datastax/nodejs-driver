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

const errors = require('../errors');
const utils = require('../utils');

/**
 * Provides utility methods to adapt and map user provided docInfo and executionOptions to a predictable object format.
 * @ignore
 */
class DocInfoAdapter {
  /**
   * Returns an Array where each item contains the property name, the column name and the property value (to obtain
   * the operator).
   * When docInfo.fields is specified, it uses that array to obtain the information.
   * @param {Array<String>} docKeys
   * @param {null|{fields}} docInfo
   * @param {Object} doc
   * @param {ModelMappingInfo} mappingInfo
   * @returns {Array}
   */
  static getPropertiesInfo(docKeys, docInfo, doc, mappingInfo) {
    let propertyKeys = docKeys;
    if (docInfo && docInfo.fields && docInfo.fields.length > 0) {
      propertyKeys = docInfo.fields;
    }

    return propertyKeys.map(propertyName => ({
      propertyName,
      columnName: mappingInfo.getColumnName(propertyName),
      value: doc[propertyName],
      fromModel: mappingInfo.getFromModelFn(propertyName)
    }));
  }

  /**
   * @param {{orderBy}} docInfo
   * @param {ModelMappingInfo} mappingInfo
   * @returns {Array<String>}
   */
  static adaptOrderBy(docInfo, mappingInfo){
    if (!docInfo || !docInfo.orderBy) {
      return utils.emptyArray;
    }
    return Object.keys(docInfo.orderBy).map(key => {
      const value = docInfo.orderBy[key];
      const ordering = typeof value === 'string' ? value.toUpperCase() : value;
      if (ordering !== 'ASC' && ordering !== 'DESC') {
        throw new errors.ArgumentError('Order must be either "ASC" or "DESC", obtained: ' + value);
      }
      return [ mappingInfo.getColumnName(key), ordering ];
    });
  }

  /**
   * Returns the QueryOptions for an INSERT/UPDATE/DELETE statement.
   * @param {Object|String|undefined} executionOptions
   * @param {Boolean} isIdempotent
   */
  static adaptOptions(executionOptions, isIdempotent) {
    const options = {
      prepare: true,
      executionProfile: undefined,
      timestamp: undefined,
      isIdempotent: isIdempotent
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executionProfile = executionOptions.executionProfile;
      options.timestamp = executionOptions.timestamp;

      if (executionOptions.isIdempotent !== undefined) {
        options.isIdempotent = executionOptions.isIdempotent;
      }
    }
    return options;
  }

  /**
   * Returns the QueryOptions for a SELECT statement.
   * @param {Object|String|undefined} executionOptions
   * @param {Boolean} [overrideIdempotency]
   */
  static adaptAllOptions(executionOptions, overrideIdempotency) {
    const options = {
      prepare: true,
      executionProfile: undefined,
      fetchSize: undefined,
      pageState: undefined,
      timestamp: undefined,
      isIdempotent: undefined
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executionProfile = executionOptions.executionProfile;
      options.fetchSize = executionOptions.fetchSize;
      options.pageState = executionOptions.pageState;
      options.timestamp = executionOptions.timestamp;
      options.isIdempotent = executionOptions.isIdempotent;
    }

    if (overrideIdempotency) {
      options.isIdempotent = true;
    }

    return options;
  }

  /**
   * Returns the QueryOptions for a batch statement.
   * @param {Object|String|undefined} executionOptions
   * @param {Boolean} isIdempotent
   * @param {Boolean} isCounter
   */
  static adaptBatchOptions(executionOptions, isIdempotent, isCounter) {
    const options = {
      prepare: true,
      executionProfile: undefined,
      timestamp: undefined,
      logged: undefined,
      isIdempotent: isIdempotent,
      counter: isCounter
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executionProfile = executionOptions.executionProfile;
      options.timestamp = executionOptions.timestamp;
      options.logged = executionOptions.logged !== false;

      if (executionOptions.isIdempotent !== undefined) {
        options.isIdempotent = executionOptions.isIdempotent;
      }
    }
    return options;
  }
}

module.exports = DocInfoAdapter;