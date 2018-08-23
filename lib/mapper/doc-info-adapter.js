'use strict';

const errors = require('../errors');
const utils = require('../utils');

/**
 * Provides utility methods to adapt and map user provided docInfo and executionOptions to a predictable object format.
 */
class DocInfoAdapter {
  /**
   * Returns an Array where each item contains the property name, the column name and the property value (to obtain
   * the operator).
   * When docInfo.fields is specified, it uses that array to obtain the information.
   * @param {Array<String>} docKeys
   * @param {null|{fields}} docInfo
   * @param {Object} doc
   * @param {TableMappingInfo} mappingInfo
   * @returns {Array}
   */
  static getPropertiesInfo(docKeys, docInfo, doc, mappingInfo) {
    let propertyKeys = docKeys;
    if (docInfo && docInfo.fields && docInfo.fields.length > 0) {
      propertyKeys = docInfo.fields;
    }

    return propertyKeys.map(propertyName => ({
      propertyName, columnName: mappingInfo.getColumnName(propertyName), value: doc[propertyName]
    }));
  }

  /**
   * @param {{orderBy}} docInfo
   * @param {TableMappingInfo} mappingInfo
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
   */
  static adaptOptions(executionOptions) {
    const options = {
      prepare: true,
      executionProfile: null,
      executeAs: null,
      timestamp: null
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executeAs = executionOptions.executeAs;
      options.executionProfile = executionOptions.executionProfile;
      options.timestamp = executionOptions.timestamp;
    }
    return options;
  }

  /**
   * Returns the QueryOptions for a SELECT statement.
   * @param {Object|String|undefined} executionOptions
   */
  static adaptSelectOptions(executionOptions) {
    const options = {
      prepare: true,
      executionProfile: null,
      executeAs: null,
      fetchSize: null,
      pageState: null
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executeAs = executionOptions.executeAs;
      options.executionProfile = executionOptions.executionProfile;
      options.fetchSize = executionOptions.fetchSize;
      options.pageState = executionOptions.pageState;
    }
    return options;
  }

  /**
   * Returns the QueryOptions for a batch statement.
   * @param {Object|String|undefined} executionOptions
   */
  static adaptBatchOptions(executionOptions) {
    const options = {
      prepare: true,
      executionProfile: null,
      executeAs: null,
      timestamp: null,
      logged: null
    };

    if (typeof executionOptions === 'string') {
      options.executionProfile = executionOptions;
    }
    else if (executionOptions !== null && executionOptions !== undefined) {
      options.executeAs = executionOptions.executeAs;
      options.executionProfile = executionOptions.executionProfile;
      options.timestamp = executionOptions.timestamp;
      options.logged = executionOptions.logged !== false;
    }
    return options;
  }
}

module.exports = DocInfoAdapter;