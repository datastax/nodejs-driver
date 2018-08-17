'use strict';

const errors = require('../errors');
const utils = require('../utils');

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
}

module.exports = DocInfoAdapter;