'use strict';

const errors = require('../errors');
const utils = require('../utils');

class DocInfoAdapter {
  /**
   * @param {{fields}} docInfo
   * @param {TableMappingInfo} mappingInfo
   * @returns {Array<String>}
   */
  static adaptFields(docInfo, mappingInfo){
    if (!docInfo || !docInfo.fields || docInfo.fields.length === 0) {
      return utils.emptyArray;
    }
    return docInfo.fields.map(x => mappingInfo.getColumnName(x));
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
      if (ordering !== 'ASC' || ordering !== 'DESC') {
        throw new errors.ArgumentError('Order must be either "ASC" or "DESC", obtained: ' + value);
      }
      return [ mappingInfo.getColumnName(key), ordering ];
    });
  }
}

module.exports = DocInfoAdapter;