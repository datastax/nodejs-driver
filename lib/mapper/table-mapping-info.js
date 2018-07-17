'use strict';

class TableMappingInfo {
  /**
   * @param {String} keyspace
   * @param {Array<{name, isView}>} tables
   * @param {TableMappings} mappings
   * @param {Map<String,String>} columns
   */
  constructor(keyspace, tables, mappings, columns) {
    this.keyspace = keyspace;
    this.tables = tables;
    this.mappings = mappings;
    this.columns = columns;
    this.documentProperties = new Map();
    columns.forEach((propName, columnName) => this.documentProperties.set(propName, columnName));
  }

  getColumnName(docKey) {
    const columnName = this.documentProperties.get(docKey);
    return columnName !== undefined ? columnName : docKey;
  }
}

module.exports = TableMappingInfo;