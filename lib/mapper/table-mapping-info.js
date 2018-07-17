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

  getColumnName(propName) {
    const columnName = this.documentProperties.get(propName);
    return columnName !== undefined ? columnName : propName;
  }

  getPropertyName(columnName) {
    const propName = this.columns.get(columnName);
    return propName !== undefined ? propName : columnName;
  }

  newInstance() {
    //TODO: Use TableMappings factory
    return {};
  }
}

module.exports = TableMappingInfo;