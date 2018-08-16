'use strict';

const vm = require('vm');

/**
 * Provides methods to generate a query and parameter handlers.
 */
class QueryGenerator {
  static getSelect(tableName, columnKeys, columnFields, orderByColumns) {
    let query = 'SELECT ';

    query += columnFields.length > 0 ? columnFields.join(', ') : '*';
    query += ' FROM ' + tableName;
    query += ' WHERE ';

    for (let i = 0; i < columnKeys.length; i++) {
      if (i > 0) {
        query += ' AND ';
      }
      query += columnKeys[i] + ' = ?';
    }

    if (orderByColumns.length > 0) {
      query += ' ORDER BY ';
      query += orderByColumns.map(order => order[0] + ' ' + order[1]).join(', ');
    }

    return query;
  }

  static selectParamsGetter(docKeys, docInfo) {
    let scriptText = '(function getParametersSelect(doc, docInfo) {\n';
    scriptText += '  return [';

    scriptText += docKeys.map(prop => `doc['${prop}']`).join(', ');

    if (docInfo && docInfo.limit !== undefined) {
      if (docKeys.length > 0) {
        scriptText += ', ';
      }
      scriptText += `docInfo['limit']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: 'gen-param-getter.js'});
  }

  /**
   *
   * Gets the query for an insert statement.
   * @param {String} tableName
   * @param {Array} filteredPropertiesInfo
   * @param {Boolean} ifNotExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static getInsert(tableName, filteredPropertiesInfo, ifNotExists, ttl) {
    let query = 'INSERT INTO ';
    query += tableName + ' (';
    query += filteredPropertiesInfo.map(pInfo => pInfo.columnName).join(', ');
    query += ') VALUES (';
    query += filteredPropertiesInfo.map(() => '?').join(', ');
    query += ')';

    if (ifNotExists === true) {
      query += ' IF NOT EXISTS';
    }

    if (typeof ttl === 'number') {
      query += ' USING TTL ?';
    }
    return query;
  }

  static insertParamsGetter(propertiesInfo, docInfo) {
    let scriptText = '(function getParametersInsert(doc, docInfo) {\n';
    scriptText += '  return [';

    scriptText += propertiesInfo.map(pInfo => `doc['${pInfo.propertyName}']`).join(', ');

    if (docInfo && typeof docInfo.ttl === 'number') {
      scriptText += `, docInfo['ttl']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: 'gen-param-getter.js'});
  }
}

module.exports = QueryGenerator;