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
    //TODO: Order by
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
   * @param {Array} columns
   * @param {Boolean} ifNotExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static getInsert(tableName, columns, ifNotExists, ttl) {
    let query = 'INSERT INTO ';
    query += tableName + ' (';
    query += columns.join(', ');
    query += ') VALUES (';
    query += columns.map(() => '?').join(', ');
    query += ')';

    if (ifNotExists === true) {
      query += ' IF NOT EXISTS';
    }

    if (typeof ttl === 'number') {
      query += ' USING TTL ?';
    }
    return query;
  }

  static insertParamsGetter(docKeys, docInfo) {
    let scriptText = '(function getParametersInsert(doc, docInfo) {\n';
    scriptText += '  return [';

    let keys = docKeys;
    let ttlParam = '';

    if (docInfo) {
      if (docInfo.fields && docInfo.fields.length > 0) {
        // Use only
        keys = docInfo.fields;
      }

      if (typeof docInfo.ttl === 'number') {
        ttlParam = `, docInfo['ttl']`;
      }
    }

    scriptText += keys.map(prop => `doc['${prop}']`).join(', ');
    scriptText += ttlParam;
    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: 'gen-param-getter.js'});
  }
}

module.exports = QueryGenerator;