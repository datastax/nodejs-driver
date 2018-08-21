'use strict';

const vm = require('vm');
const QueryOperator = require('./q').QueryOperator;
const vmFileName = 'gen-param-getter.js';

/**
 * Provides methods to generate a query and parameter handlers.
 */
class QueryGenerator {
  static getSelect(tableName, propertiesInfo, fieldsInfo, orderByColumns) {
    let query = 'SELECT ';

    query += fieldsInfo.length > 0 ? fieldsInfo.map(p => p.columnName).join(', ') : '*';
    query += ' FROM ' + tableName;
    query += ' WHERE ';
    query += QueryGenerator._getConditionWithOperators(propertiesInfo);

    if (orderByColumns.length > 0) {
      query += ' ORDER BY ';
      query += orderByColumns.map(order => order[0] + ' ' + order[1]).join(', ');
    }

    return query;
  }

  static selectParamsGetter(propertiesInfo, docInfo) {
    let scriptText = '(function getParametersSelect(doc, docInfo) {\n';
    scriptText += '  return [';

    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo);

    if (docInfo && docInfo.limit !== undefined) {
      if (propertiesInfo.length > 0) {
        scriptText += ', ';
      }
      scriptText += `docInfo['limit']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  /**
   * Gets the query for an insert statement.
   * @param {String} tableName
   * @param {Array} propertiesInfo
   * @param {Boolean} ifNotExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static getInsert(tableName, propertiesInfo, ifNotExists, ttl) {
    let query = 'INSERT INTO ';
    query += tableName + ' (';
    query += propertiesInfo.map(pInfo => pInfo.columnName).join(', ');
    query += ') VALUES (';
    query += propertiesInfo.map(() => '?').join(', ');
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

    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo);

    if (docInfo && typeof docInfo.ttl === 'number') {
      scriptText += `, docInfo['ttl']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  /**
   * Gets the query for an UPDATE statement.
   * @param {String} tableName
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Object} when
   * @param {Boolean} ifExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static getUpdate(tableName, primaryKeys, propertiesInfo, when, ifExists, ttl) {
    let query = 'UPDATE ';
    query += tableName;
    query += ' SET ';
    query += propertiesInfo.filter(p => !primaryKeys.has(p.columnName)).map(p => p.columnName + ' = ?').join(', ');
    query += ' WHERE ';
    query += propertiesInfo.filter(p => primaryKeys.has(p.columnName)).map(p => p.columnName + ' = ?').join(' AND ');

    if (ifExists === true) {
      query += ' IF EXISTS';
    }
    else if (when.length > 0) {
      query += ' IF ' + QueryGenerator._getConditionWithOperators(when);
    }

    if (typeof ttl === 'number') {
      query += ' USING TTL ?';
    }
    return query;
  }

  /**
   * Returns a function to obtain the parameter values from a doc for an UPDATE statement.
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Array} when
   * @param {Object} docInfo
   * @returns {Function}
   */
  static updateParamsGetter(primaryKeys, propertiesInfo, when, docInfo) {
    let scriptText = '(function getParametersUpdate(doc, docInfo) {\n';
    scriptText += '  return [';

    // Assignment clause
    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo.filter(p => !primaryKeys.has(p.columnName)));
    scriptText += ', ';
    // Where clause
    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo.filter(p => primaryKeys.has(p.columnName)));

    // Condition clause
    if (when.length > 0) {
      scriptText += ', ' + QueryGenerator._valueGetterExpression(when, 'docInfo.when');
    }

    if (docInfo && typeof docInfo.ttl === 'number') {
      scriptText += `, docInfo['ttl']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  /**
   * Gets the query for an UPDATE statement.
   * @param {String} tableName
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Object} when
   * @param {Boolean} ifExists
   * @return {String}
   */
  static getDelete(tableName, primaryKeys, propertiesInfo, when, ifExists) {
    let query = 'DELETE';

    const columnsToDelete = propertiesInfo.filter(p => !primaryKeys.has(p.columnName))
      .map(p => p.columnName)
      .join(', ');

    if (columnsToDelete !== '') {
      query += ' ' + columnsToDelete;
    }

    query += ' FROM ';
    query += tableName;
    query += ' WHERE ';
    query += propertiesInfo.filter(p => primaryKeys.has(p.columnName)).map(p => p.columnName + ' = ?').join(' AND ');

    if (ifExists === true) {
      query += ' IF EXISTS';
    }
    else if (when.length > 0) {
      query += ' IF ' + QueryGenerator._getConditionWithOperators(when);
    }

    return query;
  }
  /**
   * Returns a function to obtain the parameter values from a doc for an UPDATE statement.
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Array} when
   * @returns {Function}
   */
  static deleteParamsGetter(primaryKeys, propertiesInfo, when) {
    let scriptText = '(function getParametersDelete(doc, docInfo) {\n';
    scriptText += '  return [';

    // Where clause
    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo.filter(p => primaryKeys.has(p.columnName)));

    // Condition clause
    if (when.length > 0) {
      scriptText += ', ' + QueryGenerator._valueGetterExpression(when, 'docInfo.when');
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  /**
   * Gets a string containing the doc properties to get.
   * @param {Array} propertiesInfo
   * @param {String} [prefix='doc']
   * @return {string}
   * @private
   */
  static _valueGetterExpression(propertiesInfo, prefix) {
    prefix = prefix || 'doc';

    return propertiesInfo
      .map(p => `${prefix}['${p.propertyName}']${p.value instanceof QueryOperator ? '.value' : ''}`)
      .join(', ');
  }

  static _getConditionWithOperators(propertiesInfo) {
    return propertiesInfo
      .map(p => `${p.columnName} ${p.value instanceof QueryOperator ? p.value.key : '='} ?`)
      .join(' AND ');
  }
}

module.exports = QueryGenerator;