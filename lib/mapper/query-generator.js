'use strict';

const vm = require('vm');
const QueryOperator = require('./q').QueryOperator;
const vmFileName = 'gen-param-getter.js';

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
      //TODO: conditions with operators
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

    //TODO: Operators / unify with the rest of the methods
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
   * @param {TableMetadata} table
   * @param {Array} propertiesInfo
   * @param {Object} when
   * @param {Boolean} ifExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static getUpdate(table, propertiesInfo, when, ifExists, ttl) {
    const whereClause = [];
    const primaryKeys = new Map();

    const primaryKeyHandler = c => {
      primaryKeys.set(c.name, true);
      whereClause.push(c.name + ' = ?');
    };

    // Build the WHERE clause
    table.partitionKeys.forEach(primaryKeyHandler);
    table.clusteringKeys.forEach(primaryKeyHandler);

    let query = 'UPDATE ';
    query += table.name + ' SET ';
    // SET should include all properties except the ones that are primary keys
    propertiesInfo.forEach(p => {
      if (primaryKeys.get(p.columnName) === true) {
        if (p.value instanceof QueryOperator) {
          throw new Error('Primary keys can not be specified using operators on an UPDATE statement');
        }

        // Should not be included in the SET clause
        return;
      }

      query += p.columnName += ' = ?';
    });

    query += ' WHERE ';
    query += whereClause.join(' AND ');

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

  static updateParamsGetter(propertiesInfo, when, docInfo) {
    let scriptText = '(function getParametersUpdate(doc, docInfo) {\n';
    scriptText += '  return [';

    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo);

    if (when.length > 0) {
      scriptText += ', ' + QueryGenerator._valueGetterExpression(when);
    }

    if (docInfo && typeof docInfo.ttl === 'number') {
      scriptText += `, docInfo['ttl']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  static _valueGetterExpression(propertiesInfo) {
    return propertiesInfo
      .map(p => `doc['${p.propertyName}']${p.value instanceof QueryOperator ? '.value' : ''}`)
      .join(', ');
  }

  static _getConditionWithOperators(propertiesInfo) {
    return propertiesInfo
      .map(p => `${p.columnName} ${p.value instanceof QueryOperator ? p.value.key : '='} ?`)
      .join(' AND ');
  }
}

module.exports = QueryGenerator;