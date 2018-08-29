'use strict';

const vm = require('vm');
const qModule = require('./q');
const QueryOperator = qModule.QueryOperator;
const QueryAssignment = qModule.QueryAssignment;
const types = require('../types');
const dataTypes = types.dataTypes;

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
   * Gets the INSERT query and function to obtain the parameters, given the doc.
   * @param {TableMetadata} table
   * @param {Array} propertiesInfo
   * @param {Object} docInfo
   * @param {Boolean|undefined} ifNotExists
   * @return {{query: String, paramsGetter: Function, isIdempotent: Boolean}}
   */
  static getInsert(table, propertiesInfo, docInfo, ifNotExists) {
    const ttl = docInfo && docInfo.ttl;

    // Not all columns are contained in the table
    const filteredPropertiesInfo = propertiesInfo
      .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);

    return ({
      query: QueryGenerator._getInsertQuery(table.name, filteredPropertiesInfo, ifNotExists, ttl),
      paramsGetter: QueryGenerator._insertParamsGetter(filteredPropertiesInfo, docInfo),
      isIdempotent: !ifNotExists
    });
  }

  /**
   * Gets the query for an insert statement.
   * @param {String} tableName
   * @param {Array} propertiesInfo
   * @param {Boolean} ifNotExists
   * @param {Number|undefined} ttl
   * @return {String}
   */
  static _getInsertQuery(tableName, propertiesInfo, ifNotExists, ttl) {
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

  static _insertParamsGetter(propertiesInfo, docInfo) {
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
   * Gets the UPDATE query and function to obtain the parameters, given the doc.
   * @param {TableMetadata} table
   * @param {Array} propertiesInfo
   * @param {Object} docInfo
   * @param {Array} when
   * @param {Boolean|undefined} ifExists
   * @return {{query: String, paramsGetter: Function, isIdempotent: Boolean}}
   */
  static getUpdate(table, propertiesInfo, docInfo, when, ifExists) {
    const ttl = docInfo && docInfo.ttl;
    const primaryKeys = new Set(table.partitionKeys.concat(table.clusteringKeys).map(c => c.name));
    let isIdempotent = true;

    // Not all columns are contained in the table
    const filteredPropertiesInfo = propertiesInfo.filter(pInfo => {
      const column = table.columnsByName[pInfo.columnName];
      if (column === undefined) {
        return false;
      }

      if (isIdempotent
        && (column.type.code === dataTypes.counter || column.type.code === dataTypes.list)
        && pInfo.value instanceof QueryAssignment) {
        // Its not idempotent when is a counter assignment or a list assignment (+=)
        isIdempotent = false;
      }

      return true;
    });

    return {
      query: QueryGenerator._getUpdateQuery(table.name, primaryKeys, filteredPropertiesInfo, when, ifExists, ttl),
      isIdempotent: isIdempotent && when.length === 0 && !ifExists,
      paramsGetter: QueryGenerator._updateParamsGetter(primaryKeys, filteredPropertiesInfo, when, ttl)
    };
  }

  /**
   * Gets the query for an UPDATE statement.
   * @param {String} tableName
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Object} when
   * @param {Boolean} ifExists
   * @param {Number|undefined} ttl
   */
  static _getUpdateQuery(tableName, primaryKeys, propertiesInfo, when, ifExists, ttl) {
    let query = 'UPDATE ';
    query += tableName;
    query += ' SET ';

    query += propertiesInfo
      .filter(p => !primaryKeys.has(p.columnName))
      .map(p => {
        if (p.value instanceof QueryAssignment) {
          if (p.value.inverted) {
            // e.g: prepend "col1 = ? + col1"
            return `${p.columnName} = ? ${p.value.sign} ${p.columnName}`;
          }
          // e.g: increment "col1 = col1 + ?"
          return `${p.columnName} = ${p.columnName} ${p.value.sign} ?`;
        }

        return p.columnName + ' = ?';
      })
      .join(', ');

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
   * @param {Number|undefined} ttl
   * @returns {Function}
   */
  static _updateParamsGetter(primaryKeys, propertiesInfo, when, ttl) {
    let scriptText = '(function getParametersUpdate(doc, docInfo) {\n';
    scriptText += '  return [';

    // Assignment clause
    scriptText += QueryGenerator._assignmentGetterExpression(propertiesInfo.filter(p => !primaryKeys.has(p.columnName)));
    scriptText += ', ';
    // Where clause
    scriptText += QueryGenerator._valueGetterExpression(propertiesInfo.filter(p => primaryKeys.has(p.columnName)));

    // Condition clause
    if (when.length > 0) {
      scriptText += ', ' + QueryGenerator._valueGetterExpression(when, 'docInfo.when');
    }

    if (typeof ttl === 'number') {
      scriptText += `, docInfo['ttl']`;
    }

    // Finish return statement
    scriptText += '];\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: vmFileName});
  }

  /**
   * Gets the DELETE query and function to obtain the parameters, given the doc.
   * @param {TableMetadata} table
   * @param {Array} propertiesInfo
   * @param {Object} docInfo
   * @param {Array} when
   * @param {Boolean|undefined} ifExists
   * @return {{query: String, paramsGetter: Function, isIdempotent}}
   */
  static getDelete(table, propertiesInfo, docInfo, when, ifExists) {
    const deleteOnlyColumns = docInfo && docInfo.deleteOnlyColumns;
    const primaryKeys = new Set(table.partitionKeys.concat(table.clusteringKeys).map(c => c.name));

    const filteredPropertiesInfo = propertiesInfo
      .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);


    return ({
      query: QueryGenerator._getDeleteQuery(
        table.name, primaryKeys, filteredPropertiesInfo, when, ifExists, deleteOnlyColumns),
      paramsGetter: QueryGenerator._deleteParamsGetter(primaryKeys, filteredPropertiesInfo, when),
      isIdempotent: when.length === 0 && !ifExists
    });
  }

  /**
   * Gets the query for an UPDATE statement.
   * @param {String} tableName
   * @param {Set} primaryKeys
   * @param {Array} propertiesInfo
   * @param {Array} when
   * @param {Boolean} ifExists
   * @param {Boolean} deleteOnlyColumns
   * @private
   * @return {String}
   */
  static _getDeleteQuery(tableName, primaryKeys, propertiesInfo, when, ifExists, deleteOnlyColumns) {
    let query = 'DELETE';

    if (deleteOnlyColumns) {
      const columnsToDelete = propertiesInfo.filter(p => !primaryKeys.has(p.columnName))
        .map(p => p.columnName)
        .join(', ');

      if (columnsToDelete !== '') {
        query += ' ' + columnsToDelete;
      }
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
  static _deleteParamsGetter(primaryKeys, propertiesInfo, when) {
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

  /**
   * Gets a string containing the doc properties to SET, considering QueryAssignment instances.
   * @param {Array} propertiesInfo
   * @param {String} [prefix='doc']
   * @return {string}
   * @private
   */
  static _assignmentGetterExpression(propertiesInfo, prefix) {
    prefix = prefix || 'doc';

    return propertiesInfo
      .map(p => `${prefix}['${p.propertyName}']${p.value instanceof QueryAssignment ? '.value' : ''}`)
      .join(', ');
  }

  static _getConditionWithOperators(propertiesInfo) {
    return propertiesInfo
      .map(p => `${p.columnName} ${p.value instanceof QueryOperator ? p.value.key : '='} ?`)
      .join(' AND ');
  }
}

module.exports = QueryGenerator;