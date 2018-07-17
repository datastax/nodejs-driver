'use strict';

const vm = require('vm');

class QueryGenerator {
  static getSelect(tableName, columnKeys, columnFields) {
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
    return query;
  }

  static selectParamsGetter(docKeys, docInfo) {
    let scriptText = '(function getParameters(doc, docInfo) {\n';
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
}

module.exports = QueryGenerator;