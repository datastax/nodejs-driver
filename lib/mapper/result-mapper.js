'use strict';

const vm = require('vm');
const utils = require('../utils');

class ResultMapper {
  /**
   * @param {TableMappingInfo} info
   * @param {ResultSet} rs
   * @returns {Function}
   */
  static getAdapter(info, rs) {
    if (rs.columns === null) {
      // VOID result
      return utils.noop;
    }

    const columns = rs.columns;
    let scriptText = '(function rowAdapter(row, info) {\n' +
      '  const item = info.newInstance();\n';

    scriptText += columns.map(c => `  item['${info.getPropertyName(c.name)}'] = row['${c.name}'];`).join('\n');

    scriptText += '\n  return item;\n})';

    const script = new vm.Script(scriptText);
    return script.runInThisContext({ filename: 'gen-result-mapper.js'});
  }
}

module.exports = ResultMapper;