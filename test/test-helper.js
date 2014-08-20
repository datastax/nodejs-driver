var async = require('async');
var types = require('../lib/types.js');

var helper = {
  /**
   * Execute the query per each parameter array into paramsArray
   * @param {Connection|Client} con
   * @param {String} query
   * @param {Array} paramsArray Array of arrays of params
   * @param {Function} callback
   */
  batchInsert: function (con, query, paramsArray, callback) {
    async.mapSeries(paramsArray, function (params, next) {
      con.execute(query, params, types.consistencies.one, next);
    }, callback);
  },
  throwop: function (err) {
    if (err) throw err;
  }
};

module.exports = helper;