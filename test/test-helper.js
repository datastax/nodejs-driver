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
  },
  baseOptions: (function () {
    var loadBalancing = require('../lib/policies/load-balancing.js');
    var reconnection = require('../lib/policies/reconnection.js');
    var retry = require('../lib/policies/retry.js');
    return {
      policies: {
        loadBalancing: new loadBalancing.RoundRobinPolicy(),
        reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
        retry: new retry.RetryPolicy()
      },
      contactPoints: ['127.0.0.1']
    };
  })()
};

module.exports = helper;