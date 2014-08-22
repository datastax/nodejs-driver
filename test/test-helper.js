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
  })(),
  Ccm: Ccm,
  ccmHelper: {
    start: function (nodeLength) {
      return (function (done) {
        new Ccm().startAll(nodeLength, function (err) {
          if (err) return done(err);
          setTimeout(done, 10000);
        });
      });
    },
    remove: function (done) {
      new Ccm().remove(done);
    }
  }
};

function Ccm() {
  //Use an instance to maintain state
}

Ccm.prototype.startAll = function (nodeLength, callback) {
  var self = this;
  async.series([
    function (next) {
      //it wont hurt to remove
      self.exec(['remove'], function () {
        //ignore error
        next();
      });
    },
    function (next) {
      self.exec(['create', 'test', '-v', '2.0.8'], next);
    },
    function (next) {
      self.exec(['populate', '-n', nodeLength.toString()], next);
    },
    function (next) {
      self.exec(['start'], next);
    }
  ], function (err) {
    callback(err);
  });
};

Ccm.prototype.exec = function (params, callback) {
  var spawn = require('child_process').spawn;
  var process = spawn('ccm', params);
  var stdoutArray= [];
  var stderrArray= [];
  process.stdout.setEncoding('utf8');
  process.stderr.setEncoding('utf8');
  process.stdout.on('data', function (data) {
    stdoutArray.push(data);
  });

  process.stderr.on('data', function (data) {
    stderrArray.push(data);
  });

  process.on('close', function (code) {
    var info = {code: code, stdout: stdoutArray, stderr: stderrArray};
    var err = null;
    if (code !== 0) {
      err = new Error(
          'Error executing ccm\n' +
          info.stderr.join('\n') +
          info.stdout.join('\n')
      );
      err.info = info;
    }
    callback(err, info);
  });
};

Ccm.prototype.remove = function (callback) {
  this.exec(['remove'], callback);
};


module.exports = helper;