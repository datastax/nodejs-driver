'use strict';
var helper = require('../test-helper');
var http = require('http');
var spawn = require('child_process').spawn;
var util = require('util');
var fs = require('fs');
var utils = require('../../lib/utils.js');

var simulacronHelper = {
  _execute: function(processName, params, cb) {
    var originalProcessName = processName;

    // If process hasn't completed in 10 seconds.
    var timeout = undefined;
    if(cb) {
      timeout = setTimeout(function() {
        cb('Timed out while waiting for ' + processName + ' to complete.');
      }, 10000);
    }

    var p = spawn(processName, params, {});
    p.stdout.setEncoding('utf8');
    p.stderr.setEncoding('utf8');
    p.stdout.on('data', function (data) {
      helper.trace('%s_out> %s', originalProcessName, data);
    });

    p.stderr.on('data', function (data) {
      helper.trace('%s_err> %s', originalProcessName, data);
    });

    p.on('close', function (code) {
      helper.trace('%s exited with code %d', originalProcessName, code);
      if(cb) {
        clearTimeout(timeout);
        if (code === 0) {
          cb();
        } else {
          cb(Error('Process exited with non-zero exit code: ' + code));
        }
      }
    });

    return p;
  },
  start: function(cb) {
    var self = this;
    var simulacronJarPath = process.env['SIMULACRON_PATH'];
    if (!simulacronJarPath) {
      simulacronJarPath = '$HOME/simulacron.jar';
      helper.trace('SIMULACRON_PATH not set, using $home/simulacron.jar');
    }
    if (!fs.existsSync(simulacronJarPath)) {
      throw new Error('Simulacron jar not found at: ' + simulacronJarPath);
    }

    var processName = 'java';
    var params = ['-jar', simulacronJarPath, '--ip', '127.0.0.101'];
    var initialized = false;

    var timeout = setTimeout(function() {
      cb(new Error('Timed out while waiting for Simulacron server to start.'));
    }, 10000);

    self.sProcess = self._execute(processName, params, function() {
      if(!initialized) {
        cb();
      }
    });
    self.sProcess.stdout.on('data', function (data) {
      // This is a bit of a kludge, check for a particular log statement which indicates
      // that all principals have been created before invoking the completion callback.
      if(data.indexOf('Started HTTP server interface') !== -1) {
        clearTimeout(timeout);
        helper.trace('Simulacron initialized!');
        initialized = true;
        cb();
      }
    });
  },
  stop: function(cb) {
    if(this.sProcess !== undefined) {
      if(this.sProcess.exitCode) {
        helper.trace('Server already stopped with exit code %d.', this.sProcess.exitCode);
        cb();
      } else {
        this.sProcess.on('close', function () {
          cb();
        });
        this.sProcess.on('error', cb);
        if (process.platform.indexOf('win') === 0) {
          var params = ['Stop-Process', this.sProcess.pid];
          this._execute('powershell', params, cb);
        } else {
          this.sProcess.kill('SIGINT');
        }

      }
    } else {
      cb(Error('Process is not defined.'));
    }
  },
  baseOptions: (function () {
    return {
      //required
      cassandraVersion: '3.10',
      dseVersion: '',
      clusterName: 'testCluster',
      activityLog: true,
      numTokens: 1
    };
  })(),
  baseAddress: 'localhost',
  defaultPort: 8187,
  SimulacronCluster: SimulacronCluster
};

function makeRequest(options, callback) {
  var request = http.request(options, function(response) {
    // Continuously update stream with data
    var body = '';
    response.on('data', function(d) {
      body += d;
    });
    response.on('end', function() {
      if (body === '') {
        callback(null);
      } else {
        callback(JSON.parse(body));
      }
    });
  });
  request.on('error', function(err) {
    helper.trace(err.message);
    throw new Error(err);
  });
  return request;
}

function SimulacronCluster() {
  this.baseAddress = simulacronHelper.baseAddress;
  this.port = simulacronHelper.defaultPort;
}

SimulacronCluster.prototype.start = function(dcs, clientOptions, callback) {
  var self = this;
  var createClusterPath = '/cluster?data_centers=%s&cassandra_version=%s&dse_version=%s&name=%s&activity_log=%s&num_tokens=%d';

  var options = utils.extend({}, simulacronHelper.baseOptions, clientOptions);

  var urlPath = encodeURI(util.format(createClusterPath, dcs, options.cassandraVersion, options.dseVersion,
    options.clusterName, options.activityLog, options.numTokens));

  var requestOptions = {
    host: self.baseAddress,
    port: self.port,
    path: urlPath,
    method: 'POST'
  };
  makeRequest(requestOptions, function(data) {
    self.name = data.name;
    self.id = data.id;
    self.dcs = data.data_centers;
    callback(null);
  }).end();
};

SimulacronCluster.prototype.destroy = function(callback) {
  var self = this;
  var destroyClusterPath = '/cluster/%d';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(destroyClusterPath, self.id)),
    port: self.port,
    method: 'DELETE'
  };
  makeRequest(options, function(data) {
    callback();
  }).end();
};

SimulacronCluster.prototype.clearLog = function(callback) {
  var self = this;
  var destroyClusterPath = '/log/%d';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(destroyClusterPath, self.id)),
    port: self.port,
    method: 'DELETE'
  };
  makeRequest(options, function(data) {
    callback(null);
  }).end();
};

SimulacronCluster.prototype.primeQueryWithEmptyResult = function(queryStr, callback) {
  var self = this;
  var primeQueryPath = '/prime/%d';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(primeQueryPath, self.id)),
    port: self.port,
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }
  };
  var body = {
    when: {
      query: queryStr
    },
    then: {
      result: 'success',
      delay_in_ms: 0,
      rows: [],
      column_types: {}
    }
  };
  var request = makeRequest(options, function(data) {
    callback();
  });
  request.write(JSON.stringify(body));
  request.end();
};

SimulacronCluster.prototype.findNode = function(nodeAddress) {
  var self = this;

  function findInDc(dc) {
    for (var nodeId = 0; nodeId < dc.nodes.length; nodeId++) {
      if (dc.nodes[nodeId].address === nodeAddress) {
        return {
          nodeId: dc.nodes[nodeId].id,
          dataCenterId: dc.id
        };
      }
    }
  }

  for(var dcIndex = 0; dcIndex < self.dcs.length; dcIndex++) {
    var nodeFound = findInDc(self.dcs[dcIndex]);
    if (nodeFound) {
      return nodeFound;
    }
  }
};

SimulacronCluster.prototype.getContactPoints = function(dataCenterId) {
  var dcId = dataCenterId = typeof dataCenterId === 'undefined' ? 0 : dataCenterId;
  return this.dcs[dcId].nodes.map(function (node) {
    return node.address;
  });
};

SimulacronCluster.prototype.queryNodeLog = function(nodeAddress, callback) {
  var self = this;
  var node = self.findNode(nodeAddress);
  if (node === null) {
    throw new Error('invalid node address: ' + nodeAddress);
  }

  var queryLog = '/log/%d/%d/%d';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(queryLog, self.id, node.dataCenterId, node.nodeId)),
    port: self.port,
    method: 'GET'
  };
  makeRequest(options, function(data) {
    callback(data.data_centers[0].nodes[0].queries);
  }).end();
};

SimulacronCluster.prototype.stopNode = function(nodeAddress, callback) {
  var self = this;
  var node = self.findNode(nodeAddress);
  if (node === null) {
    callback(new Error('invalid node address: ' + nodeAddress));
  }

  var stopNodePath = '/listener/%d/%d/%d?type=stop';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(stopNodePath, self.id, node.dataCenterId, node.nodeId)),
    port: self.port,
    method: 'DELETE'
  };
  makeRequest(options, function(data) {
    callback(data);
  }).end();
};

SimulacronCluster.prototype.resumeNode = function(nodeAddress, callback) {
  var self = this;
  var node = self.findNode(nodeAddress);
  if (node === null) {
    callback(new Error('invalid node address: ' + nodeAddress));
  }

  var stopNodePath = '/listener/%d/%d/%d';
  var options = {
    host: self.baseAddress,
    path: encodeURI(util.format(stopNodePath, self.id, node.dataCenterId, node.nodeId)),
    port: self.port,
    method: 'PUT'
  };
  makeRequest(options, function(data) {
    callback(data);
  }).end();
};

module.exports = simulacronHelper;