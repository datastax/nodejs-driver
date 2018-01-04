/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const helper = require('../test-helper');
const http = require('http');
const spawn = require('child_process').spawn;
const util = require('util');
const fs = require('fs');
const utils = require('../../lib/utils.js');
const Client = require('../../lib/client.js');

const simulacronHelper = {
  _execute: function(processName, params, cb) {
    const originalProcessName = processName;

    // If process hasn't completed in 10 seconds.
    let timeout = undefined;
    if(cb) {
      timeout = setTimeout(() => cb('Timed out while waiting for ' + processName + ' to complete.'), 10000);
    }

    const p = spawn(processName, params, {});
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

  /**
   * Starts simulacron process.  Uses $SIMULACRON_PATH environemtn variable to determine the
   * location of the simulacron jar.  If not set $HOME/simulacron.jar is tried instead.
   * 
   * Uses the starting ip range of 127.0.0.101.
   * 
   * @param {Function} cb Callback to be executed when completed, raised with Error if fails.
   */
  start: function(cb) {
    const self = this;
    let simulacronJarPath = process.env['SIMULACRON_PATH'];
    if (!simulacronJarPath) {
      simulacronJarPath = process.env['HOME'] + "/simulacron.jar";
      helper.trace("SIMULACRON_PATH not set, using " + simulacronJarPath);
    }
    if (!fs.existsSync(simulacronJarPath)) {
      throw new Error('Simulacron jar not found at: ' + simulacronJarPath);
    }

    const processName = 'java';
    const params = ['-jar', simulacronJarPath, '--ip', self.startingIp, '-p', this.defaultPort];
    let initialized = false;

    const timeout = setTimeout(() => cb(new Error('Timed out while waiting for Simulacron server to start.')), 10000);

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

  /**
   * Stops the simulacron process if running.
   *
   * @param {Function} cb Callback to be executed when completed, raised with Error if fails.
   */
  stop: function(cb) {
    if(this.sProcess !== undefined) {
      if(this.sProcess.exitCode) {
        helper.trace('Server already stopped with exit code %d.', this.sProcess.exitCode);
        cb();
      } else {
        if (helper.isWin()) {
          const params = ['Stop-Process', this.sProcess.pid];
          this._execute('powershell', params, cb);
        } else {
          this.sProcess.on('close', function () {
            cb();
          });
          this.sProcess.on('error', cb);
          this.sProcess.kill('SIGINT');
        }
      }
    } else {
      cb(Error('Process is not defined.'));
    }
  },

  /**
   * Convenience function for setting up simulacron, starting a simulated cluster and optionally creating a client instance
   * that connects to it in before hooks.  Cleans up after itself in after hooks.
   * 
   * @param {Array} dcs array of nodes-per-dc configuration (i.e. 2,2,2 creates 3 2 node dcs)
   * @param {Object} [options]
   * @param {Object} [options.initClient] Determines whether to create a Client instance.
   * @param {Object} [options.clientOptions] The options to use to initialize the client.
   */
  setup: function (dcs, options) {
    const self = this;
    options = options || utils.emptyObject;
    const clientOptions = options.clientOptions || {};
    const simulacronCluster = new SimulacronCluster();
    const initClient = options.initClient !== false;
    let client;
    before(function (done) {
      self.start(function () {
        simulacronCluster.register(dcs, clientOptions, function() {
          done();
        });
      });
    });
    if (initClient) {
      const baseOptions = { contactPoints: [self.startingIp] };
      client = new Client(utils.extend({}, options.clientOptions, baseOptions));
      before(client.connect.bind(client));
      after(client.shutdown.bind(client));
    }
    afterEach(simulacronCluster.clear.bind(simulacronCluster));
    after(simulacronHelper.stop.bind(simulacronHelper));

    return { cluster: simulacronCluster, client: client };
  },
  baseOptions: (function () {
    return {
      cassandraVersion: helper.getSimulatedCassandraVersion(),
      dseVersion: helper.getDseVersion(),
      clusterName: 'testCluster',
      activityLog: true,
      numTokens: 1
    };
  })(),
  baseAddress: 'localhost',
  startingIp: '127.0.0.101',
  defaultPort: 8188,
  SimulacronCluster: SimulacronCluster
};

function _makeRequest(options, callback) {
  const request = http.request(options, function(response) {
    // Continuously update stream with data
    let body = '';
    const statusCode = response.statusCode;
    response.on('data', function(d) {
      body += d;
    });
    response.on('end', function() {
      if (statusCode >= 400) {
        callback(body);
      } else if (body === '') {
        callback(null, {});
      } else {
        callback(null, JSON.parse(body));
      }
    });
  });
  request.on('error', function(err) {
    helper.trace(err.message);
    callback(err);
  });
  return request;
}

/**
 * Shared base class for cluster, data center and node types.
 */
function SimulacronTopic() {
  this.baseAddress = simulacronHelper.baseAddress;
  this.port = simulacronHelper.defaultPort;
}

/**
 * @returns {Array} query log entries for the given topic.
 */
SimulacronTopic.prototype.getLogs = function(callback) {
  const self = this;
  _makeRequest(this._getOptions('log', this.id, 'GET'), function(err, data) {
    if (err) {
      callback(err);
    } else {
      callback(err, self._filterLogs(data));
    }
  }).end();
};

/**
 * Clears all query logs for a given topic.
 */
SimulacronTopic.prototype.clearLogs = function(callback) {
  _makeRequest(this._getOptions('log', this.id, 'DELETE'), function(err, data) {
    callback(err, data);
  }).end();
};

/**
 * Primes the given query.  All nodes associated with this topic will use this prime.
 *
 * @param {Object} body Body for prime query, refer to simulacron documentation for more details.
 * @param {Function} callback
 */
SimulacronTopic.prototype.prime = function(body, callback) {
  const request = _makeRequest(this._getOptions('prime', this.id, 'POST'), function(err, data) {
    callback(err, data);
  });
  request.write(JSON.stringify(body));
  request.end();
};

/**
 * Convenience method that primes the given query string with a simple successful response. 
 * 
 * @param {String} query Query string for prime query.
 * @param {Function} callback
 */
SimulacronTopic.prototype.primeQuery = function(query, callback) {
  this.prime({
    when: {
      query: query
    },
    then: {
      result: "success"
    }
  }, callback);
};

/**
 * Clear all primes associated with this topic.  Also clears primes for underlying members.
 */
SimulacronTopic.prototype.clearPrimes = function(callback) {
  _makeRequest(this._getOptions('prime', this.id, 'DELETE'), function(err, data) {
    callback(err, data);
  }).end();
};

/**
 * Clears all primes and activity logs associated with this topic.  Also clears data for underlying members.
 */
SimulacronTopic.prototype.clear = function(callback) {
  const self = this;
  utils.parallel([
    self.clearPrimes.bind(self), 
    self.clearLogs.bind(self)
  ], callback);
};

/**
 * Stops listening for connections and closes existing connections for all associated nodes.
 */
SimulacronTopic.prototype.stop = function(callback) {
  const stopNodePath = '/listener/%s?type=stop';
  const options = {
    host: this.baseAddress,
    path: encodeURI(util.format(stopNodePath, this.id)),
    port: this.port,
    method: 'DELETE'
  };
  _makeRequest(options, function(err, data) {
    callback(err, data);
  }).end();
};

/**
 * Resume listening for connections for all associated nodes.
 */
SimulacronTopic.prototype.start = function(callback) {
  const resumeNodePath = '/listener/%s';
  const options = {
    host: this.baseAddress,
    path: encodeURI(util.format(resumeNodePath, this.id)),
    port: this.port,
    method: 'PUT'
  };
  _makeRequest(options, function(err, data) {
    callback(err, data);
  }).end();
};

SimulacronTopic.prototype._filterLogs = function(data) {
  return data;
};

SimulacronTopic.prototype._getPath = function (endpoint, id) {
  const path = '/' + endpoint + '/' + id;
  return encodeURI(path);
};

SimulacronTopic.prototype._getOptions = function (endpoint, id, method) {
  return {
    host: this.baseAddress,
    path: this._getPath(endpoint, id),
    port: this.port,
    method: method,
    headers: { 'Content-Type': 'application/json' }
  };
};

/**
 * Represents a cluster with its data center and node configurations.  Use start to initialize.
 */
function SimulacronCluster() {
  SimulacronTopic.call(this);
}

util.inherits(SimulacronCluster, SimulacronTopic);

/**
 * Registers and starts cluster with given dc configuration and options.
 * 
 * @param {Array} dcs array of nodes-per-dc configuration (i.e. 2,2,2 creates 3 2 node dcs)
 * @param {Object} options Startup options
 * @param {String} [options.cassandraVersion] Version of cassandra nodes should use. (default is helper.getCassandraVersion())
 * @param {String} [options.dseVersion] Version of dse nodes should use. (default is unset)
 * @param {Boolean} [options.clusterName] Name of the cluster. (default is 'testCluster')
 * @param {Number} [options.numTokens] Number of tokens for each node. (default is 1)
 * @param {Function} callback
 */
SimulacronCluster.prototype.register = function(dcs, options, callback) {
  const self = this;
  const createClusterPath = '/cluster?data_centers=%s&cassandra_version=%s&dse_version=%s&name=%s&activity_log=%s&num_tokens=%d';

  options = utils.extend({}, simulacronHelper.baseOptions, options);

  const urlPath = encodeURI(util.format(createClusterPath, dcs, options.cassandraVersion, options.dseVersion,
    options.clusterName, options.activityLog, options.numTokens));

  const requestOptions = {
    host: self.baseAddress,
    port: self.port,
    path: urlPath,
    method: 'POST'
  };

  _makeRequest(requestOptions, function(err, data) {
    if (err) {
      return callback(err);
    }
    self.name = data.name;
    self.id = data.id;
    self.data = data;
    self.dcs = data.data_centers.map(function(dc) {
      return new SimulacronDataCenter(self, dc);
    });
    callback(null, self);
  }).end();
};

/**
 * Registers and starts cluster with given body.
 *
 * @param {Object} Request payload body.
 * @param {Function} callback
 */
SimulacronCluster.prototype.registerWithBody = function(body, callback) {
  const self = this;
  const requestOptions = {
    host: self.baseAddress,
    port: self.port,
    path: encodeURI('/cluster'),
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }
  };

  const request = _makeRequest(requestOptions, function(err, data) {
    if (err) {
      return callback(err);
    }
    self.name = data.name;
    self.id = data.id;
    self.data = data;
    self.dcs = data.data_centers.map(function(dc) {
      return new SimulacronDataCenter(self, dc);
    });
    callback(null, self);
  });
  request.write(JSON.stringify(body));
  request.end();
};

/**
 * Unregisters and destroys this cluster instance from the server.
 */
SimulacronCluster.prototype.unregister = function(callback) {
  _makeRequest(this._getOptions('cluster', this.id, 'DELETE'), function(err, data) {
    callback(err, data);
  }).end();
};


/**
 * Finds a node in the cluster by its id or address.
 *
 * @param {Number|String} key Identifier of node.  If Number, assumes the id of the node in the data center.
 * If String, assumes an 'ip:port' designation and looks up node by address.
 * @param {Number} [datacenterIndex] Second identifier.  If present, 'id' is assumed to be the data center id.  If not
 * present, 'id' is assumed to be the node id or address.  If number is passed in for 'id', 0 is the assumed
 * data center id.
 * @returns {SimulacronNode} The node, if found.
 */
SimulacronCluster.prototype.node = function(key, datacenterIndex) {
  // if the first argument is a string, assume its an address.
  let dc;
  if (typeof key === "string") {
    // iterate over DCs and their nodes looking for first node that matches.
    for (let dcIndex = 0; dcIndex < this.dcs.length; dcIndex++) {
      dc = this.dcs[dcIndex];
      const node = dc.nodes.filter(function (n) {
        return n.address === key;
      })[0];
      if (node) {
        return node;
      }
    }
    // node not found, raise error.
    throw new Error("No node found for " + key);
  }
  if (typeof key !== 'number') {
    throw new Error('Node key must be either be a String or a Number');
  }
  // If the dc is not provided, assume first
  dc = this.dcs[datacenterIndex || 0];
  return dc.node(key);
};

/**
 * Finds a data center in the cluster by its id.
 * 
 * @param {Number} id Identifier of the dc.
 * @returns {SimulacronDataCenter} The data center, if found.
 */
SimulacronCluster.prototype.dc = function(id) {
  return this.dcs[id];
};

/**
 * @param {Number} [dataCenterId] The data center to return node addresses from, if not provided assumes 0.
 * @returns {Array} Listing of addresses of the nodes in the input dc.
 */
SimulacronCluster.prototype.getContactPoints = function(dataCenterId) {
  return this.dcs[dataCenterId || 0].nodes.map(function (node) {
    return node.address;
  });
};

/**
 * Represents a data center with its associated node configurations. 
 * @param {SimulacronCluster} cluster Parent cluster.
 * @param {Object} dc json data from provision response.
 */
function SimulacronDataCenter(cluster, dc) {
  SimulacronTopic.call(this);
  this.cluster = cluster;
  this.data = dc;
  this.localId = dc.id;
  this.id = cluster.id + '/' + dc.id;
  const self = this;
  this.nodes = dc.nodes.map(function(node) {
    return new SimulacronNode(self, node);
  });
}

util.inherits(SimulacronDataCenter, SimulacronTopic);

/**
 * Finds a node in the given data center by its id or address.
 * 
 * @param {Number|String} id Identifier of node.  If Number, assumes the id of the node in the data center.
 * If String, assumes an 'ip:port' designation and looks up node by address.
 * @returns {SimulacronNode} The node, if found.
 */
SimulacronDataCenter.prototype.node = function(id) {
  // if the first argument is a string, assume its an address.
  if (typeof id === "string") {
    for (let nodeIndex = 0; nodeIndex < this.nodes.length; nodeIndex++) {
      const n = this.nodes[nodeIndex];
      if (n.address === id) {
        return n;
      }
    }
    // node not found, raise error.
    throw new Error("No node found for " + id + " in dc " + this.localId);
  }
  if (typeof id !== 'number') {
    throw new Error('Node id must be a string or a number');
  }
  return this.nodes[id];
};

/**
 * Represents a node.
 * @param {SimulacronDataCenter} dc Parent Data Center.
 * @param {Object} node json data from provision response. 
 */
function SimulacronNode(dc, node) {
  SimulacronTopic.call(this);
  this.dc = dc;
  this.data = node;
  this.localId = node.id;
  this.id = dc.id + '/' + node.id;
  this.address = node.address;
}

util.inherits(SimulacronNode, SimulacronTopic);

SimulacronNode.prototype._filterLogs = function(data) {
  return data.data_centers[0].nodes[0].queries;
};

module.exports = simulacronHelper;
