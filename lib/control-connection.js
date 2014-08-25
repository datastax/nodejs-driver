var async = require('async');

var RequestHandler = require('./request-handler.js');
var Host = require('./host.js').Host;
var HostMap = require('./host.js').HostMap;
var writers = require('./writers.js');
var utils = require('./utils.js');

var SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
var SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";
/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster
 * @param {Object} options
 * @constructor
 */
function ControlConnection(options) {
  this.protocolVersion = 2;
  this.hosts = new HostMap();
  this.options = options;
  var self = this;
  options.contactPoints.forEach(function (address) {
    var h = new Host(address, self.protocolVersion, options);
    self.hosts.push(address, h);
  });
}

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.init = function (callback) {
  var tasks = [];
  if (!this.protocolVersion) {
    tasks.push(this.determineVersion);
  }
  tasks.push(this.getHosts.bind(this));
  async.series(tasks, callback);
};

ControlConnection.prototype.determineVersion = function (callback) {
  //TODO: Implement using OPTIONS message.
  this.protocolVersion = 2;
  this.hosts.forEach(function (h) {
    h.protocolVersion = 2;
  });
  callback();
};

ControlConnection.prototype.getHosts = function (callback) {
  var handler = new RequestHandler(this.options);
  var self = this;
  async.waterfall([
    function (next) {
      handler.getFirstConnection(self.hosts, next);
    },
    function getLocalInfo(c, next) {
      var request = new writers.QueryWriter(SELECT_LOCAL, [], null, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setLocalInfo(c.address, result);
        next(err, c);
      });
    },
    function getPeersInfo(c, next) {
      var request = new writers.QueryWriter(SELECT_PEERS, [], null, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setPeersInfo(result);
        next(err, c);
      });
    },
    function subscribeEvents(c, next) {
      c.on('nodeTopologyChange', self.nodeTopologyChangeHandler.bind(self));
      c.on('nodeStatusChange', self.nodeStatusChangeHandler.bind(self));
      var request = new writers.RegisterWriter(['TOPOLOGY_CHANGE', 'STATUS_CHANGE']);
      c.sendStream(request, {}, next);
    }
  ], callback);
};

/**
 * Handles a TOPOLOGY_CHANGE event
 */
ControlConnection.prototype.nodeTopologyChangeHandler = function (event) {
  console.log('received topology change', event);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  var hostIp = utils.toIpString(event.inet.address);
  var host = this.hosts.get(hostIp);
  if (!host) return;
  if (event.up) {
    host.setUp();
  }
  else {
    host.setDown();
  }
};

ControlConnection.prototype.setLocalInfo = function (address, result) {
  if (!result || !result.rows || !result.rows.length) {
    return;
  }
  var row = result.rows[0];
  var localHost = this.hosts.get(address);
  localHost.datacenter = row.data_center;
  localHost.rack = row.rack;
};

ControlConnection.prototype.setPeersInfo = function (result) {
  if (!result || !result.rows || !result.rows.length) {
    return;
  }
  var self = this;
  result.rows.forEach(function (row) {
    var address = row.rpc_address || row.peer;
    address = utils.toIpString(address);
    var host = self.hosts.get(address);
    if (!host) {
      host = new Host(address, self.protocolVersion, self.options);
      self.hosts.push(address, host);
    }
    host.datacenter = row.data_center;
    host.rack = row.rack;
  });
};

module.exports = ControlConnection;