var async = require('async');

var RequestHandler = require('./request-handler.js');
var Host = require('./host.js').Host;
var writers = require('./writers.js');

var SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
var SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";
/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster
 * @param {Object} options
 * @constructor
 */
function ControlConnection(options) {
  this.protocolVersion = 2;
  this.hosts = [];
  this.options = options;
  var self = this;
  options.contactPoints.forEach(function (address) {
    var h = new Host(address, self.protocolVersion, options);
    self.hosts.push(h);
  });
}

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param callback
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
      c.sendStream(request, {}, function (err, results) {
        self.setLocalInfo(c.address, results);
        next(err, c);
      });
    },
    function getPeersInfo(c, next) {
      var request = new writers.QueryWriter(SELECT_PEERS, [], null, null, null);
      c.sendStream(request, {}, function (err, results) {
        self.setPeersInfo(results);
        next(err, c);
      });
    },
    function (c, next) {
      //TODO: Subscribe to events
      next(null);
    }
  ], callback);
};

ControlConnection.prototype.setLocalInfo = function (address, info) {
  //TODO
};

ControlConnection.prototype.setPeersInfo = function (info) {
  //TODO
};

module.exports = ControlConnection;