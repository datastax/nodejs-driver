var async = require('async');

var RequestHandler = require('./request-handler.js');
var Host = require('./host.js').Host;

var SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
var SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";
/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster
 * @param {Object} options
 * @constructor
 */
function ControlConnection(options) {
  this.protocolVersion = null;
  //TODO: build from options.contactPoints;
  this.hosts = [new Host()];
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
  tasks.push(this.getHosts);
  async.series(tasks, callback);
};

ControlConnection.prototype.determineVersion = function (callback) {
  //TODO: Implement using OPTIONS message.
  this.protocolVersion = 2;
  callback();
};

ControlConnection.prototype.getHosts = function (callback) {
  var self = this;
  var handler = new RequestHandler();
  async.waterfall([
    function (next) {
      handler.getFirstConnection(self.hosts, next);
    },
    function (c, next) {
      //TODO: Query hosts
      next(null, c);
    },
    function (c, next) {
      //TODO: Subscribe to events
      next(null);
    }
  ], callback);
};