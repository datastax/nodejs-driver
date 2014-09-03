var util = require('util');
var async = require('async');

var types = require('../types.js');
var utils = require('../utils.js');
var errors = require('../errors.js');

/**
 * Base class for Load Balancing Policies
 * @constructor
 */
function LoadBalancingPolicy() {

}

/**
 * Initializes the load balancing policy, called after the driver obtained the information of the cluster.
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 */
LoadBalancingPolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  callback();
};

//noinspection JSUnusedLocalSymbols
/**
 * Returns the distance assigned by this policy to the provided host.
 * @param {Host} host
 */
LoadBalancingPolicy.prototype.getDistance = function (host) {
  return types.distance.local;
};

/**
 * Returns an iterator with the hosts for a new query.
 * Each new query will call this method. The first host in the result will
 * then be used to perform the query.
 * @param {Object} queryOptions options evaluated for the execution
 * @param {Function} callback
 */
LoadBalancingPolicy.prototype.newQueryPlan = function (queryOptions, callback) {
  callback(new Error('You must implement a query plan for the LoadBalancingPolicy class'));
};

/**
 * This policy yield nodes in a round-robin fashion.
 * @constructor
 */
function RoundRobinPolicy() {
  this.index = 0;
}

util.inherits(RoundRobinPolicy, LoadBalancingPolicy);

RoundRobinPolicy.prototype.newQueryPlan = function (queryOptions, callback) {
  if (!this.hosts) {
    callback(new Error('Load balancing policy not initialized'));
  }
  //clone the hosts
  var hosts = this.hosts.slice(0);
  var self = this;
  var counter = 0;
  callback(null, {
    next: function () {
      if (++counter > hosts.length) {
        return {done: true};
      }
      self.index += 1;
      //overflow protection
      if (self.index >= utils.maxInt) {
        self.index = 0;
      }
      return {value: hosts[self.index % hosts.length], done: false};
    }
  });
};

/**
 * A data-center aware Round-robin load balancing policy.
 * This policy provides round-robin queries over the node of the local
 * data center. It also includes in the query plans returned a configurable
 * number of hosts in the remote data centers, but those are always tried
 * after the local nodes. In other words, this policy guarantees that no
 * host in a remote data center will be queried unless no host in the local
 * data center can be reached.
 * @param {?String} [localDc] local datacenter name.
 * @param {Number} [usedHostsPerRemoteDc] the number of host per remote datacenter that the policy will yield \
 * in a newQueryPlan after the local nodes.
 * @constructor
 */
function DCAwareRoundRobinPolicy(localDc, usedHostsPerRemoteDc) {
  this.localDc = localDc;
  this.usedHostsPerRemoteDc = usedHostsPerRemoteDc || 0;
  this.index = 0;
  this.remoteIndex = 0;
}

util.inherits(DCAwareRoundRobinPolicy, LoadBalancingPolicy);

/**
 * Initializes the load balancing policy.
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 */
DCAwareRoundRobinPolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  if (!this.localDc) {
    //get the first alive local, it should be local on top
    var hostsArray = hosts.slice(0);
    for (var i = 0; i < hostsArray.length; i++) {
      var h = hostsArray[i];
      if (h.datacenter) {
        this.localDc = h.datacenter;
        break;
      }
    }
    //this should never happen but it does not hurt
    if (!this.localDc) {
      return callback(new errors.DriverInternalError('Local datacenter could not be determined'));
    }
  }
  callback();
};

/**
 * Returns the distance depending on the datacenter.
 * @param {Host} host
 */
DCAwareRoundRobinPolicy.prototype.getDistance = function (host) {
  if (!host.datacenter) {
    return types.distance.ignored;
  }
  if (host.datacenter === this.localDc) {
    return types.distance.local;
  }
  return types.distance.remote;
};

/**
 * It returns an iterator that yields local nodes first and remotes nodes afterwards.
 * The amount of remote nodes returned will depend on the usedHostsPerRemoteDc
 * @param {?Object} queryOptions
 * @param {function} callback
 */
DCAwareRoundRobinPolicy.prototype.newQueryPlan = function (queryOptions, callback) {
  if (!this.hosts) {
    callback(new Error('Load balancing policy not initialized'));
  }
  //clone the hosts, possible optimization cache the copy (tada!)
  var hosts = this.hosts.slice(0);
  var remoteHostsByDc = {};
  var allRemoteHosts = null;

  var self = this;
  var counter = 0;
  var remoteCounter = 0;
  callback(null, {
    next: function () {
      var local = this.nextLocal();
      if (!local.done) {
        return local;
      }
      this.sliceRemoteHosts();
      if (++remoteCounter > allRemoteHosts.length) {
        return {done: true};
      }
      self.remoteIndex++;
      if (self.remoteIndex >= utils.maxInt) {
        self.remoteIndex = 0;
      }
      return {value: allRemoteHosts[self.remoteIndex % allRemoteHosts.length], done: false};
    },
    nextLocal: function () {
      if (++counter > hosts.length) {
        return {done: true};
      }
      self.index += 1;
      if (self.index >= utils.maxInt) {
        self.index = 0;
      }
      var host = hosts[self.index % hosts.length];
      if (self.getDistance(host) === types.distance.remote) {
        var dcHosts = remoteHostsByDc[host.datacenter];
        if (!dcHosts) {
          dcHosts = [];
          remoteHostsByDc[host.datacenter] = dcHosts;
        }
        dcHosts.push(host);
        return this.nextLocal();
      }
      return {value: host, done: !host};
    },
    sliceRemoteHosts: function () {
      if (allRemoteHosts) {
        return;
      }
      allRemoteHosts = [];
      if (self.usedHostsPerRemoteDc === 0) {
        return;
      }
      for (var dc in remoteHostsByDc) {
        if (!remoteHostsByDc.hasOwnProperty(dc)) {
          continue;
        }
        var dcHosts = remoteHostsByDc[dc].slice(0, self.usedHostsPerRemoteDc);
        allRemoteHosts.push.apply(allRemoteHosts, dcHosts);
      }
    }
  });
};

exports.DCAwareRoundRobinPolicy = DCAwareRoundRobinPolicy;
exports.LoadBalancingPolicy = LoadBalancingPolicy;
exports.RoundRobinPolicy = RoundRobinPolicy;