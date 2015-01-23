var util = require('util');
var async = require('async');

var types = require('../types');
var utils = require('../utils.js');
var errors = require('../errors.js');

/** @module policies/loadBalancing */
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
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions options evaluated for this execution
 * @param {Function} callback
 */
LoadBalancingPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  callback(new Error('You must implement a query plan for the LoadBalancingPolicy class'));
};

/**
 * This policy yield nodes in a round-robin fashion.
 * @extends LoadBalancingPolicy
 * @constructor
 */
function RoundRobinPolicy() {
  this.index = 0;
}

util.inherits(RoundRobinPolicy, LoadBalancingPolicy);

/**
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions options evaluated for this execution
 * @param {Function} callback
 */
RoundRobinPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
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
 * @extends {LoadBalancingPolicy}
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
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions
 * @param {Function} callback
 */
DCAwareRoundRobinPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
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

/**
 * A wrapper load balancing policy that add token awareness to a child policy.
 * @param {LoadBalancingPolicy} childPolicy
 * @extends LoadBalancingPolicy
 * @constructor
 */
function TokenAwarePolicy (childPolicy) {
  if (!childPolicy) {
    throw new Error("You must specify a child load balancing policy");
  }
  this.childPolicy = childPolicy;
}

util.inherits(TokenAwarePolicy, LoadBalancingPolicy);

TokenAwarePolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  this.childPolicy.init(client, hosts, callback);
};

TokenAwarePolicy.prototype.getDistance = function (host) {
  return this.childPolicy.getDistance(host);
};

/**
 * Returns the hosts to use for a new query.
 * The returned plan will first return replicas (whose HostDistance
 * for the child policy is local) for the query if it can determine
 * them is not.
 * Following what it will return the plan of the child policy.
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions
 * @param {Function} callback
 * @returns {*}
 */
TokenAwarePolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var routingKey;
  if (queryOptions) {
    routingKey = queryOptions.routingKey;
  }
  /** @type {HostMap} */
  var replicas;
  if (routingKey) {
    replicas = this.client.getReplicas(keyspace, routingKey);
  }
  if (!routingKey || !replicas) {
    return this.childPolicy.newQueryPlan(keyspace, queryOptions, callback);
  }
  var iterator = new TokenAwareIterator(keyspace, queryOptions, replicas, this.childPolicy);
  iterator.iterate(callback);
};

/**
 * An iterator that holds the context for the subsequent next() calls
 * @param {String} keyspace
 * @param queryOptions
 * @param {Array} replicas
 * @param childPolicy
 * @constructor
 */
function TokenAwareIterator(keyspace, queryOptions, replicas, childPolicy) {
  this.keyspace = keyspace;
  this.replicas = replicas;
  this.childPolicy = childPolicy;
  this.queryOptions = queryOptions;
  this.childHosts = [];
  this.remoteReplicas = [];
  this.replicaMap = {};
  this.replicaIndex = 0;
  this.childIterator = null;
  this.remoteReplicasIterator = utils.arrayIterator(this.remoteReplicas);
}

TokenAwareIterator.prototype.iterate = function (callback) {
  //Load the child policy hosts
  var self = this;
  this.childPolicy.newQueryPlan(this.keyspace, this.queryOptions, function (err, iterator) {
    if (err) return callback(err);
    //get the iterator of the child policy in case is needed
    self.childIterator = iterator;
    callback(null, {next: self.computeNext.bind(self)});
  });
};

TokenAwareIterator.prototype.computeNext = function () {
  //return local replicas
  var host = this.nextLocalReplica();
  if (host) {
    return {value: host, done: !host};
  }
  //return child hosts
  var item = this.childIterator.next();
  if (!item.done) {
    if (this.replicaMap[item.value.address]) {
      return this.computeNext();
    }
    return item;
  }
  //return remote replicas
  item = this.remoteReplicasIterator.next();
  return item;
};

TokenAwareIterator.prototype.nextLocalReplica = function () {
  if (this.replicaIndex >= this.replicas.length) {
    return null;
  }
  var host = this.replicas[this.replicaIndex++];
  this.replicaMap[host.address] = true;
  if (this.childPolicy.getDistance(host) !== types.distance.local) {
    this.remoteReplicas.push(host);
    //get next
    return this.nextLocalReplica();
  }
  return host;
};

exports.DCAwareRoundRobinPolicy = DCAwareRoundRobinPolicy;
exports.LoadBalancingPolicy = LoadBalancingPolicy;
exports.RoundRobinPolicy = RoundRobinPolicy;
exports.TokenAwarePolicy = TokenAwarePolicy;