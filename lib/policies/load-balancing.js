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
  var hosts = this.hosts.values();
  var self = this;
  var counter = 0;

  var planIndex = self.index % hosts.length;
  self.index += 1;
  if (self.index >= utils.maxInt) {
    self.index = 0;
  }

  callback(null, {
    next: function () {
      if (++counter > hosts.length) {
        return {done: true};
      }
      return {value: hosts[planIndex++ % hosts.length], done: false};
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
    var hostsArray = hosts.values();
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
  var hosts = this.hosts.values();
  var remoteHostsByDc = {};
  var allRemoteHosts = null;

  var self = this;
  var counter = 0;
  var remoteCounter = 0;

  var planIndex = self.index % hosts.length;
  self.index += 1;
  if (self.index >= utils.maxInt) {
    self.index = 0;
  }
  var planRemoteIndex = undefined;

  callback(null, {
    next: function () {
      var local = this.nextLocal();
      if (!local.done) {
        return local;
      }
      this.sliceRemoteHosts();
      if(!planRemoteIndex) {
        planRemoteIndex = self.remoteIndex % allRemoteHosts.length;
        self.remoteIndex += 1;
        if (self.remoteIndex >= utils.maxInt) {
          self.remoteIndex = 0;
        }
      }
      if (++remoteCounter > allRemoteHosts.length) {
        return {done: true};
      }
      return {value: allRemoteHosts[planRemoteIndex++ % allRemoteHosts.length], done: false};
    },
    nextLocal: function () {
      if (++counter > hosts.length) {
        return {done: true};
      }
      var host = hosts[planIndex++ % hosts.length];
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
 */
TokenAwarePolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var routingKey;
  if (queryOptions) {
    routingKey = queryOptions.routingKey;
  }
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
 * @ignore
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


/**
 * Create a new policy that wraps the provided child policy but only "allow" hosts
 * from the provided while list.
 * @class
 * @classdesc
 * A load balancing policy wrapper that ensure that only hosts from a provided
 * white list will ever be returned.
 * <p>
 * This policy wraps another load balancing policy and will delegate the choice
 * of hosts to the wrapped policy with the exception that only hosts contained
 * in the white list provided when constructing this policy will ever be
 * returned. Any host not in the while list will be considered ignored
 * and thus will not be connected to.
 * <p>
 * This policy can be useful to ensure that the driver only connects to a
 * predefined set of hosts. Keep in mind however that this policy defeats
 * somewhat the host auto-detection of the driver. As such, this policy is only
 * useful in a few special cases or for testing, but is not optimal in general.
 * If all you want to do is limiting connections to hosts of the local
 * data-center then you should use DCAwareRoundRobinPolicy and *not* this policy
 * in particular.
 * @param {LoadBalancingPolicy} childPolicy the wrapped policy.
 * @param {Array.<string>}  whiteList the white listed hosts address in the format ipAddress:port.
 * Only hosts from this list may get connected
 * to (whether they will get connected to or not depends on the child policy).
 * @extends LoadBalancingPolicy
 * @constructor
 */
function WhiteListPolicy (childPolicy, whiteList) {
  if (!childPolicy) {
    throw new Error("You must specify a child load balancing policy");
  }
  if (!util.isArray(whiteList)) {
    throw new Error("You must provide the white list of host addresses");
  }
  this.childPolicy = childPolicy;
  var map = {};
  whiteList.forEach(function (address) {
    map[address] = true;
  });
  this.whiteList = map;
}

util.inherits(WhiteListPolicy, LoadBalancingPolicy);

WhiteListPolicy.prototype.init = function (client, hosts, callback) {
  this.childPolicy.init(client, hosts, callback);
};

/**
 * Uses the child policy to return the distance to the host if included in the white list.
 * Any host not in the while list will be considered ignored.
 * @param host
 */
WhiteListPolicy.prototype.getDistance = function (host) {
  if (!this._contains(host)) {
    return types.distance.ignored;
  }
  return this.childPolicy.getDistance(host);
};

/**
 * @param {Host} host
 * @returns {boolean}
 * @private
 */
WhiteListPolicy.prototype._contains = function (host) {
  return !!this.whiteList[host.address];
};

/**
 * Returns the hosts to use for a new query filtered by the white list.
 */
WhiteListPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var self = this;
  this.childPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    if (err) return callback(err);
    callback(null, self._filter(iterator));
  });
};

WhiteListPolicy.prototype._filter = function (childIterator) {
  var self = this;
  return {
    next: function () {
      var item = childIterator.next();
      if (!item.done && !self._contains(item.value)) {
        return this.next();
      }
      return item;
    }
  };
};


exports.DCAwareRoundRobinPolicy = DCAwareRoundRobinPolicy;
exports.LoadBalancingPolicy = LoadBalancingPolicy;
exports.RoundRobinPolicy = RoundRobinPolicy;
exports.TokenAwarePolicy = TokenAwarePolicy;
exports.WhiteListPolicy = WhiteListPolicy;