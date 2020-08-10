/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const util = require('util');
const types = require('../types');
const utils = require('../utils.js');
const errors = require('../errors.js');

const doneIteratorObject = Object.freeze({ done: true });
const newlyUpInterval = 60000;

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
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
 * second parameter.
 */
LoadBalancingPolicy.prototype.newQueryPlan = function (keyspace, executionOptions, callback) {
  callback(new Error('You must implement a query plan for the LoadBalancingPolicy class'));
};

/**
 * Gets an associative array containing the policy options.
 */
LoadBalancingPolicy.prototype.getOptions = function () {
  return new Map();
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
 * Returns an iterator with the hosts to be used as coordinator for a query.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
 * second parameter.
 */
RoundRobinPolicy.prototype.newQueryPlan = function (keyspace, executionOptions, callback) {
  if (!this.hosts) {
    return callback(new Error('Load balancing policy not initialized'));
  }
  const hosts = this.hosts.values();
  const self = this;
  let counter = 0;

  let planIndex = self.index % hosts.length;
  self.index += 1;
  if (self.index >= utils.maxInt) {
    self.index = 0;
  }

  callback(null, {
    next: function () {
      if (++counter > hosts.length) {
        return doneIteratorObject;
      }
      return {value: hosts[planIndex++ % hosts.length], done: false};
    }
  });
};

/**
 * A data-center aware Round-robin load balancing policy.
 * This policy provides round-robin queries over the nodes of the local
 * data center.
 * @param {?String} [localDc] local datacenter name.  This value overrides the 'localDataCenter' Client option \
 * and is useful for cases where you have multiple execution profiles that you intend on using for routing
 * requests to different data centers.
 * @extends {LoadBalancingPolicy}
 * @constructor
 */
function DCAwareRoundRobinPolicy(localDc) {
  this.localDc = localDc;
  this.index = 0;
  /** @type {Array} */
  this.localHostsArray = null;
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
  hosts.on('add', this._cleanHostCache.bind(this));
  hosts.on('remove', this._cleanHostCache.bind(this));

  try {
    setLocalDc(this, client, this.hosts);
  } catch (err) {
    return callback(err);
  }

  callback();
};

/**
 * Returns the distance depending on the datacenter.
 * @param {Host} host
 */
DCAwareRoundRobinPolicy.prototype.getDistance = function (host) {
  if (host.datacenter === this.localDc) {
    return types.distance.local;
  }

  return types.distance.ignored;
};

DCAwareRoundRobinPolicy.prototype._cleanHostCache = function () {
  this.localHostsArray = null;
};

DCAwareRoundRobinPolicy.prototype._resolveLocalHosts = function() {
  const hosts = this.hosts.values();
  if (this.localHostsArray) {
    //there were already calculated
    return;
  }
  this.localHostsArray = [];
  hosts.forEach(function (h) {
    if (!h.datacenter) {
      //not a remote dc node
      return;
    }
    if (h.datacenter === this.localDc) {
      this.localHostsArray.push(h);
    }
  }, this);
};

/**
 * It returns an iterator that yields local nodes.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
 * second parameter.
 */
DCAwareRoundRobinPolicy.prototype.newQueryPlan = function (keyspace, executionOptions, callback) {
  if (!this.hosts) {
    return callback(new Error('Load balancing policy not initialized'));
  }
  this.index += 1;
  if (this.index >= utils.maxInt) {
    this.index = 0;
  }
  this._resolveLocalHosts();
  // Use a local reference of hosts
  const localHostsArray = this.localHostsArray;
  let planLocalIndex = this.index;
  let counter = 0;
  callback(null, {
    next: function () {
      let host;
      if (counter++ < localHostsArray.length) {
        host = localHostsArray[planLocalIndex++ % localHostsArray.length];
        return { value: host, done: false };
      }
      return doneIteratorObject;
    }
  });
};

/**
 * Gets an associative array containing the policy options.
 */
DCAwareRoundRobinPolicy.prototype.getOptions = function () {
  return new Map([
    ['localDataCenter', this.localDc ]
  ]);
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
 * The returned plan will return local replicas first, if replicas can be determined, followed by the plan of the
 * child policy.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
 * second parameter.
 */
TokenAwarePolicy.prototype.newQueryPlan = function (keyspace, executionOptions, callback) {
  let routingKey;
  if (executionOptions) {
    routingKey = executionOptions.getRoutingKey();
    if (executionOptions.getKeyspace()) {
      keyspace = executionOptions.getKeyspace();
    }
  }
  let replicas;
  if (routingKey) {
    replicas = this.client.getReplicas(keyspace, routingKey);
  }
  if (!routingKey || !replicas) {
    return this.childPolicy.newQueryPlan(keyspace, executionOptions, callback);
  }
  const iterator = new TokenAwareIterator(keyspace, executionOptions, replicas, this.childPolicy);
  iterator.iterate(callback);
};

/**
 * An iterator that holds the context for the subsequent next() calls
 * @param {String} keyspace
 * @param {ExecutionOptions} execOptions
 * @param {Array} replicas
 * @param childPolicy
 * @constructor
 * @ignore
 */
function TokenAwareIterator(keyspace, execOptions, replicas, childPolicy) {
  this.keyspace = keyspace;
  this.childPolicy = childPolicy;
  this.options = execOptions;
  this.localReplicas = [];
  this.replicaIndex = 0;
  this.replicaMap = {};
  this.childIterator = null;
  // Memoize the local replicas
  // The amount of local replicas should be defined before start iterating, in order to select an
  // appropriate (pseudo random) startIndex
  for (let i = 0; i < replicas.length; i++) {
    const host = replicas[i];
    if (this.childPolicy.getDistance(host) !== types.distance.local) {
      continue;
    }
    this.replicaMap[host.address] = true;
    this.localReplicas.push(host);
  }
  // We use a PRNG to set the replica index
  // We only care about proportional fair scheduling between replicas of a given token
  // Math.random() has an extremely short permutation cycle length but we don't care about collisions
  this.startIndex = Math.floor(Math.random() * this.localReplicas.length);
}

TokenAwareIterator.prototype.iterate = function (callback) {
  //Load the child policy hosts
  const self = this;
  this.childPolicy.newQueryPlan(this.keyspace, this.options, function (err, iterator) {
    if (err) {
      return callback(err);
    }
    //get the iterator of the child policy in case is needed
    self.childIterator = iterator;
    callback(null, {
      next: function () { return self.computeNext(); }
    });
  });
};

TokenAwareIterator.prototype.computeNext = function () {
  let host;
  if (this.replicaIndex < this.localReplicas.length) {
    host = this.localReplicas[(this.startIndex + (this.replicaIndex++)) % this.localReplicas.length];
    return { value: host, done: false };
  }
  // Return hosts from child policy
  let item;
  while ((item = this.childIterator.next()) && !item.done) {
    if (this.replicaMap[item.value.address]) {
      // Avoid yielding local replicas from the child load balancing policy query plan
      continue;
    }
    return item;
  }
  return doneIteratorObject;
};

/**
 * Gets an associative array containing the policy options.
 */
TokenAwarePolicy.prototype.getOptions = function () {
  const map = new Map([
    ['childPolicy', this.childPolicy.constructor !== undefined ? this.childPolicy.constructor.name : null ]
  ]);

  if (this.childPolicy instanceof DCAwareRoundRobinPolicy) {
    map.set('localDataCenter', this.childPolicy.localDc);
  }

  return map;
};

/**
 * Create a new policy that wraps the provided child policy but only "allow" hosts
 * from the provided list.
 * @class
 * @classdesc
 * A load balancing policy wrapper that ensure that only hosts from a provided
 * allow list will ever be returned.
 * <p>
 * This policy wraps another load balancing policy and will delegate the choice
 * of hosts to the wrapped policy with the exception that only hosts contained
 * in the allow list provided when constructing this policy will ever be
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
 * @param {Array.<string>}  allowList The hosts address in the format ipAddress:port.
 * Only hosts from this list may get connected
 * to (whether they will get connected to or not depends on the child policy).
 * @extends LoadBalancingPolicy
 * @constructor
 */
function AllowListPolicy (childPolicy, allowList) {
  if (!childPolicy) {
    throw new Error("You must specify a child load balancing policy");
  }
  if (!Array.isArray(allowList)) {
    throw new Error("You must provide the list of allowed host addresses");
  }

  this.childPolicy = childPolicy;
  this.allowList = new Map(allowList.map(address => [ address, true ]));
}

util.inherits(AllowListPolicy, LoadBalancingPolicy);

AllowListPolicy.prototype.init = function (client, hosts, callback) {
  this.childPolicy.init(client, hosts, callback);
};

/**
 * Uses the child policy to return the distance to the host if included in the allow list.
 * Any host not in the while list will be considered ignored.
 * @param host
 */
AllowListPolicy.prototype.getDistance = function (host) {
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
AllowListPolicy.prototype._contains = function (host) {
  return !!this.allowList.get(host.address);
};

/**
 * Returns the hosts to use for a new query filtered by the allow list.
 */
AllowListPolicy.prototype.newQueryPlan = function (keyspace, info, callback) {
  const self = this;
  this.childPolicy.newQueryPlan(keyspace, info, function (err, iterator) {
    if (err) {
      return callback(err);
    }
    callback(null, self._filter(iterator));
  });
};

AllowListPolicy.prototype._filter = function (childIterator) {
  const self = this;
  return {
    next: function () {
      const item = childIterator.next();
      if (!item.done && !self._contains(item.value)) {
        return this.next();
      }
      return item;
    }
  };
};

/**
 * Gets an associative array containing the policy options.
 */
AllowListPolicy.prototype.getOptions = function () {
  return new Map([
    ['childPolicy', this.childPolicy.constructor !== undefined ? this.childPolicy.constructor.name : null ],
    ['allowList', Array.from(this.allowList.keys())]
  ]);
};

/**
 * Creates a new instance of the policy.
 * @classdesc
 * Exposed for backward-compatibility only, it's recommended that you use {@link AllowListPolicy} instead.
 * @param {LoadBalancingPolicy} childPolicy the wrapped policy.
 * @param {Array.<string>} allowList The hosts address in the format ipAddress:port.
 * Only hosts from this list may get connected to (whether they will get connected to or not depends on the child
 * policy).
 * @extends AllowListPolicy
 * @deprecated Use allow-list instead. It will be removed in future major versions.
 * @constructor
 */
function WhiteListPolicy(childPolicy, allowList) {
  AllowListPolicy.call(this, childPolicy, allowList);
}

util.inherits(WhiteListPolicy, AllowListPolicy);

/**
 * A load-balancing policy implementation that attempts to fairly distribute the load based on the amount of in-flight
 * request per hosts. The local replicas are initially shuffled and
 * <a href="https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf">between the first two nodes in the
 * shuffled list, the one with fewer in-flight requests is selected as coordinator</a>.
 *
 * <p>
 *   Additionally, it detects unresponsive replicas and reorders them at the back of the query plan.
 * </p>
 *
 * <p>
 *   For graph analytics queries, it uses the preferred analytics graph server previously obtained by driver as first
 *   host in the query plan.
 * </p>
 */
class DefaultLoadBalancingPolicy extends LoadBalancingPolicy {

  /**
   * Creates a new instance of <code>DefaultLoadBalancingPolicy</code>.
   * @param {String|Object} [options] The local data center name or the optional policy options object.
   * <p>
   *   Note that when providing the local data center name, it overrides <code>localDataCenter</code> option at
   *   <code>Client</code> level.
   * </p>
   * @param {String} [options.localDc] local data center name.  This value overrides the 'localDataCenter' Client option
   * and is useful for cases where you have multiple execution profiles that you intend on using for routing
   * requests to different data centers.
   * @param {Function} [options.filter] A function to apply to determine if hosts are included in the query plan.
   * The function takes a Host parameter and returns a Boolean.
   */
  constructor(options) {
    super();

    if (typeof options === 'string') {
      options = { localDc: options };
    } else if (!options) {
      options = utils.emptyObject;
    }

    this._client = null;
    this._hosts = null;
    this._filteredHosts = null;
    this._preferredHost = null;
    this._index = 0;
    this.localDc = options.localDc;
    this._filter = options.filter || this._defaultFilter;

    // Allow some checks to be injected
    if (options.isHostNewlyUp) {
      this._isHostNewlyUp = options.isHostNewlyUp;
    }
    if (options.healthCheck) {
      this._healthCheck = options.healthCheck;
    }
    if (options.compare) {
      this._compare = options.compare;
    }
    if (options.getReplicas) {
      this._getReplicas = options.getReplicas;
    }
  }

  /**
   * Initializes the load balancing policy, called after the driver obtained the information of the cluster.
   * @param {Client} client
   * @param {HostMap} hosts
   * @param {Function} callback
   */
  init(client, hosts, callback) {
    this._client = client;
    this._hosts = hosts;

    // Clean local host cache
    this._hosts.on('add', () => this._filteredHosts = null);
    this._hosts.on('remove', () => this._filteredHosts = null);

    try {
      setLocalDc(this, client, this._hosts);
    } catch (err) {
      return callback(err);
    }

    callback();
  }

  /**
   * Returns the distance assigned by this policy to the provided host, relatively to the client instance.
   * @param {Host} host
   */
  getDistance(host) {
    if (this._preferredHost !== null && host === this._preferredHost) {
      // Set the last preferred host as local.
      // It ensures that the pool for the graph analytics host has the appropriate size
      return types.distance.local;
    }

    if (!this._filter(host)) {
      return types.distance.ignored;
    }

    return host.datacenter === this.localDc ? types.distance.local : types.distance.ignored;
  }

  /**
   * Returns a host iterator to be used for a query execution.
   * @override
   * @param {String} keyspace
   * @param {ExecutionOptions} executionOptions
   * @param {Function} callback
   */
  newQueryPlan(keyspace, executionOptions, callback) {
    let routingKey;
    let preferredHost;

    if (executionOptions) {
      routingKey = executionOptions.getRoutingKey();

      if (executionOptions.getKeyspace()) {
        keyspace = executionOptions.getKeyspace();
      }

      preferredHost = executionOptions.getPreferredHost();
    }

    let iterable;

    if (!keyspace || !routingKey) {
      iterable = this._getLocalHosts();
    } else {
      iterable = this._getReplicasAndLocalHosts(keyspace, routingKey);
    }

    if (preferredHost) {
      // Set it on an instance level field to set the distance
      this._preferredHost = preferredHost;
      iterable = DefaultLoadBalancingPolicy._getPreferredHostFirst(preferredHost, iterable);
    }

    return callback(null, iterable);
  }

  /**
   * Yields the preferred host first, followed by the host in the provided iterable
   * @param preferredHost
   * @param iterable
   * @private
   */
  static *_getPreferredHostFirst(preferredHost, iterable) {
    yield preferredHost;

    for (const host of iterable) {
      if (host !== preferredHost) {
        yield host;
      }
    }
  }

  /**
   * Yields the local hosts without the replicas already yielded
   * @param {Array<Host>} [localReplicas] The local replicas that we should avoid to include again
   * @private
   */
  *_getLocalHosts(localReplicas) {
    // Use a local reference
    const hosts = this._getFilteredLocalHosts();
    const initialIndex = this._getIndex();

    // indexOf() over an Array is a O(n) operation but given that there should be 3 to 7 replicas,
    // it shouldn't be an expensive call. Additionally, this will only be executed when the local replicas
    // have been exhausted in a lazy manner.
    const canBeYield = localReplicas
      ? h => localReplicas.indexOf(h) === -1
      : h => true;

    for (let i = 0; i < hosts.length; i++) {
      const h = hosts[(i + initialIndex) % hosts.length];
      if (canBeYield(h) && h.isUp()) {
        yield h;
      }
    }
  }

  _getReplicasAndLocalHosts(keyspace, routingKey) {
    let replicas = this._getReplicas(keyspace, routingKey);
    if (replicas === null) {
      return this._getLocalHosts();
    }

    const filteredReplicas = [];
    let newlyUpReplica = null;
    let newlyUpReplicaTimestamp = Number.MIN_SAFE_INTEGER;
    let unhealthyReplicas = 0;

    // Filter by DC, predicate and UP replicas
    // Use the same iteration to perform other checks: whether if its newly UP or unhealthy
    // As this is part of the hot path, we use a simple loop and avoid using Array.prototype.filter() + closure
    for (let i = 0; i < replicas.length; i++) {
      const h = replicas[i];
      if (!this._filter(h) || h.datacenter !== this.localDc || !h.isUp()) {
        continue;
      }
      const isUpSince = this._isHostNewlyUp(h);
      if (isUpSince !== null && isUpSince > newlyUpReplicaTimestamp) {
        newlyUpReplica = h;
        newlyUpReplicaTimestamp = isUpSince;
      }
      if (newlyUpReplica === null && !this._healthCheck(h)) {
        unhealthyReplicas++;
      }
      filteredReplicas.push(h);
    }

    replicas = filteredReplicas;

    // Shuffle remaining local replicas
    utils.shuffleArray(replicas);

    if (replicas.length < 3) {
      // Avoid reordering replicas of a set of 2 as we could be doing more harm than good
      return this.yieldReplicasFirst(replicas);
    }

    let temp;

    if (newlyUpReplica === null) {
      if (unhealthyReplicas > 0 && unhealthyReplicas < Math.floor(replicas.length / 2 + 1)) {
        // There is one or more unhealthy replicas and there is a majority of healthy replicas
        this._sendUnhealthyToTheBack(replicas, unhealthyReplicas);
      }
    }
    else if ((newlyUpReplica === replicas[0] || newlyUpReplica === replicas[1]) && Math.random() * 4 >= 1) {
      // There is a newly UP replica and the replica in first or second position is the most recent replica
      // marked as UP and dice roll 1d4!=1 -> Send it to the back of the Array
      const index = newlyUpReplica === replicas[0] ? 0 : 1;
      temp = replicas[replicas.length - 1];
      replicas[replicas.length - 1] = replicas[index];
      replicas[index] = temp;
    }

    if (this._compare(replicas[1], replicas[0]) > 0) {
      // Power of two random choices
      temp = replicas[0];
      replicas[0] = replicas[1];
      replicas[1] = temp;
    }

    return this.yieldReplicasFirst(replicas);
  }

  /**
   * Yields the local replicas followed by the rest of local nodes.
   * @param {Array<Host>} replicas The local replicas
   */
  *yieldReplicasFirst(replicas) {
    for (let i = 0; i < replicas.length; i++) {
      yield replicas[i];
    }
    yield* this._getLocalHosts(replicas);
  }

  _isHostNewlyUp(h) {
    return (h.isUpSince !== null && Date.now() - h.isUpSince < newlyUpInterval) ? h.isUpSince : null;
  }

  /**
   * Returns a boolean determining whether the host health is ok or not.
   * A Host is considered unhealthy when there are enough items in the queue (10 items in-flight) but the
   * Host is not responding to those requests.
   * @param {Host} h
   * @return {boolean}
   * @private
   */
  _healthCheck(h) {
    return !(h.getInFlight() >= 10 && h.getResponseCount() <= 1);
  }

  /**
   * Compares to host and returns 1 if it needs to favor the first host otherwise, -1.
   * @return {number}
   * @private
   */
  _compare(h1, h2) {
    return h1.getInFlight() < h2.getInFlight() ? 1 : -1;
  }

  _getReplicas(keyspace, routingKey) {
    return this._client.getReplicas(keyspace, routingKey);
  }

  /**
   * Returns an Array of hosts filtered by DC and predicate.
   * @returns {Array<Host>}
   * @private
   */
  _getFilteredLocalHosts() {
    if (this._filteredHosts === null) {
      this._filteredHosts = this._hosts.values()
        .filter(h => this._filter(h) && h.datacenter === this.localDc);
    }
    return this._filteredHosts;
  }

  _getIndex() {
    const result = this._index++;
    // Overflow protection
    if (this._index === 0x7fffffff) {
      this._index = 0;
    }
    return result;
  }

  _sendUnhealthyToTheBack(replicas, unhealthyReplicas) {
    let counter = 0;

    // Start from the back, move backwards and stop once all unhealthy replicas are at the back
    for (let i = replicas.length - 1; i >= 0 && counter < unhealthyReplicas; i--) {
      const host = replicas[i];
      if (this._healthCheck(host)) {
        continue;
      }

      const targetIndex = replicas.length - 1 - counter;
      if (targetIndex !== i) {
        const temp = replicas[targetIndex];
        replicas[targetIndex] = host;
        replicas[i] = temp;
      }
      counter++;
    }
  }

  _defaultFilter() {
    return true;
  }

  /**
   * Gets an associative array containing the policy options.
   */
  getOptions() {
    return new Map([
      ['localDataCenter', this.localDc ],
      ['filterFunction', this._filter !== this._defaultFilter ]
    ]);
  }
}

/**
 * Validates and sets the local data center to be used.
 * @param {LoadBalancingPolicy} lbp
 * @param {Client} client
 * @param {HostMap} hosts
 * @private
 */
function setLocalDc(lbp, client, hosts) {
  if (!(lbp instanceof LoadBalancingPolicy)) {
    throw new errors.DriverInternalError('LoadBalancingPolicy instance was not provided');
  }

  if (client && client.options) {
    if (lbp.localDc && !client.options.localDataCenter) {
      client.log('info', `Local data center '${lbp.localDc}' was provided as an argument to the load-balancing` +
        ` policy. It is preferable to specify the local data center using 'localDataCenter' in Client` +
        ` options instead when your application is targeting a single data center.`);
    }

    // If localDc is unset, use value set in client options.
    lbp.localDc = lbp.localDc || client.options.localDataCenter;
  }

  const dcs = getDataCenters(hosts);

  if (!lbp.localDc) {
    throw new errors.ArgumentError(
      `'localDataCenter' is not defined in Client options and also was not specified in constructor.` +
      ` At least one is required. Available DCs are: [${Array.from(dcs)}]`);
  }

  if (!dcs.has(lbp.localDc)) {
    throw new errors.ArgumentError(`Datacenter ${lbp.localDc} was not found. Available DCs are: [${Array.from(dcs)}]`);
  }
}

function getDataCenters(hosts) {
  return new Set(hosts.values().map(h => h.datacenter));
}

module.exports = {
  AllowListPolicy,
  DCAwareRoundRobinPolicy,
  DefaultLoadBalancingPolicy,
  LoadBalancingPolicy,
  RoundRobinPolicy,
  TokenAwarePolicy,
  // Deprecated: for backward compatibility only.
  WhiteListPolicy
};