var events = require('events');
var t = require('./tokenizer.js');
var utils = require('./utils.js');

/**
 * Represents cluster information.
 * The metadata class acts as a internal state of the driver. It is disconnected, the methods don't retrieve any information.
 * The Client, RequestHandler and ControlConnection are responsible for filling in the information.
 * @param {ClientOptions} options
 * @constructor
 */
function Metadata (options) {
  this.keyspaces = {};
  this.clearPrepared();
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
}

Metadata.prototype.setPartitioner = function (partitionerName) {
  if (/RandomPartitioner$/.test(partitionerName)) {
    return this.tokenizer = new t.RandomTokenizer();
  }
  if (/ByteOrderedPartitioner$/.test(partitionerName)) {
    return this.tokenizer = new t.ByteOrderedTokenizer();
  }
  return this.tokenizer = new t.Murmur3Tokenizer();
};

/**
 * @param {HostMap} hosts
 */
Metadata.prototype.buildTokens = function (hosts) {
  //Get a sorted array of tokens
  var allSorted = [];
  //Get a map of <token, primaryHost>
  var primaryReplicas = {};
  //Depending on the amount of tokens, this could be an expensive operation
  var hostArray = hosts.slice(0);
  var parser = this.tokenizer.parse;
  var compare = this.tokenizer.compare;
  var stringify = this.tokenizer.stringify;
  var datacenters = {};
  hostArray.forEach(function (h) {
    if (!h.tokens) {
      return;
    }
    h.tokens.forEach(function (tokenString) {
      var token = parser(tokenString);
      utils.insertSorted(allSorted, token, compare);
      primaryReplicas[stringify(token)] = h;
    });
    datacenters[h.datacenter] = (datacenters[h.datacenter] || 0) + 1
  });
  //Primary replica for given token
  this.primaryReplicas = primaryReplicas;
  //All the tokens in ring order
  this.ring = allSorted;
  //Amount of hosts per datacenter
  this.datacenters = datacenters;
};

Metadata.prototype.setKeyspaces = function (result) {
  if (!result || !result.rows) {
    return;
  }
  var keyspaces = {};
  for (var i = 0; i < result.rows.length; i++) {
    var row = result.rows[i];
    this.setKeyspaceInfo(row, keyspaces);
  }
  this.keyspaces = keyspaces;
};

/**
 * Creates or updates the information on a keyspace for a given system.schema_keyspaces row
 * @param {Row} row
 * @param {Object} [keyspaces] Object map where to include the info
 */
Metadata.prototype.setKeyspaceInfo = function (row, keyspaces) {
  if (!keyspaces) {
    keyspaces = this.keyspaces;
  }
  var ksInfo = {
    name: row['keyspace_name'],
    durableWrites: row['durable_writes'],
    strategy: row['strategy_class'],
    strategyOptions: row['strategy_options']
  };
  ksInfo.tokenToReplica = this.getTokenToReplicaMapper(ksInfo.strategy, ksInfo.strategyOptions);
  keyspaces[ksInfo.name] = ksInfo;
};

Metadata.prototype.getTokenToReplicaMapper = function (strategy, strategyOptions) {
  if (/SimpleStrategy$/.test(strategy)) {
    var rf = strategyOptions['replication_factor'];
    if (rf > 1 && this.ring.length > 1) {
      return this.getTokenToReplicaSimpleMapper(rf);
    }
  }
  if (/NetworkTopologyStrategy$/.test(strategy)) {
    //noinspection JSUnresolvedVariable
    return this.getTokenToReplicaNetworkMapper(JSON.parse(strategyOptions));
  }
  //default, wrap in an Array
  var self = this;
  return (function noStrategy() {
    var replicas = {};
    for (var key in self.primaryReplicas) {
      if (!self.primaryReplicas.hasOwnProperty(key)) {
        continue;
      }
      replicas[key] = [self.primaryReplicas[key]];
    }
    return replicas;
  });
};

/**
 * @param {Number} replicationFactor
 * @returns {function}
 */
Metadata.prototype.getTokenToReplicaSimpleMapper = function (replicationFactor) {
  var self = this;
  return (function tokenSimpleStrategy() {
    var rf = Math.min(replicationFactor, self.ring.length);
    var replicas = {};
    for (var i = 0; i < self.ring.length; i++) {
      var token = self.ring[i];
      var key = self.tokenizer.stringify(token);
      var tokenReplicas = [self.primaryReplicas[key]];
      for (var j = 1; j < rf; j++) {
        var nextReplicaIndex = i + j;
        if (nextReplicaIndex >= self.ring.length) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % self.ring.length;
        }
        var nextReplica = self.primaryReplicas[self.tokenizer.stringify(self.ring[nextReplicaIndex])];
        tokenReplicas.push(nextReplica);
      }
      replicas[key] = tokenReplicas;
    }
    return replicas;
  });
};

/**
 * @param {Object} replicationFactors
 * @returns {Function}
 */
Metadata.prototype.getTokenToReplicaNetworkMapper = function (replicationFactors) {
  //                A(1)
  //
  //           H         B(2)
  //                |
  //      G       --+--       C(1)
  //                |
  //           F         D(2)
  //
  //                E(1)
  var self = this;
  function isDoneForToken(replicasByDc) {
    for (var dc in replicationFactors) {
      if (!replicationFactors.hasOwnProperty(dc)) {
        continue;
      }
      var rf = Math.min(replicationFactors[dc], self.datacenters[dc]);
      if (replicasByDc[dc] < rf) {
        return false;
      }
    }
    return true;
  }

  return (function tokenNetworkStrategy() {
    //For each token
    //Get an Array of tokens
    //Checking that there aren't more tokens per dc than specified by the replication factors
    var replicas = {};
    for (var i = 0; i < self.ring.length; i++) {
      var token = self.ring[i];
      var key = self.tokenizer.stringify(token);
      var tokenReplicas = [];
      var replicasByDc = {};
      for (var j = 0; j < self.ring.length; j++) {
        var nextReplicaIndex = i + j;
        if (nextReplicaIndex >= self.ring.length) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % self.ring.length;
        }
        var h = self.primaryReplicas[self.tokenizer.stringify(self.ring[nextReplicaIndex])];
        //Check if the next replica belongs to one of the targeted dcs
        var dcRf = parseFloat(replicationFactors[h.datacenter]);
        if (!dcRf) {
          continue;
        }
        dcRf = Math.min(dcRf, self.datacenters[h.datacenter]);
        var dcReplicas = replicasByDc[h.datacenter] || 0;
        //Amount of replicas per dc is greater than rf or the amount of host in the datacenter
        if (dcReplicas >= dcRf) {
          continue;
        }
        replicasByDc[h.datacenter] = dcReplicas + 1;
        tokenReplicas.push(h);
        if (isDoneForToken(replicasByDc)) {
          break;
        }
      }
      replicas[key] = tokenReplicas;
    }
    return replicas;
  });
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * @param {String} keyspaceName
 * @param {Buffer} tokenBuffer
 * @returns {Array}
 */
Metadata.prototype.getReplicas = function (keyspaceName, tokenBuffer) {
  var keyspace;
  if (keyspaceName) {
    keyspace = this.keyspaces[keyspaceName];
  }
  if (!this.ring) {
    return null;
  }
  var token = this.tokenizer.hash(tokenBuffer);
  var i = utils.binarySearch(this.ring, token, this.tokenizer.compare);
  if (i < 0) {
    i = ~i;
  }
  if (i >= this.ring.length) {
    //it circled back
    i = i % this.ring.length;
  }
  var closestToken = this.tokenizer.stringify(this.ring[i]);

  if (!keyspace) {
    return [this.primaryReplicas[closestToken]];
  }
  if (!keyspace.replicas) {
    //Calculate replicas the first time for the keyspace
    keyspace.replicas = keyspace.tokenToReplica();
  }
  return keyspace.replicas[closestToken];
};

Metadata.prototype.log = utils.log;

/**
 * Gets the metadata information already stored associated to a prepared statement
 * @param {String} query
 */
Metadata.prototype.getPreparedInfo = function (query) {
  //overflow protection
  if (this.preparedQueries.__length >= this.options.maxPrepared) {
    var toRemove;
    this.log('warning', 'Prepared statements exceeded maximum. This could be caused by preparing queries that contain parameters');
    for (var key in this.preparedQueries) {
      if (this.preparedQueries.hasOwnProperty(key) && this.preparedQueries[key].queryId) {
        toRemove = key;
        break;
      }
    }
    if (toRemove) {
      delete this.preparedQueries[toRemove];
      this.preparedQueries.__length--;
    }
  }
  var name = ( this.keyspace || '' ) + query;
  var info = this.preparedQueries[name];
  if (!info) {
    info = new events.EventEmitter();
    info.setMaxListeners(0);
    this.preparedQueries[name] = info;
    this.preparedQueries.__length++;
  }
  return info;
};

/**
 * Clears the internal state related to the prepared statements.
 * Following calls to the Client using the prepare flag will re-prepare the statements.
 */
Metadata.prototype.clearPrepared = function () {
  this.preparedQueries = {"__length": 0};
};

module.exports = Metadata;
