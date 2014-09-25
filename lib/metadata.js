var t = require('./tokenizer.js');
var utils = require('./utils.js');

/**
 * Represents cluster information
 * @constructor
 */
function Metadata () {
  this.keyspaces = null;
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
  hostArray.forEach(function (h) {
    h.tokens.forEach(function (tokenString) {
      var token = parser(tokenString);
      utils.insertSorted(allSorted, token, compare);
      primaryReplicas[stringify(token)] = h;
    });
  });
  this.primaryReplicas = primaryReplicas;
  this.ring = allSorted;
};

Metadata.prototype.setKeyspaces = function (result) {
  if (!result || !result.rows) {
    return;
  }
  var keyspaces = {};
  for (var i = 0; i < result.rows.length; i++) {
    var row = result.rows[i];
    var ksInfo = {
      name: row['keyspace_name'],
      durableWrites: row['durable_writes'],
      strategy: row['strategy_class'],
      strategyOptions: row['strategy_options']
    };
    ksInfo.tokenToReplica = this.getTokenToReplicaMapper(ksInfo.strategy, ksInfo.strategyOptions);
    keyspaces[ksInfo.name] = ksInfo;
  }
  this.keyspaces = keyspaces;
};

Metadata.prototype.getTokenToReplicaMapper = function (strategy, strategyOptions) {
//  if (/SimpleStrategy$/.test(strategy)) {
//    return this.getTokenToReplicaSimpleMapper(strategyOptions['replication_factor']);
//  }
//  if (/NetworkTopologyStrategy$/.test(strategy)) {
//    return this.getTokenToReplicaNetworkMapper(strategyOptions);
//  }
  //default, wrap in an Array
  var self = this;
  return (function () {
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

Metadata.prototype.getTokenToReplicaSimpleMapper = function (replicationFactor) {
  //TODO
  return (function () {});
};

Metadata.prototype.getTokenToReplicaNetworkMapper = function (replicationFactors) {
  //TODO
  return (function () {});
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * @param {String} keyspaceName
 * @param {Buffer} tokenBuffer
 * @returns {HostMap|null}
 */
Metadata.prototype.getReplicas = function (keyspaceName, tokenBuffer) {
  var token = this.tokenizer.hash(tokenBuffer);
  var keyspace = this.keyspaces[keyspaceName];
  if (!keyspace || !this.ring) {
    return null;
  }
  var i = utils.binarySearch(this.ring, token, this.tokenizer.compare);
  if (i < 0) {
    i = ~i;
  }
  if (i >= this.ring.length) {
    //it circled back
    i = i % this.ring.length;
  }
  var closestToken = this.ring[i];
  if (!keyspace.replicas) {
    //Calculate replicas the first time for the keyspace
    keyspace.replicas = keyspace.tokenToReplica();
  }
  return keyspace.replicas[this.tokenizer.stringify(closestToken)];
};

module.exports = Metadata;
