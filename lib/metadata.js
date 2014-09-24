var t = require('./tokenizer.js');

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
  //go through all the hosts, get the tokens
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
  if (/SimpleStrategy$/.test(strategy)) {
    return this.getTokenToReplicaSimpleMapper(strategyOptions['replication_factor']);
  }
  if (/NetworkTopologyStrategy$/.test(strategy)) {
    return this.getTokenToReplicaNetworkMapper(strategyOptions);
  }
  //default, wrap in an Array
  return (function (tokenPrimaries) {
    for (var key in tokenPrimaries) {
      if (!tokenPrimaries.hasOwnProperty(key)) {
        continue;
      }
      tokenPrimaries[key] = [tokenPrimaries[key]];
    }
  });
};

Metadata.prototype.getTokenToReplicaSimpleMapper = function (replicationFactor) {
  //TODO
  return (function (tokenPrimaries) {});
};

Metadata.prototype.getTokenToReplicaNetworkMapper = function (replicationFactors) {
  //TODO
  return (function (tokenPrimaries) {});
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * @param {Buffer} token
 * @returns {HostMap}
 */
Metadata.prototype.getReplicas = function (token) {
  //TODO
};

module.exports = Metadata;
