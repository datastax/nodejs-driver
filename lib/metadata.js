"use strict";
var events = require('events');
var util = require('util');
var async = require('async');

var t = require('./tokenizer');
var utils = require('./utils');
var errors = require('./errors');
var types = require('./types');

/** @const */
var selectUdt = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='%s' AND type_name = '%s'";

/**
 * Represents cluster and schema information.
 * The metadata class acts as a internal state of the driver.
 * @param {ClientOptions} options
 * @param {ControlConnection} controlConnection Control connection used to retrieve information.
 * @constructor
 */
function Metadata (options, controlConnection) {
  Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false});
  this.controlConnection = controlConnection;
  this.keyspaces = {};
  this.clearPrepared();
}

/**
 * @ignore
 * @param {String} partitionerName
 */
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
 * @ignore
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

/**
 * @ignore
 * @param {ResultSet} result
 */
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
 * @ignore
 */
Metadata.prototype.setKeyspaceInfo = function (row, keyspaces) {
  if (!keyspaces) {
    keyspaces = this.keyspaces;
  }
  var ksInfo = {
    name: row['keyspace_name'],
    durableWrites: row['durable_writes'],
    strategy: row['strategy_class'],
    strategyOptions: JSON.parse(row['strategy_options']),
    tokenToReplica: null,
    udts: {},
    tables: {}
  };
  ksInfo.tokenToReplica = this.getTokenToReplicaMapper(ksInfo.strategy, ksInfo.strategyOptions);
  keyspaces[ksInfo.name] = ksInfo;
};

/** @private */
Metadata.prototype.getTokenToReplicaMapper = function (strategy, strategyOptions) {
  if (/SimpleStrategy$/.test(strategy)) {
    var rf = strategyOptions['replication_factor'];
    if (rf > 1 && this.ring && this.ring.length > 1) {
      return this.getTokenToReplicaSimpleMapper(rf);
    }
  }
  if (/NetworkTopologyStrategy$/.test(strategy)) {
    //noinspection JSUnresolvedVariable
    return this.getTokenToReplicaNetworkMapper(strategyOptions);
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
 * @private
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
 * @private
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
 * @ignore
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

/**
 * Gets the definition of an user defined type.
 * When trying to retrieve the same udt definition concurrently,
 *  it will query once and invoke all callbacks with the retrieved information.
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the UDT
 * @param {Function} callback
 */
Metadata.prototype.getUdt = function (keyspaceName, name, callback) {
  var keyspace = this.keyspaces[keyspaceName];
  if (!keyspace) {
    return callback(null, null);
  }
  var udtInfo = keyspace.udts[name];
  if (!udtInfo) {
    keyspace.udts[name] = udtInfo = new events.EventEmitter();
    udtInfo.setMaxListeners(0);
    udtInfo.loading = false;
    udtInfo.name = name;
    udtInfo.fields = null;
  }
  if (udtInfo.fields) {
    return callback(null, udtInfo);
  }
  udtInfo.once('load', callback);
  if (udtInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  udtInfo.loading = true;
  //it is not cached, try to query for it
  var query = util.format(selectUdt, keyspace.name, name);
  var self = this;
  this.controlConnection.query(query, function (err, response) {
    udtInfo.loading = false;
    if (err) {
      return udtInfo.emit('load', err);
    }
    var row = response.rows[0];
    udtInfo.emit('load', null, self._parseUdt(udtInfo, row));
  });
};

/**
 * Parses the udt information from the row
 * @returns {{fields: Array}}|null
 * @private
 */
Metadata.prototype._parseUdt = function (udtInfo, row) {
  if (!row) {
    return null;
  }
  var fieldNames = row['field_names'];
  var fieldTypes = row['field_types'];
  var fields = new Array(fieldNames.length);
  var encoder = this.controlConnection.getEncoder();
  for (var i = 0; i < fieldNames.length; i++) {
    fields[i] = {
      name: fieldNames[i],
      type: encoder.parseTypeName(fieldTypes[i])
    };
  }
  udtInfo.fields = fields;
  return udtInfo;
};

/**
 * Uses the metadata to fill the user provided parameter hints
 * @param {String} keyspace
 * @param {Array} hints
 * @param {Function} callback
 * @internal
 * @ignore
 */
Metadata.prototype.adaptUserHints = function (keyspace, hints, callback) {
  var udts = [];
  //check for udts and get the metadata
  function checkUdtTypes(type) {
    if (type.code === types.dataTypes.udt) {
      var udtName = type.info.split('.');
      type.info = {
        keyspace: udtName[0],
        name: udtName[1]
      };
      if (!type.info.name) {
        if (!keyspace) {
          throw new TypeError('No keyspace specified for udt: ' + udtName.join('.'));
        }
        //use the provided keyspace
        type.info.name = type.info.keyspace;
        type.info.keyspace = keyspace;
      }
      udts.push(type);
      return;
    }
    if (!type.info) {
      return;
    }
    if (type.code === types.dataTypes.list || type.code === types.dataTypes.set) {
      return checkUdtTypes(type.info);
    }
    if (type.code === types.dataTypes.map) {
      checkUdtTypes(type.info[0]);
      checkUdtTypes(type.info[1]);
    }
  }
  for (var i = 0; i < hints.length; i++) {
    var hint = hints[i];
    if (typeof hint !== 'string') {
      continue;
    }
    try {
      var type = types.dataTypes.getByName(hint);
      checkUdtTypes(type);
      hints[i] = type;
    }
    catch (err) {
      return callback(err);
    }
  }
  var self = this;
  async.each(udts, function (type, next) {
    self.getUdt(type.info.keyspace, type.info.name, function (err, udtInfo) {
      if (err) return next(err);
      if (!udtInfo) {
        return next(new TypeError('User defined type not found: ' + type.info.keyspace + '.' + type.info.name));
      }
      type.info = udtInfo;
      next();
    });
  }, callback);
};

module.exports = Metadata;
