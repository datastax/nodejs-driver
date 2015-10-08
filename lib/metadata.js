"use strict";
var events = require('events');
var util = require('util');
var async = require('async');

var t = require('./tokenizer');
var utils = require('./utils');
var errors = require('./errors');
var types = require('./types');
var requests = require('./requests');

/**
 * @const
 * @private
 */
var _selectUdt = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='%s' AND type_name='%s'";
/**
 * @const
 * @private
 */
var _selectTable = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'";
/**
 * @const
 * @private
 */
var _selectColumns = "SELECT * FROM system.schema_columns WHERE keyspace_name='%s' AND columnfamily_name='%s'";
/**
 * @const
 * @private
 */
var _selectTraceSession = "SELECT * FROM system_traces.sessions WHERE session_id=%s";
/**
 * @const
 * @private
 */
var _selectTraceEvents = "SELECT * FROM system_traces.events WHERE session_id=%s";
/**
 * @const
 * @private
 */
var _selectSchemaVersionPeers = "SELECT schema_version FROM system.peers";
/**
 * @const
 * @private
 */
var _selectSchemaVersionLocal = "SELECT schema_version FROM system.local";
/**
 * @const
 * @private
 */
var _selectFunctions = "SELECT * FROM system.schema_functions WHERE keyspace_name = '%s' AND function_name = '%s'";
/**
 * @const
 * @private
 */
var _selectAggregates = "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";
/**
 * @const
 * @private
 */
var _traceMaxAttemps = 5;
/**
 * @const
 * @private
 */
var _traceAttemptDelay = 200;

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
  var hostArray = hosts.values();
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
    tables: {},
    functions: {},
    aggregates: {}
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
 * <p>
 * When trying to retrieve the same udt definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
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
  var query = util.format(_selectUdt, keyspace.name, name);
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
 * Gets the definition of a table.
 * <p>
 * When trying to retrieve the same table definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Table
 * @param {Function} callback The callback with the err as a first parameter and the {@link TableMetadata} as second parameter.
 */
Metadata.prototype.getTable = function (keyspaceName, name, callback) {
  var keyspace = this.keyspaces[keyspaceName];
  if (!keyspace) {
    return callback(null, null);
  }
  var tableInfo = keyspace.tables[name];
  if (!tableInfo) {
    keyspace.tables[name] = tableInfo = new TableMetadata(name, this.controlConnection.getEncoder());
  }
  if (tableInfo.loaded) {
    return callback(null, tableInfo);
  }
  tableInfo.once('load', callback);
  if (tableInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  tableInfo.loading = true;
  //it is not cached, try to query for it
  var self = this;
  async.waterfall([
    function getTableRow(next) {
      var query = util.format(_selectTable, keyspace.name, name);
      self.controlConnection.query(query, function (err, response) {
        if (err) return next(err);
        next(null, response.rows[0]);
      });
    },
    function getColumnRows (tableRow, next) {
      if (!tableRow) return next();
      var query = util.format(_selectColumns, keyspace.name, name);
      self.controlConnection.query(query, function (err, response) {
        if (err) return next(err);
        next(null, tableRow, response.rows);
      });
    }
  ], function afterQuery (err, tableRow, columnRows) {
    tableInfo.loading = false;
    if (err || !tableRow) {
      return tableInfo.emit('load', err, null);
    }
    try {
      tableInfo.build(tableRow, columnRows);
    }
    catch (buildErr) {
      err = buildErr;
    }
    tableInfo.emit('load', err, tableInfo);
  });
};

/**
 * Gets the definition of CQL functions for a given name.
 * <p>
 * When trying to retrieve the same function definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Function
 * @param {Function} callback The callback with the err as a first parameter and the array of {@link SchemaFunction} as second parameter.
 */
Metadata.prototype.getFunctions = function (keyspaceName, name, callback) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError('Callback is not a function');
  }
  if (!keyspaceName || !name) {
    return callback(new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata'));
  }
  this._getFunctions(keyspaceName, name, false, function (err, functionsMap) {
    if (err) {
      return callback(err, null);
    }
    callback(null, utils.objectValues(functionsMap));
  });
};

/**
 * Gets a definition of CQL function for a given name and signature.
 * <p>
 * When trying to retrieve the same function definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Function
 * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
 * @param {Function} callback The callback with the err as a first parameter and the {@link SchemaFunction} as second parameter.
 */
Metadata.prototype.getFunction = function (keyspaceName, name, signature, callback) {
  this._getSingleFunction(keyspaceName, name, signature, false, callback);
};

/**
 * Gets the definition of CQL aggregate for a given name.
 * <p>
 * When trying to retrieve the same aggregates definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Function
 * @param {Function} callback The callback with the err as a first parameter and the array of {@link Aggregate} as second parameter.
 */
Metadata.prototype.getAggregates = function (keyspaceName, name, callback) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError('Callback is not a function');
  }
  if (!keyspaceName || !name) {
    return callback(new errors.ArgumentError('You must provide the keyspace name and cql aggregate name to retrieve the metadata'));
  }
  this._getFunctions(keyspaceName, name, true, function (err, functionsMap) {
    if (err) {
      return callback(err, null);
    }
    callback(null, utils.objectValues(functionsMap));
  });
};

/**
 * Gets a definition of CQL aggregate for a given name and signature.
 * <p>
 * When trying to retrieve the same aggregate definition concurrently,
 * it will query once and invoke all callbacks with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the aggregate
 * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
 * @param {Function} callback The callback with the err as a first parameter and the {@link Aggregate} as second parameter.
 */
Metadata.prototype.getAggregate = function (keyspaceName, name, signature, callback) {
  this._getSingleFunction(keyspaceName, name, signature, true, callback);
};

/**
 * Gets a map of cql function definitions or aggregates based on signature.
 * @param {String} keyspaceName
 * @param {String} name Name of the function or aggregate
 * @param {Boolean} aggregate
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getFunctions = function (keyspaceName, name, aggregate, callback) {
  var keyspace = this.keyspaces[keyspaceName];
  if (!keyspace) {
    return callback(null, null);
  }
  var cache = keyspace.functions;
  var query = _selectFunctions;
  var Constructor = SchemaFunction;
  if (aggregate) {
    cache = keyspace.aggregates;
    query = _selectAggregates;
    Constructor = Aggregate;
  }
  //if not already loaded
  //get all functions with that name
  //cache it by name and, within name, by signature
  var functionsInfo = cache[name];
  if (!functionsInfo) {
    cache[name] = functionsInfo = new events.EventEmitter();
    functionsInfo.setMaxListeners(0);
  }
  if (functionsInfo.values) {
    return callback(null, functionsInfo.values);
  }
  functionsInfo.once('load', callback);
  if (functionsInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  functionsInfo.loading = true;
  //it is not cached, try to query for it
  var self = this;
  query = util.format(query, keyspace.name, name);
  self.controlConnection.query(query, function (err, response) {
    functionsInfo.loading = false;
    if (err || response.rows.length === 0) {
      return functionsInfo.emit('load', err, null);
    }
    var encoder = self.controlConnection.getEncoder();
    if (response.rows.length > 0) {
      functionsInfo.values = {};
    }
    try {
      response.rows.forEach(function (row) {
        var func = new Constructor(encoder, row);
        functionsInfo.values['(' + func.signature.join(',') + ')'] = func;
      });
    }
    catch (buildErr) {
      err = buildErr;
    }
    if (err) {
      functionsInfo.values = null;
    }
    functionsInfo.emit('load', err, functionsInfo.values);
  });
};

/**
 * Gets a single cql function or aggregate definition
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Array} signature
 * @param {Boolean} aggregate
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getSingleFunction = function (keyspaceName, name, signature, aggregate, callback) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError('Callback is not a function');
  }
  if (!keyspaceName || !name) {
    return callback(new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata'));
  }
  if (!util.isArray(signature)) {
    return callback(new errors.ArgumentError('Signature must be an array of types'));
  }
  try {
    signature = signature.map(function (item) {
      if (typeof item === 'string') {
        return item;
      }
      return types.getDataTypeNameByCode(item);
    });
  }
  catch (err) {
    return callback(err);
  }
  this._getFunctions(keyspaceName, name, aggregate, function (err, functionsMap) {
    if (err) {
      return callback(err, null);
    }
    var f;
    if (functionsMap) {
      f = functionsMap['(' + signature.join(',') + ')'];
    }
    callback(null, f || null);
  });
};


/**
 * Gets the trace session generated by Cassandra when query tracing is enabled for the
 * query. The trace itself is stored in Cassandra in the <code>sessions</code> and
 * <code>events</code> table in the <code>system_traces</code> keyspace and can be
 * retrieve manually using the trace identifier.
 * @param {Uuid} traceId Identifier of the trace session.
 * @param {Function} callback The callback with the err as first parameter and the query trace as second parameter
 */
Metadata.prototype.getTrace = function (traceId, callback) {
  var trace;
  var attempts = 0;
  var selectSession = util.format(_selectTraceSession, traceId);
  var selectEvents = util.format(_selectTraceEvents, traceId);
  var self = this;
  async.whilst(function condition() {
    return !trace && (attempts++  < _traceMaxAttemps);
  }, function iterator(next) {
    self.controlConnection.query(selectSession, function (err, result) {
      if (err) return next(err);
      var sessionRow = result.rows[0];
      if (!sessionRow || !sessionRow['duration']) {
        return setTimeout(next, _traceAttemptDelay);
      }
      trace = {
        requestType: sessionRow['request'],
        coordinator: sessionRow['coordinator'],
        parameters: sessionRow['parameters'],
        startedAt: sessionRow['started_at'],
        duration: sessionRow['duration'],
        clientAddress: sessionRow['client'],
        events: []
      };
      self.controlConnection.query(selectEvents, function (err, result) {
        if (err) return next(err);
        result.rows.forEach(function (row) {
          trace.events.push({
            id: row['event_id'],
            activity: row['activity'],
            source: row['source'],
            elapsed: row['source_elapsed'],
            thread: row['thread']
          });
        });
        next();
      });
    });
  }, function getTraceEnded(err) {
    if (!err && !trace) {
      err = new Error(util.format('Trace %s could not fully retrieved after %d attempts', traceId, _traceMaxAttemps));
    }
    callback(err, trace);
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

/**
 * Uses the provided connection to query for the local schema version
 * @param {Connection} connection
 * @param {Function} callback
 * @internal
 * @ignore
 */
Metadata.prototype.getLocalSchemaVersion = function (connection, callback) {
  var request = new requests.QueryRequest(_selectSchemaVersionLocal, null, null);
  connection.sendStream(request, utils.emptyObject, function (err, result) {
    var version;
    if (!err && result && result.rows && result.rows.length === 1) {
      version = result.rows[0]['schema_version'];
    }
    callback(err, version);
  });
};

/**
 * Uses the provided connection to query for peers' schema version
 * @param {Connection} connection
 * @param {Function} callback
 * @internal
 * @ignore
 */
Metadata.prototype.getPeersSchemaVersions = function (connection, callback) {
  var request = new requests.QueryRequest(_selectSchemaVersionPeers, null, null);
  connection.sendStream(request, utils.emptyObject, function (err, result) {
    var versions = [];
    if (!err && result && result.rows) {
      for (var i = 0; i < result.rows.length; i++) {
        versions.push(result.rows[i]['schema_version']);
      }
    }
    callback(err, versions);
  });
};

/**
 * Creates a new instance of TableMetadata
 * @param {String} name
 * @param {Encoder} encoder
 * @class
 * @classdesc Describes a table
 * @constructor
 */
function TableMetadata(name, encoder) {
  events.EventEmitter.call(this);
  this.setMaxListeners(0);
  //private
  Object.defineProperty(this, 'loading', { value: false, enumerable: false, writable: true });
  Object.defineProperty(this, 'loaded', { value: false, enumerable: false, writable: true });
  Object.defineProperty(this, 'encoder', { value: encoder, enumerable: false, writable: false });
  /**
   * Name of the table
   * @type {String}
   */
  this.name = name;
  /**
   * False-positive probability for SSTable Bloom filters.
   * @type {number}
   */
  this.bloomFilterFalsePositiveChance = 0;
  /**
   * Level of caching: all, keys_only, rows_only, none
   * @type {String}
   */
  this.caching = null;
  /**
   * A human readable comment describing the table.
   * @type {String}
   */
  this.comment = null;
  /**
   * Specifies the time to wait before garbage collecting tombstones (deletion markers)
   * @type {number}
   */
  this.gcGraceSeconds = 0;
  /**
   * Compaction strategy class used for the table.
   * @type {String}
   */
  this.compactionClass = null;
  /**
   * Associative-array containing the compaction options keys and values.
   * @type {Object}
   */
  this.compactionOptions = null;
  /**
   * Associative-array containing the compression options.
   * @type {Object}
   */
  this.compression = null;
  /**
   * Specifies the probability of read repairs being invoked over all replicas in the current data center.
   * @type {number}
   */
  this.localReadRepairChance = 0;
  /**
   * Specifies the probability with which read repairs should be invoked on non-quorum reads. The value must be between 0 and 1.
   * @type {number}
   */
  this.readRepairChance = 0;
  /**
   * Applies only to counter tables.
   * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
   * the client for a write request. For counter tables, this should always be set to true.
   * @type {boolean}
   */
  this.replicateOnWrite = true;
  /**
   * Array describing the table columns.
   * @type {Array}
   */
  this.columns = [];
  /**
   * An associative Array of columns by name.
   * @type {Object}
   */
  this.columnsByName = null;
  /**
   * Array describing the columns that are part of the partition key.
   * @type {Array}
   */
  this.partitionKeys = [];
  /**
   * Array describing the columns that form the clustering key.
   * @type {Array}
   */
  this.clusteringKeys = [];
  /**
   * Array describing the clustering order of the columns in the same order as the clusteringKeys.
   * @type {Array}
   */
  this.clusteringOrder = [];
}

util.inherits(TableMetadata, events.EventEmitter);

/**
 * Builds the metadata based on the table and column rows
 * @param {Row} tableRow
 * @param {Array.<Row>} columnRows
 * @throws {Error}
 * @internal
 * @ignore
 */
TableMetadata.prototype.build = function (tableRow, columnRows) {
  var i, c, name, types;
  var columnsKeyed = {};
  var partitionKeys = [];
  var clusteringKeys = [];
  this.loaded = true;
  this.bloomFilterFalsePositiveChance = tableRow['bloom_filter_fp_chance'];
  this.caching = tableRow['caching'];
  this.comment = tableRow['comment'];
  this.compactionClass = tableRow['compaction_strategy_class'];
  this.compactionOptions = JSON.parse(tableRow['compaction_strategy_options']);
  this.compression = JSON.parse(tableRow['compression_parameters']);
  this.gcGraceSeconds = tableRow['gc_grace_seconds'];
  this.localReadRepairChance = tableRow['local_read_repair_chance'];
  this.readRepairChance = tableRow['read_repair_chance'];
  if (typeof tableRow['replicate_on_write'] !== 'undefined') {
    //leave the default otherwise
    this.replicateOnWrite = tableRow['replicate_on_write'];
  }
  for (i = 0; i < columnRows.length; i++) {
    var row = columnRows[i];
    var type = this.encoder.parseTypeName(row['validator']);
    c = {
      name: row['column_name'],
      type: type
    };
    this.columns.push(c);
    columnsKeyed[c.name] = c;
    switch (row['type']) {
      case 'partition_key':
        partitionKeys.push({ c: c, index: (row['component_index'] || 0)});
        break;
      case 'clustering_key':
        clusteringKeys.push({ c: c, index: (row['component_index'] || 0), order: c.type.options.reversed ? 'DESC' : 'ASC'});
        break;
    }
  }
  if (partitionKeys.length > 0) {
    this.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(function (item) {
      return item.c;
    });
    clusteringKeys.sort(utils.propCompare('index'));
    this.clusteringKeys = clusteringKeys.map(function (item) {
      return item.c;
    });
    this.clusteringOrder = clusteringKeys.map(function (item) {
      return item.order;
    });
  }
  //In C* 1.2, keys are not stored on the schema_columns table
  var keysStoredInTableRow = (this.partitionKeys.length === 0);
  if (keysStoredInTableRow && tableRow['key_aliases']) {
    partitionKeys = JSON.parse(tableRow['key_aliases']);
    types = this.encoder.parseKeyTypes(tableRow['key_validator']).types;
    for (i = 0; i < partitionKeys.length; i++) {
      name = partitionKeys[i];
      c = columnsKeyed[name];
      if (!c) {
        c = {
          name: name,
          type: types[i]
        };
        this.columns.push(c);
      }
      this.partitionKeys.push(c);
    }
  }
  var comparator = this.encoder.parseKeyTypes(tableRow['comparator']);
  if (keysStoredInTableRow && tableRow['column_aliases']) {
    clusteringKeys = JSON.parse(tableRow['column_aliases']);
    for (i = 0; i < clusteringKeys.length; i++) {
      name = clusteringKeys[i];
      c = columnsKeyed[name];
      if (!c) {
        c = {
          name: name,
          type: comparator.types[i]
        };
        this.columns.push(c);
      }
      this.clusteringKeys.push(c);
      this.clusteringOrder.push(c.type.options.reversed ? 'DESC' : 'ASC');
    }
  }
  this.isCompact = !!tableRow['is_dense'];
  if (!this.isCompact) {
    //is_dense column does not exist in previous versions of Cassandra
    //also, compact pk, ck and val appear as is_dense false
    // clusteringKeys != comparator types - 1
    // or not composite (comparator)
    this.isCompact = (
      //clustering keys are not marked as composite
      !comparator.isComposite ||
      //only 1 column not part of the partition or clustering keys
      (!comparator.hasCollections && this.clusteringKeys.length !== comparator.types.length - 1)
    );
  }
  name = tableRow['value_alias'];
  if (this.isCompact && name && !columnsKeyed[name]) {
    //additional column in C* 1.2 as value_alias
    c = {
      name: name,
      type: this.encoder.parseTypeName(tableRow['default_validator'])
    };
    this.columns.push(c);
    columnsKeyed[name] = c;
  }
  this.columnsByName = columnsKeyed;
  return this;
};

/**
 * Creates a new SchemaFunction.
 * @class
 * @classdesc Describes a CQL function.
 * @param {Encoder} encoder
 * @param {Row} row
 * @constructor
 */
function SchemaFunction(encoder, row) {
  /**
   * Name of the cql function.
   * @type {String}
   */
  this.name = row['function_name'];
  /**
   * Name of the keyspace where the cql function is declared.
   */
  this.keyspaceName = row['keyspace_name'];
  /**
   * Signature of the function.
   * @type {Array.<String>}
   */
  this.signature = row['signature'] || utils.emptyArray;
  /**
   * List of the function argument names.
   * @type {Array.<String>}
   */
  this.argumentNames = row['argument_names'] || utils.emptyArray;
  /**
   * List of the function argument types.
   * @type {Array.<{code, info}>}
   */
  this.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
    return encoder.parseTypeName(name);
  });
  /**
   * Body of the function.
   * @type {String}
   */
  this.body = row['body'];
  /**
   * Determines if the function is called when the input is null.
   * @type {Boolean}
   */
  this.calledOnNullInput = row['called_on_null_input'];
  /**
   * Name of the programming language, for example: java, javascript, ...
   * @type {String}
   */
  this.language = row['language'];
  /**
   * Type of the return value.
   * @type {{code: number, info: (Object|Array|null)}}
   */
  this.returnType = encoder.parseTypeName(row['return_type']);
}

/**
 * Creates a new Aggregate.
 * @class
 * @classdesc Describes a CQL aggregate.
 * @param {Encoder} encoder
 * @param {Row} row
 * @constructor
 */
function Aggregate(encoder, row) {
  /**
   * Name of the aggregate.
   * @type {String}
   */
  this.name = row['aggregate_name'];
  /**
   * Name of the keyspace where the aggregate is declared.
   */
  this.keyspaceName = row['keyspace_name'];
  /**
   * Signature of the aggregate.
   * @type {Array.<String>}
   */
  this.signature = row['signature'] || utils.emptyArray;
  /**
   * List of the CQL aggregate argument types.
   * @type {Array.<{code, info}>}
   */
  this.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
    return encoder.parseTypeName(name);
  });
  /**
   * State Function.
   * @type {String}
   */
  this.stateFunction = row['state_func'];
  /**
   * State Type.
   * @type {{code, info}}
   */
  this.stateType = encoder.parseTypeName(row['state_type']);
  /**
   * Final Function.
   * @type {String}
   */
  this.finalFunction = row['final_func'];
  /**
   * Initial state value of this aggregate.
   * @type {Buffer}
   */
  this.initConditionRaw = row['initcond'];
  /**
   * Initial state value of this aggregate.
   * @type {Object}
   */
  this.initCondition = encoder.decode(this.initConditionRaw, this.stateType);
  /**
   * Type of the return value.
   * @type {{code: number, info: (Object|Array|null)}}
   */
  this.returnType = encoder.parseTypeName(row['return_type']);
}

module.exports = Metadata;
