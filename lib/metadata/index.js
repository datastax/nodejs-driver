"use strict";
var events = require('events');
var util = require('util');
/**
 * Module containing classes and fields related to metadata.
 * @module metadata
 */
var t = require('../tokenizer');
var utils = require('../utils');
var errors = require('../errors');
var types = require('../types');
var requests = require('../requests');
var schemaParserFactory = require('./schema-parser');

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
  Object.defineProperty(this, 'controlConnection', { value: controlConnection, enumerable: false, writable: false});
  this.keyspaces = {};
  this.clearPrepared();
  this._schemaParser = schemaParserFactory.getByVersion(controlConnection, this.getUdt.bind(this));
}

/**
 * Sets the cassandra version
 * @internal
 * @ignore
 * @param {Array.<Number>} version
 */
Metadata.prototype.setCassandraVersion = function (version) {
  this._schemaParser = schemaParserFactory.getByVersion(
    this.controlConnection, this.getUdt.bind(this), version, this._schemaParser);
};

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
 * Populates the information regarding primary replica per token, datacenters (+ racks) and sorted token ring.
 * @ignore
 * @param {HostMap} hosts
 */
Metadata.prototype.buildTokens = function (hosts) {
  if (!this.tokenizer) {
    return this.log('error', 'Tokenizer could not be determined');
  }
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
    var dc = datacenters[h.datacenter];
    if (!dc) {
      dc = datacenters[h.datacenter] = {
        hostLength: 0,
        racks: new utils.HashSet()
      };
    }
    dc.hostLength++;
    dc.racks.add(h.rack);
  });
  //Primary replica for given token
  this.primaryReplicas = primaryReplicas;
  //All the tokens in ring order
  this.ring = allSorted;
  //Datacenter metadata (host length and racks)
  this.datacenters = datacenters;
};

/**
 * Gets the keyspace metadata information and updates the internal state of the driver.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the keyspaces metadata refresh completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * @param {String} name Name of the keyspace.
 * @param {Function} [callback] Optional callback.
 */
Metadata.prototype.refreshKeyspace = function (name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, true, function handler(cb) {
    this._refreshKeyspaceCb(name, cb);
  });
};

/**
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._refreshKeyspaceCb = function (name, callback) {
  this.log('info', util.format('Retrieving keyspace %s metadata', name));
  var self = this;
  this._schemaParser.getKeyspace(name, function (err, ksInfo) {
    if (err) {
      self.log('error', 'There was an error while trying to retrieve keyspace information', err);
      return callback(err);
    }
    if (!ksInfo) {
      // the keyspace was dropped
      delete self.keyspaces[name];
      return callback();
    }
    // tokens are lazily init on the keyspace, once a replica from that keyspace is retrieved.
    self.keyspaces[ksInfo.name] = ksInfo;
    callback(null, ksInfo);
  });
};

/**
 * Gets the metadata information of all the keyspaces and updates the internal state of the driver.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the keyspace metadata refresh completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * @param {Boolean|Function} [waitReconnect] Determines if it should wait for reconnection in case the control connection is not
 * connected at the moment. Default: true.
 * @param {Function} [callback] Optional callback.
 */
Metadata.prototype.refreshKeyspaces = function (waitReconnect, callback) {
  if (typeof waitReconnect === 'function' || typeof waitReconnect === 'undefined') {
    callback = waitReconnect;
    waitReconnect = true;
  }

  return utils.promiseWrapper.call(this, this.options, callback, true, function handler(cb) {
    this._refreshKeyspacesCb(waitReconnect, cb);
  });
};

/**
 * @param {Boolean} waitReconnect
 * @param {Function} callback
 * @private
 */
Metadata.prototype._refreshKeyspacesCb = function (waitReconnect, callback) {
  this.log('info', 'Retrieving keyspaces metadata');
  var self = this;
  this._schemaParser.getKeyspaces(waitReconnect, function getKeyspacesCallback(err, keyspaces) {
    if (err) {
      self.log('error', 'There was an error while trying to retrieve keyspaces information', err);
      return callback(err);
    }
    self.keyspaces = keyspaces;
    callback(null, keyspaces);
  });
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * <p>
 *   It uses the pre-loaded keyspace metadata to retrieve the replicas for a token for a given keyspace.
 *   When the keyspace metadata has not been loaded, it returns null.
 * </p>
 * @param {String} keyspaceName
 * @param {Buffer} tokenBuffer
 * @returns {Array}
 */
Metadata.prototype.getReplicas = function (keyspaceName, tokenBuffer) {
  if (!this.ring) {
    return null;
  }
  var keyspace;
  if (keyspaceName) {
    keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      // the keyspace was not found, the metadata should be loaded beforehand
      return null;
    }
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

  if (!keyspaceName) {
    return [this.primaryReplicas[closestToken]];
  }
  if (!keyspace.replicas) {
    //Calculate replicas the first time for the keyspace
    keyspace.replicas = keyspace.tokenToReplica(this.tokenizer, this.ring, this.primaryReplicas, this.datacenters);
  }
  return keyspace.replicas[closestToken];
};

Metadata.prototype.log = utils.log;

/**
 * Gets the metadata information already stored associated to a prepared statement
 * @param {String} keyspaceName
 * @param {String} query
 * @ignore
 */
Metadata.prototype.getPreparedInfo = function (keyspaceName, query) {
  //overflow protection
  if (this.preparedQueries.__length >= this.options.maxPrepared) {
    var toRemove;
    this.log('warning',
      'Prepared statements exceeded maximum. This could be caused by preparing queries that contain parameters');
    var existingKeys = Object.keys(this.preparedQueries);
    for (var i = 0; i < existingKeys.length; i++) {
      var key = existingKeys[i];
      if (this.preparedQueries[key].queryId) {
        toRemove = key;
        break;
      }
    }
    if (toRemove) {
      delete this.preparedQueries[toRemove];
      this.preparedQueries.__length--;
    }
  }
  var name = ( keyspaceName || '' ) + query;
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
 * Gets the definition of an user-defined type.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same UDT definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace.
 * @param {String} name Name of the UDT.
 * @param {Function} [callback] The callback to invoke when retrieval completes.
 */
Metadata.prototype.getUdt = function (keyspaceName, name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getUdtCb(keyspaceName, name, cb);
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getUdtCb = function (keyspaceName, name, callback) {
  var cache;
  if (this.options.isMetadataSyncEnabled) {
    var keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      return callback(null, null);
    }
    cache = keyspace.udts;
  }
  this._schemaParser.getUdt(keyspaceName, name, cache, callback);
};

/**
 * Gets the definition of a table.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same table definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace.
 * @param {String} name Name of the Table.
 * @param {Function} [callback] The callback with the err as a first parameter and the {@link TableMetadata} as
 * second parameter.
 */
Metadata.prototype.getTable = function (keyspaceName, name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getTableCb(keyspaceName, name, cb);
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getTableCb = function (keyspaceName, name, callback) {
  var cache;
  if (this.options.isMetadataSyncEnabled) {
    var keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      return callback(null, null);
    }
    cache = keyspace.tables;
  }
  this._schemaParser.getTable(keyspaceName, name, cache, callback);
};

/**
 * Gets the definition of CQL functions for a given name.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same function definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace.
 * @param {String} name Name of the Function.
 * @param {Function} [callback] The callback with the err as a first parameter and the array of {@link SchemaFunction}
 * as second parameter.
 */
Metadata.prototype.getFunctions = function (keyspaceName, name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getFunctionsCb(keyspaceName, name, cb);
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getFunctionsCb = function (keyspaceName, name, callback) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError('Callback is not a function');
  }
  if (!keyspaceName || !name) {
    return callback(
      new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata'));
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
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same function definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Function
 * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
 * @param {Function} [callback] The callback with the err as a first parameter and the {@link SchemaFunction} as second
 * parameter.
 */
Metadata.prototype.getFunction = function (keyspaceName, name, signature, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getSingleFunctionCb(keyspaceName, name, signature, false, cb);
  });
};

/**
 * Gets the definition of CQL aggregate for a given name.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same aggregates definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the Function
 * @param {Function} [callback] The callback with the err as a first parameter and the array of {@link Aggregate} as
 * second parameter.
 */
Metadata.prototype.getAggregates = function (keyspaceName, name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getAggregatesCb(keyspaceName, name, cb);
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getAggregatesCb = function (keyspaceName, name, callback) {
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
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 * When trying to retrieve the same aggregate definition concurrently, it will query once and invoke all callbacks
 * with the retrieved information.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the aggregate
 * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
 * @param {Function} [callback] The callback with the err as a first parameter and the {@link Aggregate} as second parameter.
 */
Metadata.prototype.getAggregate = function (keyspaceName, name, signature, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getSingleFunctionCb(keyspaceName, name, signature, true, cb);
  });
};

/**
 * Gets the definition of a CQL materialized view for a given name.
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * <p>
 *   Note that, unlike the rest of the {@link Metadata} methods, this method does not cache the result for following
 *   calls, as the current version of the Cassandra native protocol does not support schema change events for
 *   materialized views. Each call to this method will produce one or more queries to the cluster.
 * </p>
 * @param {String} keyspaceName Name of the keyspace
 * @param {String} name Name of the materialized view
 * @param {Function} [callback] The callback with the err as a first parameter and the {@link MaterializedView} as
 * second parameter.
 */
Metadata.prototype.getMaterializedView = function (keyspaceName, name, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getMaterializedViewCb(keyspaceName, name, cb);
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getMaterializedViewCb = function (keyspaceName, name, callback) {
  var cache;
  if (this.options.isMetadataSyncEnabled) {
    var keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      return callback(null, null);
    }
    cache = keyspace.views;
  }
  this._schemaParser.getMaterializedView(keyspaceName, name, cache, callback);
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
  var cache;
  if (this.options.isMetadataSyncEnabled) {
    var keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      return callback(null, null);
    }
    cache = aggregate ? keyspace.aggregates : keyspace.functions;
  }
  this._schemaParser.getFunctions(keyspaceName, name, aggregate, cache, callback);
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
Metadata.prototype._getSingleFunctionCb = function (keyspaceName, name, signature, aggregate, callback) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError('Callback is not a function');
  }
  if (!keyspaceName || !name) {
    return callback(
      new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata'));
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
 * <p>
 *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
 *   Otherwise, it returns a <code>Promise</code>.
 * </p>
 * @param {Uuid} traceId Identifier of the trace session.
 * @param {Function} [callback] The callback with the err as first parameter and the query trace as second parameter.
 */
Metadata.prototype.getTrace = function (traceId, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._getTraceCb(traceId, cb);
  });
};

/**
 * @param {Uuid} traceId
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getTraceCb = function (traceId, callback) {
  var trace;
  var attempts = 0;
  var selectSession = util.format(_selectTraceSession, traceId);
  var selectEvents = util.format(_selectTraceEvents, traceId);
  var self = this;
  utils.whilst(function condition() {
    return !trace && (attempts++ < _traceMaxAttemps);
  }, function iterator(next) {
    self.controlConnection.query(selectSession, function (err, result) {
      if (err) {
        return next(err);
      }
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
        if (err) {
          return next(err);
        }
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
  utils.each(udts, function (type, next) {
    self.getUdt(type.info.keyspace, type.info.name, function (err, udtInfo) {
      if (err) {
        return next(err);
      }
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
        var schemaVersion = result.rows[i]['schema_version'];
        if (schemaVersion) {
          versions.push(schemaVersion);
        }
      }
    }
    callback(err, versions);
  });
};

module.exports = Metadata;
