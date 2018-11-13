"use strict";
const events = require('events');
const util = require('util');
/**
 * Module containing classes and fields related to metadata.
 * @module metadata
 */
const t = require('../tokenizer');
const utils = require('../utils');
const errors = require('../errors');
const types = require('../types');
const requests = require('../requests');
const schemaParserFactory = require('./schema-parser');
const TokenRange = require('../token').TokenRange;
const ExecutionOptions = require('../execution-options').ExecutionOptions;

/**
 * @const
 * @private
 */
const _selectTraceSession = "SELECT * FROM system_traces.sessions WHERE session_id=%s";
/**
 * @const
 * @private
 */
const _selectTraceEvents = "SELECT * FROM system_traces.events WHERE session_id=%s";
/**
 * @const
 * @private
 */
const _selectSchemaVersionPeers = "SELECT schema_version FROM system.peers";
/**
 * @const
 * @private
 */
const _selectSchemaVersionLocal = "SELECT schema_version FROM system.local";
/**
 * @const
 * @private
 */
const _traceMaxAttemps = 5;
/**
 * @const
 * @private
 */
const _traceAttemptDelay = 200;

/**
 * Represents cluster and schema information.
 * The metadata class acts as a internal state of the driver.
 * @param {ClientOptions} options
 * @param {ControlConnection} controlConnection Control connection used to retrieve information.
 * @constructor
 */
function Metadata (options, controlConnection) {
  if (!options) {
    throw new errors.ArgumentError('Options are not defined');
  }
  Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false});
  Object.defineProperty(this, 'controlConnection', { value: controlConnection, enumerable: false, writable: false});
  this.keyspaces = {};
  this.initialized = false;
  this._schemaParser = schemaParserFactory.getByVersion(options, controlConnection, this.getUdt.bind(this));
  const self = this;
  this._preparedQueries = new PreparedQueries(options.maxPrepared, function () {
    self.log.apply(self, arguments);
  });
}

/**
 * Sets the cassandra version
 * @internal
 * @ignore
 * @param {Array.<Number>} version
 */
Metadata.prototype.setCassandraVersion = function (version) {
  this._schemaParser = schemaParserFactory.getByVersion(
    this.options, this.controlConnection, this.getUdt.bind(this), version, this._schemaParser);
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
  const allSorted = [];
  //Get a map of <token, primaryHost>
  const primaryReplicas = {};
  //Depending on the amount of tokens, this could be an expensive operation
  const hostArray = hosts.values();
  const stringify = this.tokenizer.stringify;
  const datacenters = {};
  hostArray.forEach((h) => {
    if (!h.tokens) {
      return;
    }
    h.tokens.forEach((tokenString) => {
      const token = this.tokenizer.parse(tokenString);
      utils.insertSorted(allSorted, token, (t1, t2) => t1.compare(t2));
      primaryReplicas[stringify(token)] = h;
    });
    let dc = datacenters[h.datacenter];
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

  // Build TokenRanges.
  const tokenRanges = new Set();
  if(this.ring.length === 1) {
    // If there is only one token, return the range ]minToken, minToken]
    const min = this.tokenizer.minToken();
    tokenRanges.add(new TokenRange(min, min, this.tokenizer));
  } else {
    for (let i = 0; i < this.ring.length; i++) {
      const start = this.ring[i];
      const end = this.ring[(i + 1) % this.ring.length];
      tokenRanges.add(new TokenRange(start, end, this.tokenizer));
    }
  }
  this.tokenRanges = tokenRanges;

  //Compute string versions as it's potentially expensive and frequently reused later
  this.ringTokensAsStrings = new Array(allSorted.length);
  for (let i = 0; i < allSorted.length; i++) {
    this.ringTokensAsStrings[i] = stringify(allSorted[i]);
  }
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    this._refreshKeyspaceCb(name, cb);
  });
};

/**
 * @param {String} name
 * @param {Function} callback
 * @private
 */
Metadata.prototype._refreshKeyspaceCb = function (name, callback) {
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  this.log('info', util.format('Retrieving keyspace %s metadata', name));
  const self = this;
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

  return this._refreshKeyspaces(waitReconnect, false, callback);
};

/**
 * @param {Boolean} waitReconnect
 * @param {Boolean} internal Whether or not this was called by driver (i.e. control connection)
 * @param {Function} [callback] 
 * @private
 */
Metadata.prototype._refreshKeyspaces = function (waitReconnect, internal, callback) {
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    this._refreshKeyspacesCb(waitReconnect, internal, cb);
  });
};

/**
 * @param {Boolean} waitReconnect
 * @param {Boolean} internal
 * @param {Function} callback
 * @private
 */
Metadata.prototype._refreshKeyspacesCb = function (waitReconnect, internal, callback) {
  if (!internal && !this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  this.log('info', 'Retrieving keyspaces metadata');
  const self = this;
  this._schemaParser.getKeyspaces(waitReconnect, function getKeyspacesCallback(err, keyspaces) {
    if (err) {
      self.log('error', 'There was an error while trying to retrieve keyspaces information', err);
      return callback(err);
    }
    self.keyspaces = keyspaces;
    callback(null, keyspaces);
  });
};

Metadata.prototype._getKeyspaceReplicas = function (keyspace) {
  if (!keyspace.replicas) {
    //Calculate replicas the first time for the keyspace
    keyspace.replicas =
      keyspace.tokenToReplica(this.tokenizer, this.ringTokensAsStrings, this.primaryReplicas, this.datacenters);
  }
  return keyspace.replicas;
};

/**
 * Gets the host list representing the replicas that contain the given partition key, token or token range.
 * <p>
 *   It uses the pre-loaded keyspace metadata to retrieve the replicas for a token for a given keyspace.
 *   When the keyspace metadata has not been loaded, it returns null.
 * </p>
 * @param {String} keyspaceName
 * @param {Buffer|Token|TokenRange} token Can be Buffer (serialized partition key), Token or TokenRange
 * @returns {Array}
 */
Metadata.prototype.getReplicas = function (keyspaceName, token) {
  if (!this.ring) {
    return null;
  }
  if (Buffer.isBuffer(token)) {
    token = this.tokenizer.hash(token);
  }
  if (token instanceof TokenRange) {
    token = token.end;
  }

  let keyspace;
  if (keyspaceName) {
    keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      // the keyspace was not found, the metadata should be loaded beforehand
      return null;
    }
  }
  let i = utils.binarySearch(this.ring, token, (t1, t2) => t1.compare(t2));
  if (i < 0) {
    i = ~i;
  }
  if (i >= this.ring.length) {
    //it circled back
    i = i % this.ring.length;
  }
  const closestToken = this.ringTokensAsStrings[i];

  if (!keyspaceName) {
    return [this.primaryReplicas[closestToken]];
  }

  const replicas = this._getKeyspaceReplicas(keyspace);
  return replicas[closestToken];
};

/**
 * Gets the token ranges that define data distribution in the ring.
 *
 * @returns {Set<TokenRange>} The ranges of the ring or empty set if schema metadata is not enabled.
 */
Metadata.prototype.getTokenRanges = function () {
  return this.tokenRanges;
};

/**
 * Gets the token ranges that are replicated on the given host, for
 * the given keyspace.
 *
 * @param {String} keyspaceName The name of the keyspace to get ranges for.
 * @param {Host} host The host.
 * @returns {Set<TokenRange>|null} Ranges for the keyspace on this host or null if keyspace isn't found or hasn't been loaded.
 */
Metadata.prototype.getTokenRangesForHost = function (keyspaceName, host) {
  if (!this.ring) {
    return null;
  }
  let keyspace;
  if (keyspaceName) {
    keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      // the keyspace was not found, the metadata should be loaded beforehand
      return null;
    }
  }

  // If the ring has only 1 token, just return the ranges as we should only have a single node cluster.
  if (this.ring.length === 1) {
    return this.getTokenRanges();
  }

  const replicas = this._getKeyspaceReplicas(keyspace);
  const ranges = new Set();

  // for each range, find replicas for end token, if replicas include host, add range.
  this.tokenRanges.forEach((tokenRange) => {
    const replicasForToken = replicas[this.tokenizer.stringify(tokenRange.end)];
    if (replicasForToken.indexOf(host) !== -1) {
      ranges.add(tokenRange);
    }
  });
  return ranges;
};

/**
 * Constructs a Token from the input buffer(s) or string input.  If a string is passed in
 * it is assumed this matches the token representation reported by cassandra.
 * @param {Array<Buffer>|Buffer|String} components
 * @returns {Token} constructed token from the input buffer.
 */
Metadata.prototype.newToken = function (components) {
  if (!this.tokenizer) {
    throw new Error('Partitioner not established.  This should only happen if metadata was disabled or you have not connected yet.');
  }
  if (util.isArray(components)) {
    return this.tokenizer.hash(Buffer.concat(components));
  } else if (util.isString(components)) {
    return this.tokenizer.parse(components);
  }
  return this.tokenizer.hash(components);
};

/**
 * Constructs a TokenRange from the given start and end tokens.
 * @param {Token} start 
 * @param {Token} end 
 * @returns TokenRange build range spanning from start (exclusive) to end (inclusive).
 */
Metadata.prototype.newTokenRange = function(start, end) {
  if (!this.tokenizer) {
    throw new Error('Partitioner not established.  This should only happen if metadata was disabled or you have not connected yet.');
  }
  return new TokenRange(start, end, this.tokenizer);
};

Metadata.prototype.log = utils.log;

/**
 * Gets the metadata information already stored associated to a prepared statement
 * @param {String} keyspaceName
 * @param {String} query
 * @internal
 * @ignore
 */
Metadata.prototype.getPreparedInfo = function (keyspaceName, query) {
  //overflow protection
  return this._preparedQueries.getOrAdd(keyspaceName, query);
};

/**
 * Clears the internal state related to the prepared statements.
 * Following calls to the Client using the prepare flag will re-prepare the statements.
 */
Metadata.prototype.clearPrepared = function () {
  this._preparedQueries.clear();
};

/** @ignore */
Metadata.prototype.getPreparedById = function (id) {
  return this._preparedQueries.getById(id);
};

/** @ignore */
Metadata.prototype.setPreparedById = function (info) {
  return this._preparedQueries.setById(info);
};

/** @ignore */
Metadata.prototype.getAllPrepared = function () {
  return this._preparedQueries.getAll();
};

/** @ignore */
Metadata.prototype._uninitializedError = function () {
  return new Error('Metadata has not been initialized.  This could only happen if you have not connected yet.');
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  let cache;
  if (this.options.isMetadataSyncEnabled) {
    const keyspace = this.keyspaces[keyspaceName];
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  let cache;
  let virtual;
  if (this.options.isMetadataSyncEnabled) {
    const keyspace = this.keyspaces[keyspaceName];
    if (!keyspace) {
      return callback(null, null);
    }
    cache = keyspace.tables;
    virtual = keyspace.virtual;
  }
  this._schemaParser.getTable(keyspaceName, name, cache, virtual, callback);
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
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
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  let cache;
  if (this.options.isMetadataSyncEnabled) {
    const keyspace = this.keyspaces[keyspaceName];
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
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  let cache;
  if (this.options.isMetadataSyncEnabled) {
    const keyspace = this.keyspaces[keyspaceName];
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
    let f;
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
 * @param {Number} [consistency] The consistency level to obtain the trace.
 * @param {Function} [callback] The callback with the err as first parameter and the query trace as second parameter.
 */
Metadata.prototype.getTrace = function (traceId, consistency, callback) {
  if (!callback && typeof consistency === 'function') {
    // Both callback and consistency are optional parameters
    // In this case, the second parameter is the callback
    callback = consistency;
    consistency = null;
  }
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    this._getTraceCb(traceId, consistency, cb);
  });
};

/**
 * @param {Uuid} traceId
 * @param {Number} consistency
 * @param {Function} callback
 * @private
 */
Metadata.prototype._getTraceCb = function (traceId, consistency, callback) {
  if (!this.initialized) {
    return callback(this._uninitializedError(), null);
  }
  let trace;
  let attempts = 0;
  const info = ExecutionOptions.empty();
  info.getConsistency = () => consistency;
  const sessionRequest = new requests.QueryRequest(util.format(_selectTraceSession, traceId), null, info);
  const eventsRequest = new requests.QueryRequest(util.format(_selectTraceEvents, traceId), null, info);
  const self = this;

  utils.whilst(function condition() {
    return !trace && (attempts++ < _traceMaxAttemps);
  }, function iterator(next) {
    self.controlConnection.query(sessionRequest, function (err, result) {
      if (err) {
        return next(err);
      }
      const sessionRow = result.rows[0];
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
      self.controlConnection.query(eventsRequest, function (err, result) {
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
 * Checks whether hosts that are currently up agree on the schema definition.
 * <p>
 *   This method performs a one-time check only, without any form of retry; therefore
 *   <code>protocolOptions.maxSchemaAgreementWaitSeconds</code> setting does not apply in this case.
 * </p>
 * @param {Function} [callback] A function that is invoked with a value
 * <code>true</code> when all hosts agree on the schema and <code>false</code> when there is no agreement or when
 * the check could not be performed (for example, if the control connection is down).
 * @returns {Promise} Returns a <code>Promise</code> when a callback is not provided. The promise resolves to
 * <code>true</code> when all hosts agree on the schema and <code>false</code> when there is no agreement or when
 * the check could not be performed (for example, if the control connection is down).
 */
Metadata.prototype.checkSchemaAgreement = function (callback) {
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    const connection = this.controlConnection.connection;
    if (!connection) {
      return cb(null, false);
    }

    this.compareSchemaVersions(connection, (err, agreement) => {
      // The error is never thrown
      cb(null, !err && agreement);
    });
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
  if (!util.isArray(hints)) {
    return callback();
  }
  const udts = [];
  //check for udts and get the metadata
  function checkUdtTypes(type) {
    if (type.code === types.dataTypes.udt) {
      const udtName = type.info.split('.');
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
  for (let i = 0; i < hints.length; i++) {
    const hint = hints[i];
    if (typeof hint !== 'string') {
      continue;
    }
    try {
      const type = types.dataTypes.getByName(hint);
      checkUdtTypes(type);
      hints[i] = type;
    }
    catch (err) {
      return callback(err);
    }
  }
  const self = this;
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
 * Uses the provided connection to query the schema versions and compare them.
 * @param {Connection} connection
 * @param {Function} callback
 * @internal
 * @ignore
 */
Metadata.prototype.compareSchemaVersions = function (connection, callback) {
  const versions = new Set();

  utils.series([
    next => {
      const request = new requests.QueryRequest(_selectSchemaVersionLocal, null, null);
      connection.sendStream(request, null, function (err, result) {
        if (!err && result && result.rows && result.rows.length === 1) {
          versions.add(result.rows[0]['schema_version'].toString());
        }
        next(err);
      });
    },
    next => {
      const request = new requests.QueryRequest(_selectSchemaVersionPeers, null, null);
      connection.sendStream(request, null, function (err, result) {
        if (!err && result && result.rows) {
          for (const row of result.rows) {
            const value = row['schema_version'];
            if (!value) {
              continue;
            }
            versions.add(value.toString());
          }
        }
        next(err);
      });
    }
  ], (err) => {
    callback(err, versions.size === 1);
  });
};

/**
 * Allows to store prepared queries and retrieval by query or query id.
 * @param {Number} maxPrepared
 * @param {Function} logger
 * @constructor
 * @ignore
 */
function PreparedQueries(maxPrepared, logger) {
  this.length = 0;
  this._maxPrepared = maxPrepared;
  this._mapByKey = {};
  this._mapById = {};
  this._logger = logger;
}

PreparedQueries.prototype._getKey = function (keyspace, query) {
  return ( keyspace || '' ) + query;
};

PreparedQueries.prototype.getOrAdd = function (keyspace, query) {
  const key = this._getKey(keyspace, query);
  let info = this._mapByKey[key];
  if (info) {
    return info;
  }
  this._validateOverflow();
  info = new events.EventEmitter();
  info.setMaxListeners(0);
  info.query = query;
  // The keyspace in which it was prepared
  info.keyspace = keyspace;
  this._mapByKey[key] = info;
  this.length++;
  return info;
};

PreparedQueries.prototype._validateOverflow = function () {
  if (this.length < this._maxPrepared) {
    return;
  }
  const toRemove = [];
  this._logger('warning',
    'Prepared statements exceeded maximum. This could be caused by preparing queries that contain parameters');
  const existingKeys = Object.keys(this._mapByKey);
  for (let i = 0; i < existingKeys.length && this.length - toRemove.length < this._maxPrepared; i++) {
    const info = this._mapByKey[existingKeys[i]];
    if (!info.queryId) {
      // Only remove queries that contain queryId
      continue;
    }
    toRemove.push(info);
  }
  toRemove.forEach(function (item) {
    delete this._mapByKey[item.query];
    delete this._mapById[item.queryId];
    this.length--;
  }, this);
};

PreparedQueries.prototype.setById = function (info) {
  this._mapById[info.queryId.toString('hex')] = info;
};

PreparedQueries.prototype.getById = function (id) {
  return this._mapById[id.toString('hex')];
};

PreparedQueries.prototype.clear = function () {
  this._mapByKey = {};
  this._mapById = {};
  this.length = 0;
};

PreparedQueries.prototype.getAll = function () {
  return utils.objectValues(this._mapByKey).filter(function (info) {
    return !!info.queryId;
  });
};

module.exports = Metadata;
