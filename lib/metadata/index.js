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
const promiseUtils = require('../promise-utils');
const { TokenRange } = require('../token');
const { ExecutionOptions } = require('../execution-options');

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
const _traceAttemptDelay = 400;

/**
 * Represents cluster and schema information.
 * The metadata class acts as a internal state of the driver.
 */
class Metadata {

  /**
   * Creates a new instance of {@link Metadata}.
   * @param {ClientOptions} options
   * @param {ControlConnection} controlConnection Control connection used to retrieve information.
   */
  constructor(options, controlConnection) {
    if (!options) {
      throw new errors.ArgumentError('Options are not defined');
    }

    Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false });
    Object.defineProperty(this, 'controlConnection', { value: controlConnection, enumerable: false, writable: false });
    this.keyspaces = {};
    this.initialized = false;
    this._isDbaas = false;
    this._schemaParser = schemaParserFactory.getByVersion(options, controlConnection, this.getUdt.bind(this));
    this.log = utils.log;
    this._preparedQueries = new PreparedQueries(options.maxPrepared, (...args) => this.log(...args));
  }

  /**
   * Sets the cassandra version
   * @internal
   * @ignore
   * @param {Array.<Number>} version
   */
  setCassandraVersion(version) {
    this._schemaParser = schemaParserFactory.getByVersion(
      this.options, this.controlConnection, this.getUdt.bind(this), version, this._schemaParser);
  }

  /**
   * Determines whether the cluster is provided as a service.
   * @returns {boolean} true when the cluster is provided as a service (DataStax Astra), <code>false<code> when it's a
   * different deployment (on-prem).
   */
  isDbaas() {
    return this._isDbaas;
  }

  /**
   * Sets the product type as DBaaS.
   * @internal
   * @ignore
   */
  setProductTypeAsDbaas() {
    this._isDbaas = true;
  }

  /**
   * @ignore
   * @param {String} partitionerName
   */
  setPartitioner(partitionerName) {
    if (/RandomPartitioner$/.test(partitionerName)) {
      return this.tokenizer = new t.RandomTokenizer();
    }
    if (/ByteOrderedPartitioner$/.test(partitionerName)) {
      return this.tokenizer = new t.ByteOrderedTokenizer();
    }
    return this.tokenizer = new t.Murmur3Tokenizer();
  }

  /**
   * Populates the information regarding primary replica per token, datacenters (+ racks) and sorted token ring.
   * @ignore
   * @param {HostMap} hosts
   */
  buildTokens(hosts) {
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
    if (this.ring.length === 1) {
      // If there is only one token, return the range ]minToken, minToken]
      const min = this.tokenizer.minToken();
      tokenRanges.add(new TokenRange(min, min, this.tokenizer));
    }
    else {
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
  }

  /**
   * Gets the keyspace metadata information and updates the internal state of the driver.
   * <p>
   *   If a <code>callback</code> is provided, the callback is invoked when the keyspaces metadata refresh completes.
   *   Otherwise, it returns a <code>Promise</code>.
   * </p>
   * @param {String} name Name of the keyspace.
   * @param {Function} [callback] Optional callback.
   */
  refreshKeyspace(name, callback) {
    return promiseUtils.optionalCallback(this._refreshKeyspace(name), callback);
  }

  /**
   * @param {String} name
   * @private
   */
  async _refreshKeyspace(name) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }
    this.log('info', util.format('Retrieving keyspace %s metadata', name));
    try {
      const ksInfo = await this._schemaParser.getKeyspace(name);
      if (!ksInfo) {
        // the keyspace was dropped
        delete this.keyspaces[name];
        return null;
      }
      // Tokens are lazily init on the keyspace, once a replica from that keyspace is retrieved.
      this.keyspaces[ksInfo.name] = ksInfo;
      return ksInfo;
    }
    catch (err) {
      this.log('error', 'There was an error while trying to retrieve keyspace information', err);
      throw err;
    }
  }

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
  refreshKeyspaces(waitReconnect, callback) {
    if (typeof waitReconnect === 'function' || typeof waitReconnect === 'undefined') {
      callback = waitReconnect;
      waitReconnect = true;
    }
    if (!this.initialized) {
      const err = this._uninitializedError();
      if (callback) {
        return callback(err);
      }
      return Promise.reject(err);
    }
    return promiseUtils.optionalCallback(this.refreshKeyspacesInternal(waitReconnect), callback);
  }

  /**
   * @param {Boolean} waitReconnect
   * @returns {Promise<Object<string, Object>>}
   * @ignore
   * @internal
   */
  async refreshKeyspacesInternal(waitReconnect) {
    this.log('info', 'Retrieving keyspaces metadata');
    try {
      this.keyspaces = await this._schemaParser.getKeyspaces(waitReconnect);
      return this.keyspaces;
    }
    catch (err) {
      this.log('error', 'There was an error while trying to retrieve keyspaces information', err);
      throw err;
    }
  }

  _getKeyspaceReplicas(keyspace) {
    if (!keyspace.replicas) {
      //Calculate replicas the first time for the keyspace
      keyspace.replicas =
        keyspace.tokenToReplica(this.tokenizer, this.ringTokensAsStrings, this.primaryReplicas, this.datacenters);
    }
    return keyspace.replicas;
  }

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
  getReplicas(keyspaceName, token) {
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
  }

  /**
   * Gets the token ranges that define data distribution in the ring.
   *
   * @returns {Set<TokenRange>} The ranges of the ring or empty set if schema metadata is not enabled.
   */
  getTokenRanges() {
    return this.tokenRanges;
  }

  /**
   * Gets the token ranges that are replicated on the given host, for
   * the given keyspace.
   *
   * @param {String} keyspaceName The name of the keyspace to get ranges for.
   * @param {Host} host The host.
   * @returns {Set<TokenRange>|null} Ranges for the keyspace on this host or null if keyspace isn't found or hasn't been loaded.
   */
  getTokenRangesForHost(keyspaceName, host) {
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
  }

  /**
   * Constructs a Token from the input buffer(s) or string input.  If a string is passed in
   * it is assumed this matches the token representation reported by cassandra.
   * @param {Array<Buffer>|Buffer|String} components
   * @returns {Token} constructed token from the input buffer.
   */
  newToken(components) {
    if (!this.tokenizer) {
      throw new Error('Partitioner not established.  This should only happen if metadata was disabled or you have not connected yet.');
    }
    if (Array.isArray(components)) {
      return this.tokenizer.hash(Buffer.concat(components));
    }
    else if (typeof components === "string") {
      return this.tokenizer.parse(components);
    }
    return this.tokenizer.hash(components);
  }

  /**
   * Constructs a TokenRange from the given start and end tokens.
   * @param {Token} start
   * @param {Token} end
   * @returns TokenRange build range spanning from start (exclusive) to end (inclusive).
   */
  newTokenRange(start, end) {
    if (!this.tokenizer) {
      throw new Error('Partitioner not established.  This should only happen if metadata was disabled or you have not connected yet.');
    }
    return new TokenRange(start, end, this.tokenizer);
  }

  /**
   * Gets the metadata information already stored associated to a prepared statement
   * @param {String} keyspaceName
   * @param {String} query
   * @internal
   * @ignore
   */
  getPreparedInfo(keyspaceName, query) {
    return this._preparedQueries.getOrAdd(keyspaceName, query);
  }

  /**
   * Clears the internal state related to the prepared statements.
   * Following calls to the Client using the prepare flag will re-prepare the statements.
   */
  clearPrepared() {
    this._preparedQueries.clear();
  }

  /** @ignore */
  getPreparedById(id) {
    return this._preparedQueries.getById(id);
  }

  /** @ignore */
  setPreparedById(info) {
    return this._preparedQueries.setById(info);
  }

  /** @ignore */
  getAllPrepared() {
    return this._preparedQueries.getAll();
  }

  /** @ignore */
  _uninitializedError() {
    return new Error('Metadata has not been initialized.  This could only happen if you have not connected yet.');
  }

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
  getUdt(keyspaceName, name, callback) {
    return promiseUtils.optionalCallback(this._getUdt(keyspaceName, name), callback);
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @returns {Promise<Object|null>}
   * @private
   */
  async _getUdt(keyspaceName, name) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }
    let cache;
    if (this.options.isMetadataSyncEnabled) {
      const keyspace = this.keyspaces[keyspaceName];
      if (!keyspace) {
        return null;
      }
      cache = keyspace.udts;
    }
    return await this._schemaParser.getUdt(keyspaceName, name, cache);
  }

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
  getTable(keyspaceName, name, callback) {
    return promiseUtils.optionalCallback(this._getTable(keyspaceName, name), callback);
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @private
   */
  async _getTable(keyspaceName, name) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }
    let cache;
    let virtual;
    if (this.options.isMetadataSyncEnabled) {
      const keyspace = this.keyspaces[keyspaceName];
      if (!keyspace) {
        return null;
      }
      cache = keyspace.tables;
      virtual = keyspace.virtual;
    }
    return await this._schemaParser.getTable(keyspaceName, name, cache, virtual);
  }

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
  getFunctions(keyspaceName, name, callback) {
    return promiseUtils.optionalCallback(this._getFunctionsWrapper(keyspaceName, name), callback);
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @private
   */
  async _getFunctionsWrapper(keyspaceName, name) {
    if (!keyspaceName || !name) {
      throw new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata');
    }
    const functionsMap = await this._getFunctions(keyspaceName, name, false);
    return Array.from(functionsMap.values());
  }

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
  getFunction(keyspaceName, name, signature, callback) {
    return promiseUtils.optionalCallback(this._getSingleFunction(keyspaceName, name, signature, false), callback);
  }

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
  getAggregates(keyspaceName, name, callback) {
    return promiseUtils.optionalCallback(this._getAggregates(keyspaceName, name), callback);
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @private
   */
  async _getAggregates(keyspaceName, name) {
    if (!keyspaceName || !name) {
      throw new errors.ArgumentError('You must provide the keyspace name and cql aggregate name to retrieve the metadata');
    }
    const functionsMap = await this._getFunctions(keyspaceName, name, true);
    return Array.from(functionsMap.values());
  }

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
  getAggregate(keyspaceName, name, signature, callback) {
    return promiseUtils.optionalCallback(this._getSingleFunction(keyspaceName, name, signature, true), callback);
  }

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
  getMaterializedView(keyspaceName, name, callback) {
    return promiseUtils.optionalCallback(this._getMaterializedView(keyspaceName, name), callback);
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @returns {Promise<MaterializedView|null>}
   * @private
   */
  async _getMaterializedView(keyspaceName, name) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }
    let cache;
    if (this.options.isMetadataSyncEnabled) {
      const keyspace = this.keyspaces[keyspaceName];
      if (!keyspace) {
        return null;
      }
      cache = keyspace.views;
    }
    return await this._schemaParser.getMaterializedView(keyspaceName, name, cache);
  }

  /**
   * Gets a map of cql function definitions or aggregates based on signature.
   * @param {String} keyspaceName
   * @param {String} name Name of the function or aggregate
   * @param {Boolean} aggregate
   * @returns {Promise<Map>}
   * @private
   */
  async _getFunctions(keyspaceName, name, aggregate) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }
    let cache;
    if (this.options.isMetadataSyncEnabled) {
      const keyspace = this.keyspaces[keyspaceName];
      if (!keyspace) {
        return new Map();
      }
      cache = aggregate ? keyspace.aggregates : keyspace.functions;
    }
    return await this._schemaParser.getFunctions(keyspaceName, name, aggregate, cache);
  }

  /**
   * Gets a single cql function or aggregate definition
   * @param {String} keyspaceName
   * @param {String} name
   * @param {Array} signature
   * @param {Boolean} aggregate
   * @returns {Promise<SchemaFunction|Aggregate|null>}
   * @private
   */
  async _getSingleFunction(keyspaceName, name, signature, aggregate) {
    if (!keyspaceName || !name) {
      throw new errors.ArgumentError('You must provide the keyspace name and cql function name to retrieve the metadata');
    }
    if (!Array.isArray(signature)) {
      throw new errors.ArgumentError('Signature must be an array of types');
    }
    signature = signature.map(item => {
      if (typeof item === 'string') {
        return item;
      }
      return types.getDataTypeNameByCode(item);
    });
    const functionsMap = await this._getFunctions(keyspaceName, name, aggregate);
    return functionsMap.get(signature.join(',')) || null;
  }

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
  getTrace(traceId, consistency, callback) {
    if (!callback && typeof consistency === 'function') {
      // Both callback and consistency are optional parameters
      // In this case, the second parameter is the callback
      callback = consistency;
      consistency = null;
    }

    return promiseUtils.optionalCallback(this._getTrace(traceId, consistency), callback);
  }

  /**
   * @param {Uuid} traceId
   * @param {Number} consistency
   * @returns {Promise<Object>}
   * @private
   */
  async _getTrace(traceId, consistency) {
    if (!this.initialized) {
      throw this._uninitializedError();
    }

    let trace;
    let attempts = 0;
    const info = ExecutionOptions.empty();
    info.getConsistency = () => consistency;

    const sessionRequest = new requests.QueryRequest(util.format(_selectTraceSession, traceId), null, info);
    const eventsRequest = new requests.QueryRequest(util.format(_selectTraceEvents, traceId), null, info);

    while (!trace && (attempts++ < _traceMaxAttemps)) {
      const sessionResponse = await this.controlConnection.query(sessionRequest);
      const sessionRow = sessionResponse.rows[0];

      if (!sessionRow || typeof sessionRow['duration'] !== 'number') {
        await promiseUtils.delay(_traceAttemptDelay);
        continue;
      }

      trace = {
        requestType: sessionRow['request'],
        coordinator: sessionRow['coordinator'],
        parameters: sessionRow['parameters'],
        startedAt: sessionRow['started_at'],
        duration: sessionRow['duration'],
        clientAddress: sessionRow['client'],
        events: null
      };

      const eventsResponse = await this.controlConnection.query(eventsRequest);
      trace.events = eventsResponse.rows.map(row => ({
        id: row['event_id'],
        activity: row['activity'],
        source: row['source'],
        elapsed: row['source_elapsed'],
        thread: row['thread']
      }));
    }

    if (!trace) {
      throw new Error(`Trace ${traceId.toString()} could not fully retrieved after ${_traceMaxAttemps} attempts`);
    }

    return trace;
  }

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
  checkSchemaAgreement(callback) {
    return promiseUtils.optionalCallback(this._checkSchemaAgreement(), callback);
  }

  /**
   * Async-only version of check schema agreement.
   * @private
   */
  async _checkSchemaAgreement() {
    const connection = this.controlConnection.connection;
    if (!connection) {
      return false;
    }
    try {
      return await this.compareSchemaVersions(connection);
    }
    catch (err) {
      return false;
    }
  }

  /**
   * Uses the metadata to fill the user provided parameter hints
   * @param {String} keyspace
   * @param {Array} hints
   * @internal
   * @ignore
   */
  async adaptUserHints(keyspace, hints) {
    if (!Array.isArray(hints)) {
      return;
    }
    const udts = [];
    // Check for udts and get the metadata
    for (let i = 0; i < hints.length; i++) {
      const hint = hints[i];
      if (typeof hint !== 'string') {
        continue;
      }

      const type = types.dataTypes.getByName(hint);
      this._checkUdtTypes(udts, type, keyspace);
      hints[i] = type;
    }

    for (const type of udts) {
      const udtInfo = await this.getUdt(type.info.keyspace, type.info.name);
      if (!udtInfo) {
        throw new TypeError('User defined type not found: ' + type.info.keyspace + '.' + type.info.name);
      }
      type.info = udtInfo;
    }
  }

  /**
   * @param {Array} udts
   * @param {{code, info}} type
   * @param {string} keyspace
   * @private
   */
  _checkUdtTypes(udts, type, keyspace) {
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
      return this._checkUdtTypes(udts, type.info, keyspace);
    }
    if (type.code === types.dataTypes.map) {
      this._checkUdtTypes(udts, type.info[0], keyspace);
      this._checkUdtTypes(udts, type.info[1], keyspace);
    }
  }

  /**
   * Uses the provided connection to query the schema versions and compare them.
   * @param {Connection} connection
   * @internal
   * @ignore
   */
  async compareSchemaVersions(connection) {
    const versions = new Set();
    const response1 = await connection.send(new requests.QueryRequest(_selectSchemaVersionLocal), null);
    if (response1 && response1.rows && response1.rows.length === 1) {
      versions.add(response1.rows[0]['schema_version'].toString());
    }
    const response2 = await connection.send(new requests.QueryRequest(_selectSchemaVersionPeers), null);
    if (response2 && response2.rows) {
      for (const row of response2.rows) {
        const value = row['schema_version'];
        if (!value) {
          continue;
        }
        versions.add(value.toString());
      }
    }
    return versions.size === 1;
  }
}

/**
 * Allows to store prepared queries and retrieval by query or query id.
 * @ignore
 */
class PreparedQueries {

  /**
   * @param {Number} maxPrepared
   * @param {Function} logger
   */
  constructor(maxPrepared, logger) {
    this.length = 0;
    this._maxPrepared = maxPrepared;
    this._mapByKey = new Map();
    this._mapById = new Map();
    this._logger = logger;
  }

  _getKey(keyspace, query) {
    return (keyspace || '') + query;
  }

  getOrAdd(keyspace, query) {
    const key = this._getKey(keyspace, query);
    let info = this._mapByKey.get(key);
    if (info) {
      return info;
    }

    this._validateOverflow();

    info = new events.EventEmitter();
    info.setMaxListeners(0);
    info.query = query;
    // The keyspace in which it was prepared
    info.keyspace = keyspace;
    this._mapByKey.set(key, info);
    this.length++;
    return info;
  }

  _validateOverflow() {
    if (this.length < this._maxPrepared) {
      return;
    }

    const toRemove = [];
    this._logger('warning',
      'Prepared statements exceeded maximum. This could be caused by preparing queries that contain parameters');

    const toRemoveLength = this.length - this._maxPrepared + 1;

    for (const [key, info] of this._mapByKey) {
      if (!info.queryId) {
        // Only remove queries that contain queryId
        continue;
      }

      const length = toRemove.push([key, info]);
      if (length >= toRemoveLength) {
        break;
      }
    }

    for (const [key, info] of toRemove) {
      this._mapByKey.delete(key);
      this._mapById.delete(info.queryId.toString('hex'));
      this.length--;
    }
  }

  setById(info) {
    this._mapById.set(info.queryId.toString('hex'), info);
  }

  getById(id) {
    return this._mapById.get(id.toString('hex'));
  }

  clear() {
    this._mapByKey = new Map();
    this._mapById = new Map();
    this.length = 0;
  }

  getAll() {
    return Array.from(this._mapByKey.values()).filter(info => !!info.queryId);
  }
}

module.exports = Metadata;
