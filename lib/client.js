"use strict";
var events = require('events');
var util = require('util');
var async = require('async');

var utils = require('./utils.js');
var errors = require('./errors.js');
var types = require('./types');
var ControlConnection = require('./control-connection');
var RequestHandler = require('./request-handler');
var requests = require('./requests');
var clientOptions = require('./client-options');

/**
 * Client options
 * @typedef {Object} ClientOptions
 * @property {Array.<string>} contactPoints
 * Array of addresses or host names of the nodes to add as contact points.
 * <p>
 *  Contact points are addresses of Cassandra nodes that the driver uses to discover the cluster topology.
 * </p>
 * <p>
 *  Only one contact point is required (the driver will retrieve the address of the other nodes automatically),
 *  but it is usually a good idea to provide more than one contact point, because if that single contact point is unavailable, the driver will not be able to initialize correctly.
 * </p>
 * @property {Object} policies
 * @property {LoadBalancingPolicy} policies.loadBalancing The load balancing policy instance to be used to determine the coordinator per query.
 * @property {RetryPolicy} policies.retry The retry policy.
 * @property {ReconnectionPolicy} policies.reconnection The reconnection policy to be used.
 * @property {AddressTranslator} policies.addressResolution The address resolution policy.
 * @property {QueryOptions} queryOptions Default options for all queries.
 * @property {Object} pooling Pooling options.
 * @property {Number} pooling.heartBeatInterval The amount of idle time in milliseconds that has to pass before the driver issues a request on an active connection to avoid idle time disconnections. Default: 30000.
 * @property {Object} pooling.coreConnectionsPerHost Associative array containing amount of connections per host distance.
 * @property {Object} protocolOptions
 * @property {Number} protocolOptions.port The port to use to connect to the Cassandra host. If not set through this method, the default port (9042) will be used instead.
 * @property {Number} protocolOptions.maxSchemaAgreementWaitSeconds The maximum time in seconds to wait for schema agreement between nodes before returning from a DDL query. Default: 10.
 * @property {Number} protocolOptions.maxVersion When set, it limits the maximum protocol version used to connect to the nodes.
 * Useful for using the driver against a cluster that contains nodes with different major/minor versions of Cassandra.
 * @property {Object} socketOptions
 * @property {Number} socketOptions.connectTimeout Connection timeout in milliseconds. Default: 5000.
 * @property {Boolean} socketOptions.keepAlive Whether to enable TCP keep-alive on the socket. Default: true.
 * @property {Number} socketOptions.keepAliveDelay TCP keep-alive delay in milliseconds. Default: 0.
 * @property {Boolean} socketOptions.tcpNoDelay When set to true, it disables the Nagle algorithm. Default: true.
 * @property {AuthProvider} authProvider Provider to be used to authenticate to an auth-enabled cluster.
 * @property {Object} sslOptions Client-to-node ssl options, when set the driver will use the secure layer.
 * You can specify cert, ca, ... options named after the Node.js tls.connect options.
 * @property {Object} encoding
 * @property {Function} encoding.map Map constructor to use for Cassandra map<k,v> type encoding and decoding.
 * If not set, it will default to Javascript Object with map keys as property names.
 * @property {Function} encoding.set Set constructor to use for Cassandra set<k> type encoding and decoding.
 * If not set, it will default to Javascript Array.
 * @property {Boolean} encoding.copyBuffer Determines if the network buffer should be copied for buffer based data types (blob, uuid, timeuuid and inet).
 * <p>
 *   Setting it to true will cause that the network buffer is copied for each row value of those types,
 *   causing additional allocations but freeing the network buffer to be reused.
 *   Setting it to true is a good choice for cases where the Row and ResultSet returned by the queries are long-lived objects.
 * </p>
 * <p>
 *  Setting it to false will cause less overhead and the reference of the network buffer to be maintained until the row / result set are de-referenced.
 *  Default: true.
 * </p>
 * @property {Boolean} encoding.useUndefinedAsUnset Valid for Cassandra 2.2 and above. Determines that, if a parameter is set to
 * <code>undefined</code> it should be encoded as <code>unset</code>.
 * <p>
 *  By default, ECMAScript <code>undefined</code> is encoded as <code>null</code> in the driver. Cassandra 2.2 introduced the concept of unset.
 *  At driver level, you can set a parameter to unset using the field <code>types.unset</code>. Setting this flag to true
 *  allows you to use ECMAScript undefined as Cassandra <code>unset</code>.
 * </p>
 * <p>
 *   Default: false.
 * </p>
 */

/**
 * Query options
 * @typedef {Object} QueryOptions
 * @property {Number} consistency Consistency level.
 * @property {Number} fetchSize Amount of rows to retrieve per page.
 * @property {Boolean} prepare Determines if the query must be executed as a prepared statement.
 * @property {Boolean} autoPage Determines if the driver must retrieve the next pages.
 * @property {Buffer|Array} routingKey Partition key(s) to determine which coordinator should be used for the query.
 * @property {Array} routingIndexes Index of the parameters that are part of the partition key to determine the routing.
 * @property {Array} routingNames Array of the parameters names that are part of the partition key to determine the routing.
 * @property {Array|Array<Array>} hints Type hints for parameters given in the query, ordered as for the parameters.
 * <p>
 *   For batch queries, an array of such arrays, ordered as with the queries in the batch.
 * </p>
 * @property {Buffer|String} pageState Buffer or string token representing the paging state.
 * <p>
 *   Useful for manual paging, if provided, the query will be executed starting from a given paging state.
 * </p>
 * @property {RetryPolicy} retry Retry policy for the query.
 * <p>
 *   This property can be used to specify a different [retry policy]{@link module:policies/retry} to the one specified in the {@link ClientOptions}.policies.
 * </p>
 * @property {Number|Long} timestamp the default timestamp for the query in microseconds from the unix epoch (00:00:00, January 1st, 1970).
 * If provided, this will replace the server side assigned timestamp as default timestamp.
 * @property {Number} serialConsistency Serial consistency is the consistency level for the serial phase of conditional updates.
 * This option will be ignored for anything else that a conditional update/insert.
 * @property {Boolean} traceQuery Enable query tracing for the execution. Use query tracing to diagnose performance problems related to query executions. Default: false.
 * <p>
 *  To retrieve trace, you can call [Metadata.getTrace()]{@link Metadata} method.
 * </p>
 */

/**
 * A Client holds connections to a Cassandra cluster, allowing it to be queried.
 * Each Client instance maintains multiple connections to the cluster nodes,
 * provides [policies]{@link module:policies} to choose which node to use for each query,
 * and handles [retries]{@link module:policies/retry} for failed query (when it makes sense), etc...
 * <p>
 * Client instances are designed to be long-lived and usually a single instance is enough
 * per application. As a given Client can only be "logged" into one keyspace at
 * a time (where the "logged" keyspace is the one used by query if the query doesn't
 * explicitly use a fully qualified table name), it can make sense to create one
 * client per keyspace used. This is however not necessary to query multiple keyspaces
 * since it is always possible to use a single session with fully qualified table name
 * in queries.
 * </p>
 * @param {ClientOptions} options The options for this instance.
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  this.options = clientOptions.extend({ logEmitter: this.emit.bind(this) }, options);
  this.controlConnection = new ControlConnection(this.options);
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.connected = false;
  /**
   * Gets the name of the active keyspace.
   * @type {String}
   */
  this.keyspace = options.keyspace;
  /**
   * Gets the schema and cluster metadata information.
   * @type {Metadata}
   */
  this.metadata = this.controlConnection.metadata;
  /**
   * Gets an associative array of cluster hosts.
   * @type {HostMap}
   */
  this.hosts = null;
}

util.inherits(Client, events.EventEmitter);

/**
 * Tries to connect to one of the [contactPoints]{@link ClientOptions} and discover the nodes of the cluster.
 * <p>
 *   If the {@link Client} is already connected, it immediately invokes callback.
 * </p>
 * @param {function} callback The callback is invoked when the pool is connected
 *  (or at least 1 connected and the rest failed to connect) or it is not possible to connect
 */
Client.prototype.connect = function (callback) {
  if (this.connected) return callback();
  if (this.connecting) {
    //add a listener and move on
    return this.once('connected', callback);
  }
  this.connecting = true;
  var self = this;
  this.controlConnection.init(function (err) {
    if (err) return connectCallback(err);
    //we have all the data from the cluster
    self.hosts = self.controlConnection.hosts;
    self.options.policies.loadBalancing.init(self, self.hosts, function (err) {
      if (err) return connectCallback(err);
      if (self.keyspace) {
        return self._setKeyspace(connectCallback);
      }
      connectCallback();
    });
  });
  function connectCallback(err) {
    self.connected = !err;
    self.connecting = false;
    if (!err) {
      //Set the pooling options depending on the protocol version
      var coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV3;
      if (self.controlConnection.protocolVersion < 3) {
        coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV2;
      }
      self.options.pooling = utils.deepExtend({}, {coreConnectionsPerHost: coreConnectionsPerHost}, self.options.pooling);
    }
    try{
      callback(err);
    }
    finally {
      self.emit('connected', err);
    }
  }
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Executes a query on an available connection.
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple hosts if needed.
 * </p>
 * @param {String} query The query to execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names as keys and its value
 * @param {QueryOptions} [options]
 * @param {ResultCallback} callback Executes callback(err, result) when finished
 */
Client.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  args.options = utils.extend({}, this.options.queryOptions, args.options);
  args.callback = utils.bindDomain(args.callback);
  this._innerExecute(args.query, args.params, args.options, args.callback);
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Executes the query and calls rowCallback for each row as soon as they are received.
 *  Calls final callback after all rows have been sent, or when there is an error.
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple hosts if needed.
 * </p>
 * @param {String} query The query to execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names as keys and its value
 * @param {QueryOptions} [options]
 * @param {function} rowCallback Executes rowCallback(n, row) per each row received, where n is the row index and row is the current Row.
 * @param {function} [callback] Executes callback(err, totalCount) after all rows have been received.
 */
Client.prototype.eachRow = function () {
  var args = Array.prototype.slice.call(arguments);
  var rowCallback;
  //accepts an extra callback
  if(typeof args[args.length-1] === 'function' && typeof args[args.length-2] === 'function') {
    //pass it through the options parameter
    rowCallback = args.splice(args.length-2, 1)[0];
  }
  args = utils.parseCommonArgs.apply(null, args);
  if (!rowCallback) {
    //only one callback has been defined
    rowCallback = args.callback;
    args.callback = function () {};
  }
  args.options = utils.extend({}, this.options.queryOptions, args.options, {
    byRow: true,
    rowCallback: utils.bindDomain(rowCallback)
  });
  args.callback = utils.bindDomain(args.callback);
  var self = this;
  function pageCallback (err, result) {
    if (err) {
      return args.callback(err);
    }
    if (args.options.autoPage) {
      //Next requests for auto-paging
      args.options.rowLength = args.options.rowLength || 0;
      args.options.rowLength += result.rowLength;
      args.options.rowLengthArray = args.options.rowLengthArray || [];
      args.options.rowLengthArray.push(result.rowLength);

      if (result.meta && result.meta.pageState) {
        //Use new page state as next request page state
        args.options.pageState = result.meta.pageState;
        //Issue next request for the next page
        self._innerExecute(args.query, args.params, args.options, pageCallback);
        return;
      }
      //finished auto-paging
      result.rowLength = args.options.rowLength;
      result.rowLengthArray = args.options.rowLengthArray;
    }
    args.callback(null, result);
  }
  this._innerExecute(args.query, args.params, args.options, pageCallback);
};


//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Executes the query and pushes the rows to the result stream
 *  as soon as they received.
 * Calls callback after all rows have been sent, or when there is an error.
 * <p>
 * The stream is a [Readable Streams2]{@link http://nodejs.org/api/stream.html#stream_class_stream_readable} object
 *  that contains the raw bytes of the field value.
 *  It can be piped downstream and provides automatic pause/resume logic (it buffers when not read).
 * </p>
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple hosts if needed.
 * </p>
 * @param {String} query The query to prepare and execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names as keys and its value
 * @param {QueryOptions} [options]
 * @param {function} [callback], executes callback(err) after all rows have been received or if there is an error
 * @returns {exports.ResultStream}
 */
Client.prototype.stream = function () {
  var args = Array.prototype.slice.call(arguments);
  if (typeof args[args.length-1] !== 'function') {
    //the callback is not required
    args.push(function noop() {});
  }
  args = utils.parseCommonArgs.apply(null, args);
  var resultStream = new types.ResultStream({objectMode: 1});
  this.eachRow(args.query, args.params, args.options, function rowCallback(n, row) {
    resultStream.add(row);
  }, function (err) {
    if (err) {
      resultStream.emit('error', err);
    }
    resultStream.add(null);
    args.callback(err);
  });
  return resultStream;
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Executes batch of queries on an available connection to a host.
 * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array of object containing the query and params
 * @param {QueryOptions} [options]
 * @param {ResultCallback} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function () {
  var args = this._parseBatchArgs.apply(null, arguments);
  //logged batch by default
  args.options = utils.extend({logged: true}, this.options.queryOptions, args.options);
  args.callback = utils.bindDomain(args.callback);
  var self = this;
  this.connect(function afterConnect(err) {
    if (err) {
      return args.callback(err);
    }
    if (args.options.prepare) {
      //Batch of prepared statements
      return self._batchPrepared(args.queries, args.options, args.callback);
    }
    //Batch of simple statements
    self._sendBatch(args.queries, args.options, args.callback);
  });
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * @param {String} keyspace
 * @param {Buffer} token
 * @returns {Array}
 */
Client.prototype.getReplicas = function (keyspace, token) {
  return this.metadata.getReplicas(keyspace, token);
};

Client.prototype.log = utils.log;

/**
 * Closes all connections to all hosts
 * @param {Function} [callback]
 */
Client.prototype.shutdown = function (callback) {
  //Go through all the host and shut down their pools
  this.log('info', 'Shutting down');
  if (!callback) {
    callback = function() {};
  }
  if (!this.hosts) {
    // not sure if could be true if there are no hosts
    this.connected = false;
    return callback();
  }
  var self = this;
  var hosts = this.hosts.values();
  async.each(hosts, function (h, next) {
    h.shutdown(next);
  }, function(err) {
    self.connected = !!err;
    callback(err);
  });
};

/**
 * Waits until that the schema version in all nodes is the same or the waiting time passed.
 * @param {Function} callback
 * @ignore
 */
Client.prototype.waitForSchemaAgreement = function (callback) {
  if (this.hosts.length === 1) {
    return setImmediate(callback);
  }
  var self = this;
  var start = new Date();
  var maxWaitTime = this.options.protocolOptions.maxSchemaAgreementWaitSeconds * 1000;
  this.log('info', 'Waiting for schema agreement');
  var versionsMatch;
  async.doWhilst(function (next) {
    async.series([
      self.controlConnection.getPeersSchemaVersions.bind(self.controlConnection),
      self.controlConnection.getLocalSchemaVersion.bind(self.controlConnection)
    ], function (err, results) {
      if (err) return next(err);
      //check the different versions
      versionsMatch = true;
      var localVersion = results[1].toString();
      var peerVersions = results[0];
      for (var i = 0; i < peerVersions.length; i++) {
        if (peerVersions[i].toString() !== localVersion) {
          versionsMatch = false;
          break;
        }
      }
      if (versionsMatch) {
        self.log('info', 'Schema versions match');
      }
      //let some time pass before the next check
      setTimeout(next, 500);
    });
  }, function () {
    return !versionsMatch && (new Date() - start) < maxWaitTime;
  }, callback);
};

/**
 * Connects and handles the execution of prepared and simple statements. All parameters are mandatory.
 * @param {string} query
 * @param {Array} params
 * @param {Object} options Options, contained already all the required QueryOptions.
 * @param {Function} callback
 * @private
 */
Client.prototype._innerExecute = function (query, params, options, callback) {
  if (options.prepare) {
    return this._executeAsPrepared(query, params, options, callback);
  }
  var self = this;
  async.waterfall([
    function connecting(next) {
      self.connect(next);
    },
    function settingOptions(next) {
      self._setQueryOptions(options, params, null, next);
    },
    function sendingQuery(params, next) {
      var request = new requests.QueryRequest(
        query,
        params,
        options);
      var handler = new RequestHandler(self, self.options);
      handler.send(request, options, next);
    }
  ], callback);
};

/**
 * Prepares (the first time) and executes the prepared query, retrying on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array|Object} params Array of params or params object with the name as keys
 * @param {Object} options
 * @param {ResultCallback} callback Executes callback(err, result) when finished
 * @private
 */
Client.prototype._executeAsPrepared = function (query, params, options, callback) {
  var self = this;
  async.waterfall([
    function connecting(next) {
      self.connect(next);
    },
    function preparing(next) {
      self._getPrepared(query, next);
    },
    function settingOptions(queryId, meta, next) {
      self._setQueryOptions(options, params, meta, function (err, params) {
        next(err, queryId, params)
      });
    },
    function sendingExecute(queryId, params, next) {
      var request = new requests.ExecuteRequest(
        queryId,
        params,
        options);
      request.query = query;
      var handler = new RequestHandler(self, self.options);
      handler.send(request, options, next);
    }
  ], callback);
};

/**
 * Prepares the queries and then executes the batch.
 * @param {Array.<{query, params}>} queries Array of object instances containing query and params properties.
 * @param {QueryOptions} options
 * @param {ResultCallback} callback Executes callback(err, result) when the batch was executed
 * @private
 */
Client.prototype._batchPrepared = function (queries, options, callback) {
  var self = this;
  queries = queries.map(function (item) {
    return { info: self.metadata.getPreparedInfo(item.query), query: item.query, params: item.params};
  });
  //Identify the query that are being prepared and wait for it
  this._waitForPendingPrepares(queries, function afterWait(err, toPrepare) {
    if (err) return callback(err);
    var queriesToPrepare = Object.keys(toPrepare);
    if (queriesToPrepare.length === 0) {
      //The ones that were being prepared are now prepared
      return self._sendBatch(queries, options, callback);
    }
    //Prepare the pending
    var callbacksArray = new Array(queriesToPrepare.length);
    queriesToPrepare.forEach(function (query, i) {
      var info = toPrepare[query];
      info.preparing = true;
      callbacksArray[i] = function prepareCallback(err, response) {
        info.preparing = false;
        if (err) {
          return info.emit('prepared', err);
        }
        info.queryId = response.id;
        info.meta = response.meta;
        info.emit('prepared', null, info.queryId, info.meta);
      };
    });
    var handler = new RequestHandler(self, self.options);
    //Prepare the queries that are not already prepared on a single host
    handler.prepareMultiple(queriesToPrepare, callbacksArray, options, function (err) {
      if (err) return callback(err);
      return self._sendBatch(queries, options, callback);
    });
  });
};

/** @private */
Client.prototype._sendBatch = function (queries, options, callback) {
  var request = new requests.BatchRequest(queries, options);
  var handler = new RequestHandler(this, this.options);
  handler.send(request, options, callback);
};

/**
 * Waits for all pending prepared queries to be prepared and callbacks with the queries to prepare
 * @param {Array} queries
 * @param {Function} callback
 * @private
 */
Client.prototype._waitForPendingPrepares = function (queries, callback) {
  function doWait(queriesMap) {
    var toPrepare = {};
    var pendingIO = false;
    async.each(Object.keys(queriesMap), function waitIterator(query, next) {
      var info = queriesMap[query];
      if (info.queryId) {
        //Its already prepared
        return next();
      }
      if (info.preparing) {
        //it is already being prepared
        pendingIO = true;
        return info.once('prepared', next);
      }
      toPrepare[query] = info;
      next();
    }, function waitFinished(err) {
      if (err) {
        //There was an error with the queries being prepared
        return callback(err);
      }
      if (pendingIO) {
        //There was IO between the last call
        //it is possible that queries marked to prepare are being prepared
        //iterate again until we have the filtered list of items to prepare
        return setImmediate(function pendingIOCallback() {
          doWait(toPrepare);
        });
      }
      callback(null, toPrepare);
    });
  }
  var queriesMap = {};
  queries.forEach(function (item) {
    queriesMap[item.query] = item.info;
  });
  doWait(queriesMap);
};

/**
 * Parses and validates the arguments received by executeBatch
 * @private
 */
Client.prototype._parseBatchArgs = function (queries, options, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (args.length < 2 || typeof args[args.length-1] !== 'function') {
    throw new errors.ArgumentError('It should contain at least 2 arguments, with the callback as the last argument.');
  }
  if (!util.isArray(queries)) {
    throw new errors.ArgumentError('The first argument must be an Array of queries.');
  }
  if (args.length < 3) {
    callback = args[args.length-1];
    options = null;
  }
  if (queries.length === 0) {
    throw new errors.ArgumentError('The Array of queries to batch execute can not be empty');
  }
  var parsedQueries = new Array(queries.length);
  for (var i = 0; i < queries.length; i++) {
    var item = queries[i];
    if (!item) {
      throw new errors.ArgumentError(util.format('Invalid query at index %d', i));
    }
    var query = item.query;
    if (typeof item === 'string') {
      query = item;
    }
    if (!query) {
      throw new errors.ArgumentError(util.format('Invalid query at index %d', i));
    }
    parsedQueries[i] = { query: query, params: item.params};
  }
  args.queries = parsedQueries;
  args.options = options;
  args.callback = callback;
  return args;
};

/**
 * It returns the id of the prepared query.
 * If its not prepared, it prepares the query.
 * If its being prepared, it queues the callback
 * @param {String} query Query to prepare with ? or :param_name as parameter placeholders
 * @param {function} callback Executes callback(err, queryId) when there is a prepared statement on a connection or there is an error.
 * @private
 */
Client.prototype._getPrepared = function (query, callback) {
  var info = this.metadata.getPreparedInfo(query);
  if (info.queryId) {
    return callback(null, info.queryId, info.meta);
  }
  info.once('prepared', callback);
  if (info.preparing) {
    //it is already being prepared
    return;
  }
  info.preparing = true;
  var request = new requests.PrepareRequest(query);
  var handler = new RequestHandler(this, this.options);
  handler.send(request, null, function (err, result) {
    info.preparing = false;
    if (err) {
      err.query = query;
      return info.emit('prepared', err);
    }
    info.queryId = result.id;
    info.meta = result.meta;
    info.emit('prepared', null, info.queryId, info.meta);
  });
};

/**
 * Sets the keyspace in a connection that is already opened.
 * @param {Function} callback
 * @private
 */
Client.prototype._setKeyspace = function (callback) {
  var handler = new RequestHandler(this, this.options);
  handler.setKeyspace(callback);
};

/**
 * @returns {Encoder}
 * @private
 */
Client.prototype._getEncoder = function () {
  var encoder;
  encoder = this.controlConnection.getEncoder();
  if (!encoder) {
    throw new errors.DriverInternalError('Encoder is not defined');
  }
  return encoder;
};

/**
 * Validates the values and sets the default values for the {@link QueryOptions} to be used in the query.
 * @param {QueryOptions} options Options specified by the user
 * @param params
 * @param [meta] Prepared statement metadata
 * @param {Function} callback
 * @private
 */
Client.prototype._setQueryOptions = function (options, params, meta, callback) {
  var paramsInfo;
  var self = this;
  function adaptPageStateAndRouting(keys, params) {
    if (typeof options.pageState === 'string') {
      //pageState can be a hex string
      options.pageState = new Buffer(options.pageState, 'hex');
    }
    self._getEncoder().setRoutingKey(params, options, keys);
  }
  if (!options.prepare && params && !util.isArray(params) && this.controlConnection.protocolVersion < 3) {
    //Only Cassandra 2.1 and above supports named parameters
    return callback(new errors.ArgumentError('Named parameters for simple statements are not supported, use prepare flag'));
  }
  try {
    if (options.prepare) {
      paramsInfo = utils.adaptNamedParamsPrepared(params, meta.columns);
      //Add the type information provided by the prepared metadata
      options.hints = meta.columns.map(function (c) { return c.type; });
      adaptPageStateAndRouting(paramsInfo.keys, paramsInfo.params);
    }
    else {
      paramsInfo = utils.adaptNamedParamsWithHints(params, options);
      adaptPageStateAndRouting(paramsInfo.keys, paramsInfo.params);
    }
  }
  catch (err) {
    return callback(err);
  }
  if (options.prepare || !util.isArray(options.hints)) {
    //if we have all the information to execute
    return callback(null, paramsInfo.params);
  }
  //Only not prepared with hints
  //Adapting user hints is an async op
  this.metadata.adaptUserHints(self.keyspace, options.hints, function (err) {
    if (err) return callback(err);
    try {
      adaptPageStateAndRouting(paramsInfo.keys, paramsInfo.params);
    }
    catch (err) {
      return callback(err);
    }
    callback(null, paramsInfo.params);
  });
};

/**
 * Callback used by execution methods.
 * @callback ResultCallback
 * @param {Error} err Error occurred in the execution of the query.
 * @param {ResultSet} result Result of the execution of the query.
 */

module.exports = Client;
