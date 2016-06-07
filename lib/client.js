"use strict";
var events = require('events');
var util = require('util');

var utils = require('./utils.js');
var errors = require('./errors.js');
var types = require('./types');
var ControlConnection = require('./control-connection');
var ProfileManager = require('./execution-profile').ProfileManager;
var RequestHandler = require('./request-handler');
var requests = require('./requests');
var clientOptions = require('./client-options');
/**
 * Max amount of pools being warmup in parallel, when warmup is enabled
 * @const {Number}
 * @private
 */
var warmupLimit = 32;

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
 *  but it is usually a good idea to provide more than one contact point, because if that single contact point is
 *  unavailable, the driver will not be able to initialize correctly.
 * </p>
 * @property {String} keyspace The logged keyspace for all the connections created within the {@link Client} instance.
 * @property {Number} refreshSchemaDelay The default window size in milliseconds used to debounce node list and schema
 * refresh metadata requests. Default: 1000.
 * @property {Boolean} isMetadataSyncEnabled Determines whether client-side schema metadata retrieval and update is
 * enabled.
 * <p>Setting this value to <code>false</code> will cause keyspace information not to be automatically loaded, affecting
 * replica calculation per token in the different keyspaces. When disabling metadata synchronization, use
 * [Metadata.refreshKeyspaces()]{@link module:metadata~Metadata#refreshKeyspaces} to keep keyspace information up to
 * date or token-awareness will not work correctly.</p>
 * Default: <code>true</code>.
 * @property {Object} policies
 * @property {LoadBalancingPolicy} policies.loadBalancing The load balancing policy instance to be used to determine
 * the coordinator per query.
 * @property {RetryPolicy} policies.retry The retry policy.
 * @property {ReconnectionPolicy} policies.reconnection The reconnection policy to be used.
 * @property {AddressTranslator} policies.addressResolution The address resolution policy.
 * @property {QueryOptions} queryOptions Default options for all queries.
 * @property {Object} pooling Pooling options.
 * @property {Number} pooling.heartBeatInterval The amount of idle time in milliseconds that has to pass before the
 * driver issues a request on an active connection to avoid idle time disconnections. Default: 30000.
 * @property {Object} pooling.coreConnectionsPerHost Associative array containing amount of connections per host
 * distance.
 * @property {Boolean} pooling.warmup Determines if all connections to hosts in the local datacenter must be opened on
 * connect. Default: false.
 * @property {Object} protocolOptions
 * @property {Number} protocolOptions.port The port to use to connect to the Cassandra host. If not set through this
 * method, the default port (9042) will be used instead.
 * @property {Number} protocolOptions.maxSchemaAgreementWaitSeconds The maximum time in seconds to wait for schema
 * agreement between nodes before returning from a DDL query. Default: 10.
 * @property {Number} protocolOptions.maxVersion When set, it limits the maximum protocol version used to connect to
 * the nodes.
 * Useful for using the driver against a cluster that contains nodes with different major/minor versions of Cassandra.
 * @property {Object} socketOptions
 * @property {Number} socketOptions.connectTimeout Connection timeout in milliseconds. Default: 5000.
 * @property {Number} socketOptions.defunctReadTimeoutThreshold Determines the amount of requests that simultaneously
 * have to timeout before closing the connection. Default: 64.
 * @property {Boolean} socketOptions.keepAlive Whether to enable TCP keep-alive on the socket. Default: true.
 * @property {Number} socketOptions.keepAliveDelay TCP keep-alive delay in milliseconds. Default: 0.
 * @property {Number} socketOptions.readTimeout Per-host read timeout in milliseconds.
 * <p>
 *   Please note that this is not the maximum time a call to {@link Client#execute} may have to wait;
 *   this is the maximum time that call will wait for one particular Cassandra host, but other hosts will be tried if
 *   one of them timeout. In other words, a {@link Client#execute} call may theoretically wait up to
 *   <code>readTimeout * number_of_cassandra_hosts</code> (though the total number of hosts tried for a given query also
 *   depends on the LoadBalancingPolicy in use).
 * <p>When setting this value, keep in mind the following:</p>
 * <ul>
 *   <li>the timeout settings used on the Cassandra side (*_request_timeout_in_ms in cassandra.yaml) should be taken
 *   into account when picking a value for this read timeout. You should pick a value a couple of seconds greater than
 *   the Cassandra timeout settings.
 *   </li>
 *   <li>
 *     the read timeout is only approximate and only control the timeout to one Cassandra host, not the full query.
 *   </li>
 * </ul>
 * Setting a value of 0 disables read timeouts. Default: 0.
 * @property {Boolean} socketOptions.tcpNoDelay When set to true, it disables the Nagle algorithm. Default: true.
 * @property {Number} socketOptions.coalescingThreshold Buffer length in bytes use by the write queue before flushing
 * the frames. Default: 8000.
 * @property {AuthProvider} authProvider Provider to be used to authenticate to an auth-enabled cluster.
 * @property {Object} sslOptions Client-to-node ssl options, when set the driver will use the secure layer.
 * You can specify cert, ca, ... options named after the Node.js tls.connect options.
 * @property {Object} encoding
 * @property {Function} encoding.map Map constructor to use for Cassandra map<k,v> type encoding and decoding.
 * If not set, it will default to Javascript Object with map keys as property names.
 * @property {Function} encoding.set Set constructor to use for Cassandra set<k> type encoding and decoding.
 * If not set, it will default to Javascript Array.
 * @property {Boolean} encoding.copyBuffer Determines if the network buffer should be copied for buffer based data
 * types (blob, uuid, timeuuid and inet).
 * <p>
 *   Setting it to true will cause that the network buffer is copied for each row value of those types,
 *   causing additional allocations but freeing the network buffer to be reused.
 *   Setting it to true is a good choice for cases where the Row and ResultSet returned by the queries are long-lived
 *   objects.
 * </p>
 * <p>
 *  Setting it to false will cause less overhead and the reference of the network buffer to be maintained until the row
 *  / result set are de-referenced.
 *  Default: true.
 * </p>
 * @property {Boolean} encoding.useUndefinedAsUnset Valid for Cassandra 2.2 and above. Determines that, if a parameter
 * is set to
 * <code>undefined</code> it should be encoded as <code>unset</code>.
 * <p>
 *  By default, ECMAScript <code>undefined</code> is encoded as <code>null</code> in the driver. Cassandra 2.2
 *  introduced the concept of unset.
 *  At driver level, you can set a parameter to unset using the field <code>types.unset</code>. Setting this flag to
 *  true allows you to use ECMAScript undefined as Cassandra <code>unset</code>.
 * </p>
 * <p>
 *   Default: true.
 * </p>
 * @property {Array.<ExecutionProfile>} profiles The array of [execution profiles]{@link ExecutionProfile}.
 */

/**
 * Query options
 * @typedef {Object} QueryOptions
 * @property {Boolean} [autoPage] Determines if the driver must retrieve the following result pages automatically.
 * @property {Boolean} [captureStackTrace] Determines if the stack trace before the query execution should be
 * maintained.
 * <p>
 *   Useful for debugging purposes, it should be set to <code>false</code> under production environment as it adds an
 *   unnecessary overhead to each execution.
 * </p>
 * Default: false.
 * @property {Number} [consistency] [Consistency level]{@link module:types~consistencies}. Default: localOne.
 * @property {Object} [customPayload] Key-value payload to be passed to the server. On the Cassandra side, 
 * implementations of QueryHandler can use this data.
 * @property {String|ExecutionProfile} [executionProfile] Name or instance of the [profile]{@link ExecutionProfile} to
 * be used for this execution. If not set, it will the use "default" execution profile.
 * @property {Number} [fetchSize] Amount of rows to retrieve per page.
 * @property {Array|Array<Array>} [hints] Type hints for parameters given in the query, ordered as for the parameters.
 * <p>For batch queries, an array of such arrays, ordered as with the queries in the batch.</p>
 * @property {Boolean} [logged] Determines if the batch should be written to the batchlog. Only valid for
 * [Client#batch()]{@link Client#batch}, it will be ignored by other methods. Default: true.
 * @property {Buffer|String} [pageState] Buffer or string token representing the paging state.
 * <p>Useful for manual paging, if provided, the query will be executed starting from a given paging state.</p>
 * @property {Boolean} [prepare] Determines if the query must be executed as a prepared statement.
 * @property {Number} [readTimeout] When defined, it overrides the default read timeout
 * (<code>socketOptions.readTimeout</code>) in milliseconds for this execution per coordinator.
 * <p>
 *   Suitable for statements for which the coordinator may allow a longer server-side timeout, for example aggregation
 *   queries.
 * </p>
 * <p>
 *   A value of <code>0</code> disables client side read timeout for the execution. Default: <code>undefined</code>.
 * </p>
 * @property {RetryPolicy} [retry] Retry policy for the query.
 * <p>
 *   This property can be used to specify a different [retry policy]{@link module:policies/retry} to the one specified
 *   in the {@link ClientOptions}.policies.
 * </p>
 * @property {Boolean} [retryOnTimeout] Determines if the client should retry when it didn't hear back from a host
 * within <code>socketOptions.readTimeout</code>. Default: true.
 * @property {Array} [routingIndexes] Index of the parameters that are part of the partition key to determine
 * the routing.
 * @property {Buffer|Array} [routingKey] Partition key(s) to determine which coordinator should be used for the query.
 * @property {Array} [routingNames] Array of the parameters names that are part of the partition key to determine the
 * routing.
 * @property {Number} [serialConsistency] Serial consistency is the consistency level for the serial phase of
 * conditional updates.
 * This option will be ignored for anything else that a conditional update/insert.
 * @property {Number|Long} [timestamp] The default timestamp for the query in microseconds from the unix epoch
 * (00:00:00, January 1st, 1970).
 * <p>If provided, this will replace the server side assigned timestamp as default timestamp.</p>
 * <p>Use [generateTimestamp()]{@link module:types~generateTimestamp} utility method to generate a valid timestamp
 * based on a Date and microseconds parts.</p>
 * @property {Boolean} [traceQuery] Enable query tracing for the execution. Use query tracing to diagnose performance
 * problems related to query executions. Default: false.
 * <p>To retrieve trace, you can call [Metadata.getTrace()]{@link module:metadata~Metadata#getTrace} method.</p>
 */

/**
 * Creates a new instance of {@link Client}.
 * @classdesc
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
 * @extends EventEmitter
 * @param {ClientOptions} options The options for this instance.
 * @example <caption>Creating a new client instance</caption>
 * const client = new Client({ contactPoints: ['192.168.1.100'] });
 * client.connect(function (err) {
 *   if (err) return console.error(err);
 *   console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
 * });
 * @example <caption>Executing a query</caption>
 * // calling #execute() can be made without previously calling #connect(), as internally
 * // it will ensure it's connected before attempting to execute the query
 * client.execute('SELECT key FROM system.local', function (err, result) {
 *   if (err) return console.error(err);
 *   const row = result.first();
 *   console.log(row['key']);
 * });
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  this.options = clientOptions.extend({ logEmitter: this.emit.bind(this) }, options);
  Object.defineProperty(this, 'profileManager', { value: new ProfileManager(this.options) });
  Object.defineProperty(this, 'controlConnection', {
    value: new ControlConnection(this.options, this.profileManager), writable: true }
  );
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.connected = false;
  this.isShuttingDown = false;
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
 * Emitted when a new host is added to the cluster.
 * <ul>
 *   <li>{@link Host} The host being added.</li>
 * </ul>
 * @event Client#hostAdd
 */
/**
 * Emitted when a host is removed from the cluster
 * <ul>
 *   <li>{@link Host} The host being removed.</li>
 * </ul>
 * @event Client#hostRemove
 */
/**
 * Emitted when a host in the cluster changed status from down to up.
 * <ul>
 *   <li>{@link Host host} The host that changed the status.</li>
 * </ul>
 * @event Client#hostUp
 */
/**
 * Emitted when a host in the cluster changed status from up to down.
 * <ul>
 *   <li>{@link Host host} The host that changed the status.</li>
 * </ul>
 * @event Client#hostDown
 */

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
  if (this.isShuttingDown) {
    //it is being shutdown, don't allow further calls to connect()
    return callback(new errors.NoHostAvailableError(null, 'Connecting after shutdown is not supported'));
  }
  this.once('connected', callback);
  if (this.connecting) {
    //the listener to connect was added, move on
    return;
  }
  this.connecting = true;
  var self = this;
  utils.series([
    function initControlConnection(next) {
      self.controlConnection.init(next);
    },
    function initLoadBalancingPolicy(next) {
      self.hosts = self.controlConnection.hosts;
      self._setHostListeners();
      self.profileManager.init(self, self.hosts, next);
    },
    function setKeyspace(next) {
      if (!self.keyspace) {
        return next();
      }
      self._setKeyspace(next);
    },
    function setPoolOptionsAndWarmup(next) {
      //Set the pooling options depending on the protocol version
      var coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV3;
      if (self.controlConnection.protocolVersion < 3) {
        coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV2;
      }
      self.options.pooling = utils.deepExtend({}, { coreConnectionsPerHost: coreConnectionsPerHost }, self.options.pooling);
      if (!self.options.pooling.warmup) {
        return next();
      }
      self._warmup(next);
    }
  ], function connectFinished(err) {
    self.connected = !err;
    self.connecting = false;
    self.emit('connected', err);
    if (self.connected) {
      // Set the distance of the control connection host relatively to this instance
      self.profileManager.getDistance(self.controlConnection.host);
    }
  });
};

/**
 * Executes a query on an available connection.
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
 *   hosts if needed.
 * </p>
 * @param {String} query The query to execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value
 * @param {QueryOptions} [options]
 * @param {ResultCallback} callback Executes callback(err, result) when finished
 */
Client.prototype.execute = function (query, params, options, callback) {
  // set default argument values for optional parameters
  callback = utils.bindDomain(callback || (options ? options : params));
  params = typeof params !== 'function' ? params : null;
  options = clientOptions.createQueryOptions(this, options);
  this._innerExecute(query, params, options, callback);
};

/**
 * Executes the query and calls rowCallback for each row as soon as they are received.
 *  Calls final callback after all rows have been sent, or when there is an error.
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
 *   hosts if needed.
 * </p>
 * @param {String} query The query to execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value.
 * @param {QueryOptions} [options]
 * @param {function} rowCallback Executes <code>rowCallback(n, row)</code> per each row received, where n is the row
 * index and row is the current Row.
 * @param {function} [callback] Executes <code>callback(err, result)</code> after all rows have been received.
 * <p>
 *   When dealing with paged results, [ResultSet#nextPage()]{@link module:types~ResultSet#nextPage} method can be used
 *   to retrieve the following page. In that case, <code>rowCallback()</code> will be again called for each row and
 *   the final callback will be invoked when all rows in the following page has been retrieved.
 * </p>
 * @example <caption>Using per-row callback and arrow functions</caption>
 * client.eachRow(query, params, { prepare: true }, (n, row) => console.log(n, row), err => console.error(err));
 * @example <caption>Overloads</caption>
 * client.eachRow(query, rowCallback);
 * client.eachRow(query, params, rowCallback);
 * client.eachRow(query, params, options, rowCallback);
 * client.eachRow(query, params, rowCallback, callback);
 * client.eachRow(query, params, options, rowCallback, callback);
 */
Client.prototype.eachRow = function (query, params, options, rowCallback, callback) {
  if (!callback && rowCallback && typeof options === 'function') {
    callback = utils.bindDomain(rowCallback);
    rowCallback = utils.bindDomain(options);
  }
  else {
    callback = utils.bindDomain(callback || utils.noop);
    rowCallback = utils.bindDomain(rowCallback || options || params);
  }
  params = typeof params !== 'function' ? params : null;
  options = clientOptions.createQueryOptions(this, options, rowCallback);
  var self = this;
  var rowLength = 0;
  function nextPage() {
    self._innerExecute(query, params, options, pageCallback);
  }
  function pageCallback (err, result) {
    if (err) {
      return callback(err);
    }
    // Next requests in case paging (auto or explicit) is used
    rowLength += result.rowLength;
    if (result.meta && result.meta.pageState) {
      // Use new page state as next request page state
      options.pageState = result.meta.pageState;
      if (options.autoPage) {
        // Issue next request for the next page
        return nextPage();
      }
      // Allows for explicit (manual) paging, in case the caller needs it
      result.nextPage = nextPage;
    }
    // Finished auto-paging
    result.rowLength = rowLength;
    callback(null, result);
  }
  this._innerExecute(query, params, options, pageCallback);
};

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
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
 *   hosts if needed.
 * </p>
 * @param {String} query The query to prepare and execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value
 * @param {QueryOptions} [options]
 * @param {function} [callback], executes callback(err) after all rows have been received or if there is an error
 * @returns {types.ResultStream}
 */
Client.prototype.stream = function (query, params, options, callback) {
  callback = utils.bindDomain(callback || utils.noop);
  // NOTE: the nodejs stream maintains yet another internal buffer 
  // we rely on the default stream implementation to keep memory 
  // usage reasonable.
  var resultStream = new types.ResultStream({ objectMode: 1 });
  function onFinish(err, result) {
    if (err) {
      resultStream.emit('error', err);
    }
    if (result && result.nextPage ) {
      // allows for throttling as per the
      // default nodejs stream implementation
      resultStream._valve(function pageValve() {
        try {
          result.nextPage();
        }
        catch( ex ) {
          resultStream.emit('error', ex );
        }
      });
      return;
    }
    // Explicitly dropping the valve (closure)
    resultStream._valve(null);
    resultStream.add(null);
    callback(err);
  }
  var sync = true;
  this.eachRow(query, params, options, function rowCallback(n, row) {
    resultStream.add(row);
  }, function eachRowFinished(err, result) {
    if (sync) {
      // Prevent sync callback
      return setImmediate(function eachRowFinishedImmediate() {
        onFinish(err, result);
      });
    }
    onFinish(err, result);
  });
  sync = false;
  return resultStream;
};

/**
 * Executes batch of queries on an available connection to a host.
 * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array
 * of object containing the query and params
 * @param {QueryOptions} [options]
 * @param {ResultCallback} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function (queries, options, callback) {
  callback = utils.bindDomain(callback || options);
  queries = validateBatchQueries(queries);
  options = clientOptions.createQueryOptions(this, options, null, true);
  if (options.message && options instanceof Error) {
    return callback(options);
  }
  var self = this;
  this.connect(function afterConnect(err) {
    if (err) {
      return callback(err);
    }
    if (options.prepare) {
      //Batch of prepared statements
      return self._batchPrepared(queries, options, callback);
    }
    //Batch of simple statements
    self._sendBatch(queries, options, callback);
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
  var self = this;
  function doShutdown() {
    self.connected = false;
    self.isShuttingDown = true;
    var hosts = self.hosts.values();
    self.controlConnection.shutdown();
    // go through all the host and shut down their pools
    utils.each(hosts, function (h, next) {
      h.shutdown(false, next);
    }, callback);
  }
  this.log('info', 'Shutting down');
  callback = callback || utils.noop;
  if (!this.hosts || !this.connected) {
    // not initialized
    this.connected = false;
    return callback();
  }
  if (this.connecting) {
    this.log('warning', 'Shutting down while connecting');
    // wait until finish connecting for easier troubleshooting
    return this.once('connected', doShutdown);
  }
  doShutdown();
};

/**
 * Waits until that the schema version in all nodes is the same or the waiting time passed.
 * @param {Connection} connection
 * @param {Function} callback
 * @ignore
 */
Client.prototype._waitForSchemaAgreement = function (connection, callback) {
  if (this.hosts.length === 1) {
    return setImmediate(callback);
  }
  var self = this;
  var start = new Date();
  var maxWaitTime = this.options.protocolOptions.maxSchemaAgreementWaitSeconds * 1000;
  this.log('info', 'Waiting for schema agreement');
  var versionsMatch;
  var peerVersions;
  utils.whilst(function condition() {
    return !versionsMatch && (new Date() - start) < maxWaitTime;
  }, function fn(next) {
    utils.series([
      function (next) {
        self.metadata.getPeersSchemaVersions(connection, function (err, result) {
          peerVersions = result;
          next(err);
        });
      },
      function (next) {
        self.metadata.getLocalSchemaVersion(connection, next);
      }
    ], function seriesEnded(err, localVersion) {
      if (err) return next(err);
      //check the different versions
      versionsMatch = true;
      localVersion = localVersion.toString();
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
  }, callback);
};

/**
 * Waits for schema agreements and schedules schema metadata refresh.
 * @param {Connection} connection
 * @param event
 * @param {Function} callback
 * @ignore
 * @internal
 */
Client.prototype.handleSchemaAgreementAndRefresh = function (connection, event, callback) {
  var self = this;
  this._waitForSchemaAgreement(connection, function agreementCb(err) {
    if (err) {
      //we issue a warning but we continue with the normal flow
      self.log('warning', 'There was an error while waiting for the schema agreement between nodes', err);
    }
    if (!self.options.isMetadataSyncEnabled) {
      return callback();
    }
    // schedule metadata refresh immediately and the callback will be invoked once it was refreshed
    self.controlConnection.handleSchemaChange(event, true, callback);
  });
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
  // Use Error#message property because is faster than checking prototypes
  if (options.message && options instanceof Error) {
    return callback(options);
  }
  if (options.prepare) {
    return this._executeAsPrepared(query, params, options, callback);
  }
  var self = this;
  utils.series([
    function connecting(next) {
      self.connect(next);
    },
    function settingOptions(next) {
      self._setQueryOptions(options, params, null, function setOptionsCallback(err, p) {
        params = p;
        next(err);
      });
    },
    function sendingQuery(next) {
      var request = new requests.QueryRequest(
        query,
        params,
        options);
      var handler = new RequestHandler(self, options.executionProfile.loadBalancing, options.retry);
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
  var queryId;
  var meta;
  var self = this;
  utils.series([
    function connecting(next) {
      self.connect(next);
    },
    function preparing(next) {
      self._getPrepared(query, options, function (err, id, m) {
        queryId = id;
        meta = m;
        next(err);
      });
    },
    function settingOptions(next) {
      self._setQueryOptions(options, params, meta, function (err, p) {
        params = p;
        next(err);
      });
    },
    function sendingExecute(next) {
      var request = new requests.ExecuteRequest(
        queryId,
        params,
        options);
      request.query = query;
      var handler = new RequestHandler(self, options.executionProfile.loadBalancing, options.retry);
      handler.send(request, options, next);
    }
  ], callback);
};

/**
 * Prepares the queries and then executes the batch.
 * @param {Array.<{query, params}>} queries Array of object instances containing query and params properties.
 * @param {Object} options
 * @param {ResultCallback} callback Executes callback(err, result) when the batch was executed
 * @private
 */
Client.prototype._batchPrepared = function (queries, options, callback) {
  var self = this;
  queries = queries.map(function batchQueryMap(item) {
    return { info: self.metadata.getPreparedInfo(self.keyspace, item.query), query: item.query, params: item.params};
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
    var handler = new RequestHandler(self, options.executionProfile.loadBalancing, options.retry);
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
  var handler = new RequestHandler(this, options.executionProfile.loadBalancing, options.retry);
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
    utils.each(Object.keys(queriesMap), function waitIterator(query, next) {
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
function validateBatchQueries(queries) {
  if (!util.isArray(queries)) {
    throw new errors.ArgumentError('The first argument must be an Array of queries.');
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
  return parsedQueries;
}

/**
 * It returns the id of the prepared query.
 * If its not prepared, it prepares the query.
 * If its being prepared, it queues the callback
 * @param {String} query Query to prepare with ? or :param_name as parameter placeholders
 * @param {Object} options Execution query options
 * @param {function} callback Executes callback(err, queryId) when there is a prepared statement on a connection or
 * there is an error.
 * @private
 */
Client.prototype._getPrepared = function (query, options, callback) {
  var info = this.metadata.getPreparedInfo(this.keyspace, query);
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
  var handler = new RequestHandler(this, options.executionProfile.loadBalancing, options.retry);
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
  var handler = new RequestHandler(this, this.options.policies.loadBalancing, this.options.policies.retry);
  handler.setKeyspace(callback);
};

/**
 * Sets the listeners for the nodes.
 * @private
 */
Client.prototype._setHostListeners = function () {
  var self = this;
  function getHostUpListener(emitter, h) {
    return (function hostUpListener() {
      emitter.emit('hostUp', h);
    });
  }
  function getHostDownListener(emitter, h) {
    return (function hostDownListener() {
      emitter.emit('hostDown', h);
    });
  }
  //Add status listeners when new nodes are added and emit hostAdd
  this.hosts.on('add', function hostAddedListener(h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
    self.emit('hostAdd', h);
  });
  //Remove all listeners and emit hostRemove
  this.hosts.on('remove', function hostRemovedListener(h) {
    h.removeAllListeners();
    self.emit('hostRemove', h);
  });
  //Add status listeners for existing hosts
  this.hosts.forEach(function (h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
  });
};

Client.prototype._warmup = function (callback) {
  var self = this;
  var hosts = this.hosts.values();
  utils.timesLimit(hosts.length, warmupLimit, function warmupEachCallback(i, next) {
    var h = hosts[i];
    var distance = self.profileManager.getDistance(h);
    if (distance !== types.distance.local) {
      //do not warmup pool for remote or ignored hosts
      return next();
    }
    h.warmupPool(function (err) {
      if (err) {
        //An error while trying to create a connection
        //To 1 host is not an issue, warn the user and move on
        self.log('warning', util.format('Connection pool to host %s could not be created: %s', h.address, err));
      }
      next();
    });
  }, callback);
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
  var protocolVersion = this.controlConnection.protocolVersion;
  if (!options.prepare && params && !util.isArray(params) && protocolVersion < 3) {
    //Only Cassandra 2.1 and above supports named parameters
    return callback(new errors.ArgumentError('Named parameters for simple statements are not supported, use prepare flag'));
  }
  var paramsInfo;
  var self = this;
  utils.series([
    function fillRoutingKeys(next) {
      if (options.routingKey || options.routingIndexes || options.routingNames || !meta) {
        //it is filled by the user
        //or it is not prepared
        return next();
      }
      if (util.isArray(meta.partitionKeys)) {
        //the partition keys are provided as part of the metadata
        options.routingIndexes = meta.partitionKeys;
        return next();
      }
      self.metadata.getTable(meta.keyspace, meta.table, function (err, tableInfo) {
        if (err) {
          self.log('warning', util.format('Table %s.%s metadata could not be retrieved', meta.keyspace, meta.table));
          //execute without a routing key
          return next();
        }
        if (!tableInfo) {
          //The data is not there, maybe it is being recreated
          return next();
        }
        options.routingIndexes = tableInfo.partitionKeys.map(function (c) {
          return meta.columnsByName[c.name];
        });
        //Skip parsing metadata next time
        meta.partitionKeys = options.routingIndexes;
        next();
      });
    },
    function adaptParameterNames(next) {
      try {
        if (options.prepare) {
          paramsInfo = utils.adaptNamedParamsPrepared(params, meta.columns);
          //Add the type information provided by the prepared metadata
          options.hints = meta.columns.map(function (c) {
            return c.type;
          });
        }
        else {
          paramsInfo = utils.adaptNamedParamsWithHints(params, options);
        }
      }
      catch (err) {
        return next(err);
      }
      next();
    },
    function adaptParameterTypes(next) {
      if (options.prepare || !util.isArray(options.hints)) {
        return next();
      }
      //Only not prepared with hints
      //Adapting user hints is an async op
      self.metadata.adaptUserHints(self.keyspace, options.hints, next);
    }
  ], function finishSettingOptions(err) {
    if (err) {
      //There was an error setting the query options
      return callback(err);
    }
    try {
      if (typeof options.pageState === 'string') {
        //pageState can be a hex string
        options.pageState = new Buffer(options.pageState, 'hex');
      }
      //noinspection JSAccessibilityCheck
      self._getEncoder().setRoutingKey(paramsInfo.params, options, paramsInfo.keys);
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
 * @param {ResultSet} [result] Result of the execution of the query.
 */

module.exports = Client;
