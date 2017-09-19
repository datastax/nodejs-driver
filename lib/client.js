"use strict";
var events = require('events');
var util = require('util');

var utils = require('./utils.js');
var errors = require('./errors.js');
var types = require('./types');
var ControlConnection = require('./control-connection');
var ProfileManager = require('./execution-profile').ProfileManager;
var RequestHandler = require('./request-handler');
var PrepareHandler = require('./prepare-handler');
var requests = require('./requests');
var clientOptions = require('./client-options');
var ClientState = require('./metadata/client-state');

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
 * @property {Boolean} prepareOnAllHosts Determines if the driver should prepare queries on all hosts in the cluster.
 * Default: <code>true</code>.
 * @property {Boolean} rePrepareOnUp Determines if the driver should re-prepare all cached prepared queries on a
 * host when it marks it back up.
 * Default: <code>true</code>.
 * @property {Number} maxPrepared Determines the maximum amount of different prepared queries before evicting items
 * from the internal cache. Reaching a high threshold hints that the queries are not being reused, like when
 * hard-coding parameter values inside the queries.
 * Default: <code>500</code>.
 * @property {Object} policies
 * @property {LoadBalancingPolicy} policies.loadBalancing The load balancing policy instance to be used to determine
 * the coordinator per query.
 * @property {RetryPolicy} policies.retry The retry policy.
 * @property {ReconnectionPolicy} policies.reconnection The reconnection policy to be used.
 * @property {AddressTranslator} policies.addressResolution The address resolution policy.
 * @property {SpeculativeExecutionPolicy} policies.speculativeExecution The <code>SpeculativeExecutionPolicy</code>
 * instance to be used to determine if the client should send speculative queries when the selected host takes more
 * time than expected.
 * <p>
 *   Default: <code>[NoSpeculativeExecutionPolicy]{@link
  *   module:policies/speculativeExecution~NoSpeculativeExecutionPolicy}</code>
 * </p>
 * @property {TimestampGenerator} policies.timestampGeneration The client-side
 * [query timestamp generator]{@link module:policies/timestampGeneration~TimestampGenerator}.
 * <p>
 *   Default: <code>[MonotonicTimestampGenerator]{@link module:policies/timestampGeneration~MonotonicTimestampGenerator}
 *   </code>
 * </p>
 * <p>Use <code>null</code> to disable client-side timestamp generation.</p>
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
 * Setting a value of 0 disables read timeouts. Default: <code>12000</code>.
 * @property {Boolean} socketOptions.tcpNoDelay When set to true, it disables the Nagle algorithm. Default: true.
 * @property {Number} socketOptions.coalescingThreshold Buffer length in bytes use by the write queue before flushing
 * the frames. Default: 8000.
 * @property {AuthProvider} authProvider Provider to be used to authenticate to an auth-enabled cluster.
 * @property {Object} sslOptions Client-to-node ssl options. When set the driver will use the secure layer.
 * You can specify cert, ca, ... options named after the Node.js <code>tls.connect()</code> options.
 * <p>
 *   It uses the same default values as Node.js <code>tls.connect()</code> except for <code>rejectUnauthorized</code>
 *   which is set to <code>false</code> by default (for historical reasons). This setting is likely to change
 *   in upcoming versions to enable validation by default.
 * </p>
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
 * @property {Function} promiseFactory Function to be used to create a <code>Promise</code> from a
 * callback-style function.
 * <p>
 *   Promise libraries often provide different methods to create a promise. For example, you can use Bluebird's
 *   <code>Promise.fromCallback()</code> method.
 * </p>
 * <p>
 *   By default, the driver will use the
 *   [Promise constructor]{@link https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Promise}.
 * </p>
 */

/**
 * Query options
 * @typedef {Object} QueryOptions
 * @property {Boolean} [autoPage] Determines if the driver must retrieve the following result pages automatically.
 * <p>
 *   This setting is only considered by the [Client#eachRow()]{@link Client#eachRow} method. For more information,
 *   check the
 *   [paging results documentation]{@link http://docs.datastax.com/en/developer/nodejs-driver/latest/features/paging/}.
 * </p>
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
 * @property {Boolean} [isIdempotent] Defines whether the query can be applied multiple times without changing the result
 * beyond the initial application.
 * <p>
 *   The query execution idempotence can be used at [RetryPolicy]{@link module:policies/retry~RetryPolicy} level to
 *   determine if an statement can be retried in case of request error or write timeout.
 * </p>
 * <p>Default: <code>false</code>.</p>
 * @property {String} [keyspace] Specifies the keyspace for the query. Used for routing within the driver, this
 * property is suitable when the query operates on a different keyspace than the current {@link Client#keyspace}.
 * <p>
 *   This property should only be set manually by the user when the query operates on a different keyspace than
 *   the current {@link Client#keyspace} and using either batch or non-prepared query executions.
 * </p>
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
 * @example <caption>Executing a query with promise-based API</caption>
 * const result = await client.execute('SELECT key FROM system.local');
 * const row = result.first();
 * console.log(row['key']);
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
  this.hosts = this.controlConnection.hosts;
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
 * Tries to connect to one of the [contactPoints]{@link ClientOptions} and discovers the rest the nodes of the cluster.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the client is connected. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * <p>
 *   If the {@link Client} is already connected, it invokes callback immediately (when provided) or the promise is
 *   fulfilled .
 * </p>
 * @example <caption>Callback-based execution</caption>
 * client.connect(function (err) {
 *   if (err) return console.error(err);
 *   console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
 * });
 * @example <caption>Promise-based execution</caption>
 * await client.connect();
 * @param {function} [callback] The callback is invoked when the pool is connected it failed to connect.
 */
Client.prototype.connect = function (callback) {
  return utils.promiseWrapper.call(this, this.options, callback, false, this._connectCb);
};

/**
 * @param {Function} callback
 * @private
 */
Client.prototype._connectCb = function (callback) {
  if (this.connected) {
    return callback();
  }
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
      self.profileManager.init(self, self.hosts, next);
    },
    function setKeyspace(next) {
      if (!self.keyspace) {
        return next();
      }
      RequestHandler.setKeyspace(self, next);
    },
    function setPoolOptionsAndWarmup(next) {
      //Set the pooling options depending on the protocol version
      var coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV3;
      if (!types.protocolVersion.uses2BytesStreamIds(self.controlConnection.protocolVersion)) {
        coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV2;
      }
      self.options.pooling = utils.deepExtend(
        {}, { coreConnectionsPerHost: coreConnectionsPerHost }, self.options.pooling);
      if (!self.options.pooling.warmup) {
        return next();
      }
      self._warmup(next);
    }
  ], function connectFinished(err) {
    if (err) {
      // We should close the pools (if any) and reset the state to allow successive calls to connect()
      return self.controlConnection.reset(function () {
        self.connected = false;
        self.connecting = false;
        self.emit('connected', err);
      });
    }
    self._setHostListeners();
    // Set the distance of the control connection host relatively to this instance
    self.profileManager.getDistance(self.controlConnection.host);
    self.connected = true;
    self.connecting = false;
    self.emit('connected');
  });
};

/**
 * Executes a query on an available connection.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the execution completes. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * <p>The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag.</p>
 * <p>
 *   Some executions failures can be handled transparently by the driver, according to the
 *   [RetryPolicy]{@link module:policies/retry~RetryPolicy} defined at {@link ClientOptions} or {@link QueryOptions}
 *   level.
 * </p>
 * @param {String} query The query to execute.
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value.
 * @param {QueryOptions} [options] The query options for the execution.
 * @param {ResultCallback} [callback] Executes callback(err, result) when execution completed. When not defined, the
 * method will return a promise.
 * @example <caption>Callback-based API</caption>
 * const query = 'SELECT name, email FROM users WHERE id = ?';
 * client.execute(query, [ id ], { prepare: true }, function (err, result) {
 *   assert.ifError(err);
 *   const row = result.first();
 *   console.log('%s: %s', row.name, row.email);
 * });
 * @example <caption>Promise-based API, using async/await</caption>
 * const query = 'SELECT name, email FROM users WHERE id = ?';
 * const result = await client.execute(query, [ id ], { prepare: true });
 * const row = result.first();
 * console.log('%s: %s', row.name, row.email);
 * @see {@link ExecutionProfile} to reuse a set of options across different query executions.
 */
Client.prototype.execute = function (query, params, options, callback) {
  // set default argument values for optional parameters
  callback = callback || (options ? options : params);
  if (typeof callback === 'function') {
    params = typeof params !== 'function' ? params : null;
  }
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    options = clientOptions.createQueryOptions(this, options);
    this._innerExecute(query, params, options, cb);
  });
};

/**
 * Executes the query and calls rowCallback for each row as soon as they are received. Calls final callback after all
 * rows have been sent, or when there is an error.
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
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the execution completes. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array
 * of object containing the query and params
 * @param {QueryOptions} [options]
 * @param {ResultCallback} [callback] Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function (queries, options, callback) {
  callback = callback || options;
  return utils.promiseWrapper.call(this, this.options, callback, false, function handler(cb) {
    this._batchCb(queries, options, cb);
  });
};

/**
 * @param {Array.<string>|Array.<{query, params}>}queries
 * @param {QueryOptions} options
 * @param {ResultCallback} callback
 * @private
 */
Client.prototype._batchCb = function (queries, options, callback) {
  var self = this;
  if (!Array.isArray(queries)) {
    // We should throw (not callback) for an unexpected type
    throw new errors.ArgumentError('Queries should be an Array');
  }
  if (queries.length === 0) {
    return callback(new errors.ArgumentError('Queries array can not be empty'));
  }
  options = clientOptions.createQueryOptions(this, options, null, true);
  if (options.message && options instanceof Error) {
    return callback(options);
  }

  utils.series([
    function connect(next) {
      self.connect(next);
    },
    function adaptQueries(next) {
      if (options.prepare) {
        return PrepareHandler.getPreparedMultiple(
          self, options.executionProfile.loadBalancing, queries, self.keyspace, next);
      }
      var parsedQueries = new Array(queries.length);
      for (var i = 0; i < queries.length; i++) {
        var item = queries[i];
        if (!item) {
          return next(new errors.ArgumentError(util.format('Invalid query at index %d', i)));
        }
        var query = typeof item === 'string' ? item : item.query;
        if (!query) {
          return next(errors.ArgumentError(util.format('Invalid query at index %d', i)));
        }
        parsedQueries[i] = { query: query, params: item.params };
      }
      next(null, parsedQueries);
    }
  ], function seriesEnd(err, queryItems) {
    if (err) {
      return callback(err);
    }
    var request = new requests.BatchRequest(queryItems, options);
    var handler = new RequestHandler(request, options, self);
    handler.send(callback);
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

/**
 * Gets a snapshot containing information on the connections pools held by this Client at the current time.
 * <p>
 *   The information provided in the returned object only represents the state at the moment this method was called and
 *   it's not maintained in sync with the driver metadata.
 * </p>
 * @return module:metadata~ClientState
 */
Client.prototype.getState = function () {
  return ClientState.from(this);
};

Client.prototype.log = utils.log;

/**
 * Closes all connections to all hosts.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the client is disconnected. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * @param {Function} [callback] Optional callback to be invoked when finished closing all connections.
 */
Client.prototype.shutdown = function (callback) {
  return utils.promiseWrapper.call(this, this.options, callback, true, this._shutdownCb);
};

/**
 * @param {Function} callback
 * @private
 */
Client.prototype._shutdownCb = function (callback) {
  var self = this;
  function doShutdown() {
    self.connected = false;
    self.isShuttingDown = true;
    var hosts = self.hosts.values();
    // Shutdown the ControlConnection before shutting down the pools
    self.controlConnection.shutdown();
    self.options.policies.speculativeExecution.shutdown();
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
      if (err) {
        return next(err);
      }
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
      var handler = new RequestHandler(request, options, self);
      handler.send(next);
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
      var lbp = options.executionProfile.loadBalancing;
      PrepareHandler.getPrepared(self, lbp, query, self.keyspace, function (err, id, m) {
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
        query,
        queryId,
        params,
        options);
      request.query = query;
      var handler = new RequestHandler(request, options, self);
      handler.send(next);
    }
  ], callback);
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
  var version = this.controlConnection.protocolVersion;
  if (!options.prepare && params && !util.isArray(params) && !types.protocolVersion.supportsNamedParameters(version)) {
    //Only Cassandra 2.1 and above supports named parameters
    return callback(
      new errors.ArgumentError('Named parameters for simple statements are not supported, use prepare flag'));
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
      if (!options.keyspace && meta.keyspace) {
        options.keyspace = meta.keyspace;
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
        options.pageState = utils.allocBufferFromString(options.pageState, 'hex');
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
