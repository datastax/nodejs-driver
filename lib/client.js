var events = require('events');
var util = require('util');
var async = require('async');

var utils = require('./utils.js');
var errors = require('./errors.js');
var types = require('./types.js');
var ControlConnection = require('./control-connection.js');
var RequestHandler = require('./request-handler.js');
var requests = require('./requests.js');
var clientOptions = require('./client-options.js');
var encoder = require('./encoder.js');

/**
 * Client options
 * @typedef {Object} ClientOptions
 * @property {Array} contactPoints Array of addresses or host names of the nodes to add as contact point.
 * @property {Object} policies
 * @property {LoadBalancingPolicy} policies.loadBalancing The load balancing policy instance to be used to determine the coordinator per query.
 * @property {RetryPolicy} policies.retry The retry policy.
 * @property {ReconnectionPolicy} policies.reconnection The reconnection policy to be used.
 * @property {QueryOptions} queryOptions Default options for all queries.
 * @property {Object} pooling Pooling options.
 * @property {Object} protocolOptions
 * @property {Number} protocolOptions.port The port to use to connect to the Cassandra host. If not set through this method, the default port (9042) will be used instead.
 * @property {Object} socketOptions
 * @property {Number} socketOptions.connectTimeout Connection timeout in milliseconds.
 * @property {AuthProvider} authProvider Provider to be used to authenticate to an auth-enabled host.
 * @property {Object} sslOptions Client-to-node ssl options, when set the driver will use the secure layer. You can specify cert, ca, ... options named after the Node.js tls.connect options.
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
 */

/**
 * Represents a pool of connection to multiple hosts
 * @param {ClientOptions} options
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.options = clientOptions.extend({logEmitter: this.emit.bind(this)}, options);
  this.controlConnection = new ControlConnection(this.options);
  this.hosts = null;
  this.connected = false;
  this.keyspace = options.keyspace;
  this.preparedQueries = {"__length": 0};
}

util.inherits(Client, events.EventEmitter);

/** 
 * Connects to all hosts, in case the pool is disconnected.
 * @param {function} callback is called when the pool is connected (or at least 1 connected and the rest failed to connect) or it is not possible to connect 
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
    self.metadata = self.controlConnection.metadata;
    self.options.policies.loadBalancing.init(self, self.hosts, function (err) {
      if (err) return connectCallback(err);
      if (self.keyspace) {
        return self._setKeyspaceFirst(connectCallback);
      }
      connectCallback();
    });
  });
  function connectCallback(err) {
    self.connected = !err;
    self.connecting = false;
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
 * @param {String} query The query to execute
 * @param {Array} [params] Array of params to replace
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
 * Prepares (the first time), executes the prepared query and calls rowCallback for each row as soon as they are received.
 * Calls final callback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
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
  args.options = utils.extend({}, args.options, {
    byRow: true,
    rowCallback: utils.bindDomain(rowCallback)
  });
  args.callback = utils.bindDomain(args.callback);
  var self = this;
  function nextPage() {
    self._innerExecute(args.query, args.params, args.options, pageCallback); 
  }
  function pageCallback (err, result) {
    if (err) {
      return args.callback(err);
    }
      
    //Next requests in case paging (auto or explicit) is used
    args.options.rowLength = args.options.rowLength || 0;
    args.options.rowLength += result.rowLength;
    args.options.rowLengthArray = args.options.rowLengthArray || [];
    args.options.rowLengthArray.push(result.rowLength);

    if (result.meta && result.meta.pageState) {
      //Use new page state as next request page state
      args.options.pageState = result.meta.pageState;
      //Issue next request for the next page
      if (args.options.autoPage) {
        //Next request for autopaging
        nextPage(); 
        return; 
      }
      else {
        //Allows for explicit (manual) paging, in case the caller needs it 
        result.nextPage = nextPage;
      }
    }
    //finished auto-paging
    result.rowLength = args.options.rowLength;
    result.rowLengthArray = args.options.rowLengthArray;

    args.callback(null, result);
  }
  this._innerExecute(args.query, args.params, args.options, pageCallback);
};


//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Prepares (the first time), executes the prepared query and pushes the rows to the result stream
 *  as soon as they received.
 * Calls callback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
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
 * Executes batch of queries on an available connection.
 * If the Cassandra node does down before responding, it retries the batch.
 * @param {Array} queries The query to execute
 * @param {QueryOptions} [options]
 * @param {ResultCallback} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function () {
  var self = this;
  var args = this._parseBatchArgs.apply(null, arguments);
  //logged batch by default
  args.options = utils.extend({logged: true}, this.options.queryOptions, args.options);
  args.callback = utils.bindDomain(args.callback);

  this.connect(function afterConnect(err) {
    if (err) {
      return args.callback(err);
    }
    var request = new requests.BatchRequest(args.queries, args.options.consistency, args.options);
    var handler = new RequestHandler(self, self.options);
    handler.send(request, null, args.callback);
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
    return callback();
  }
  var hosts = this.hosts.slice(0);
  async.each(hosts, function (h, next) {
    h.shutdown(next);
  }, callback);
};

/**
 * Connects and handles the execution of prepared and simple statements. All parameters are mandatory.
 * @param {string} query
 * @param {Array} params
 * @param {Object} options
 * @param {Function} callback
 * @private
 */
Client.prototype._innerExecute = function (query, params, options, callback) {
  var self = this;
  function innerCallback (err, result) {
    if (err) {
      //set query as an error property
      err.query = query;
    }
    callback(err, result);
  }
  this.connect(function afterConnect(err) {
    if (err) {
      return innerCallback(err);
    }
    if (options.prepare) {
      return self._executeAsPrepared(query, params, options, innerCallback);
    }
    try {
      encoder.setRoutingKey(params, options);
    }
    catch (err) {
      return innerCallback(err);
    }
    var request = new requests.QueryRequest(
      query,
      params,
      options);
    var handler = new RequestHandler(self, self.options);
    handler.send(request, options, innerCallback);
  });
};

/**
 * Prepares (the first time) and executes the prepared query, retrying on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} params Array of params
 * @param {Object} options
 * @param {ResultCallback} callback Executes callback(err, result) when finished
 * @private
 */
Client.prototype._executeAsPrepared = function (query, params, options, callback) {
  var self = this;
  async.waterfall([
    this.connect.bind(this),
    function (next) {
      self._getPrepared(query, next);
    },
    function (queryId, meta, next) {
      options = utils.extend({}, options, {hints: utils.toHint(meta.columns)});
      try {
        encoder.setRoutingKey(params, options);
      }
      catch (err) {
        return next(err);
      }
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
 * Parses and validates the arguments received by executeBatch
 * @private
 */
Client.prototype._parseBatchArgs = function (queries, options, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (args.length < 2 || typeof args[args.length-1] !== 'function') {
    throw new Error('It should contain at least 2 arguments, with the callback as the last argument.');
  }
  if (!util.isArray(queries)) {
    throw new Error('The first argument must be an Array of queries.');
  }
  if (args.length < 3) {
    callback = args[args.length-1];
    options = null;
  }
  args.queries = queries;
  args.options = options;
  args.callback = callback;
  return args;
};

/**
 * It returns the id of the prepared query.
 * If its not prepared, it prepares the query.
 * If its being prepared, it queues the callback
 * @param {String} query Query to prepare with ? as placeholders
 * @param {function} callback Executes callback(err, queryId) when there is a prepared statement on a connection or there is an error.
 * @private
 */
Client.prototype._getPrepared = function (query, callback) {
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
  handler.send(request, null, function (err, response) {
    info.preparing = false;
    if (err) {
      return info.emit('prepared', err);
    }
    info.queryId = response.id;
    info.meta = response.meta;
    info.emit('prepared', null, info.queryId, info.meta);
  });
};

/**
 * Sets the keyspace to the first connection available.
 * @param {Function} callback
 * @private
 */
Client.prototype._setKeyspaceFirst = function (callback) {
  var handler = new RequestHandler(this, this.options);
  //Until now the pool is in mint condition
  //When getting the next connection, it issues a USE keyspace if necessary
  handler.getNextConnection(null, function (err) {
    //In case there is an error it probably means that the keyspace could not be switched.
    callback(err);
  });
};

/**
 * Callback used by execution methods.
 * @callback ResultCallback
 * @param {Error} err Error occurred in the execution of the query.
 * @param {Object} result Result of the execution of the query.
 * @param {Array} result.rows Array of rows.
 */

module.exports = Client;