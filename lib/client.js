var events = require('events');
var util = require('util');
var async = require('async');

var utils = require('./utils.js');
var types = require('./types.js');
var ControlConnection = require('./control-connection.js');
var RequestHandler = require('./request-handler.js');
var writers = require('./writers.js');
var clientOptions = require('./client-options.js');

/**
 * Client options
 * @typedef {Object} ClientOptions
 * @property {{loadBalancing, retry, reconnection}} policies
 * @property {{consistency, fetchSize, prepare}} queryOptions
 * @property {Object} pooling
 * @property {{port}} protocolOptions
 * @property {{connectTimeout: number}} socketOptions
 * @property {AuthProvider} authProvider
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
    if (err) return callback(err);
    //we have all the data from the cluster
    self.hosts = self.controlConnection.hosts;
    self.options.policies.loadBalancing.init(self, self.hosts, function (err) {
      self.connected = !err;
      self.connecting = false;
      callback(err);
      self.emit('connected', err);
    });
  });
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Executes a query on an available connection.
 * @param {String} query The query to execute
 * @param {Array} [params] Array of params to replace
 * @param {Object} [options]
 * @param {resultCallback} callback Executes callback(err, result) when finished
 */
Client.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  args.options = utils.extend({}, this.options.queryOptions, args.options);
  var self = this;
  this.connect(function (err) {
    if (err) {
      return args.callback(err);
    }
    if (args.options.prepare) {
      return self.executeAsPrepared(args.query, args.params, args.options, args.callback);
    }
    var request = new writers.QueryWriter(
      args.query,
      args.params,
      args.options);
    var handler = new RequestHandler(self, self.options);
    handler.send(request, args.options, args.callback);
  });
};

/**
 * Prepares (the first time) and executes the prepared query, retrying on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} params Array of params
 * @param {Object} [options]
 * @param {resultCallback} callback Executes callback(err, result) when finished
 */
Client.prototype.executeAsPrepared = function (query, params, options, callback) {
  var self = this;
  async.waterfall([
    this.connect.bind(this),
    function (next) {
      self._getPrepared(query, next);
    },
    function (queryId, meta, next) {
      options = utils.extend({}, options, {hints: utils.toHint(meta.columns)});
      var request = new writers.ExecuteWriter(
        queryId,
        params,
        options);
      request.query = query;
      var handler = new RequestHandler(self, self.options);
      handler.send(request, options, next);
    }
  ], callback);
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Prepares (the first time), executes the prepared query and calls rowCallback for each row as soon as they are received.
 * Calls endCallback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param [options]
 * @param {function} rowCallback, executes callback(n, row) per each row received. (n = index)
 * @param {function} [callback], executes endCallback(err, totalCount) after all rows have been received.
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
    rowCallback: rowCallback
  });
  this.execute(args.query, args.params, args.options, args.callback);
};


//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Prepares (the first time), executes the prepared query and pushes the rows to the result stream
 *  as soon as they received.
 * Calls callback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param [options]
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
 * @param [options]
 * @param {resultCallback} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function () {
  var args = this._parseBatchArgs.apply(null, arguments);
  //logged batch by default
  args.options = utils.extend({logged: true}, this.options.queryOptions, args.options);
  var request = new writers.BatchWriter(args.queries, args.options.consistency, args.options);
  var handler = new RequestHandler(this, this.options);
  handler.send(request, null, args.callback);
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Prepares (the first time on each host), executes the prepared query and streams the last field of each row.
 * It executes the callback per each row as soon as the first chunk of the last field is received.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param [options]
 * @param {function} rowCallback Executes rowCallback(n, row, fieldStream) per each row
 * @param {function} [callback] Executes callback(err) when finished or there is an error
 */
Client.prototype.streamField = function () {
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
    streamField: true,
    rowCallback: rowCallback
  });
  this.executeAsPrepared(args.query, args.params, args.options, args.callback);
};

/**
 * Parses and validates the arguments received by executeBatch
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
  var name = this.keyspace || '' + query;
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
  if (info.preparing) {
    return info.once('prepared', callback);
  }
  info.preparing = true;
  var request = new writers.PrepareQueryWriter(query);
  var handler = new RequestHandler(this, this.options);
  handler.send(request, null, function (err, response) {
    if (err) return callback(err);
    info.preparing = false;
    info.queryId = response.id;
    info.meta = response.meta;
    callback(null, info.queryId, info.meta);
    info.emit('prepared', null, info.queryId, info.meta);
  });
};

Client.prototype.log = utils.log;

/**
 * Closes all connections to all hosts
 * @param {Function} [callback]
 */
Client.prototype.shutdown = function (callback) {
  //Go through all the host and shut down their pools
  if (!callback) {
    callback = function() {};
  }
  var hosts = this.hosts.slice(0);
  async.each(hosts, function (h, next) {
    h.pool.shutdown(next);
  }, callback);
};

/**
 * Callback used by execution methods.
 * @callback resultCallback
 * @param {Error} err
 * @param {Object} result
 */
module.exports = Client;