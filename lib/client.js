var events = require('events');
var util = require('util');
var async = require('async');

var Connection = require('./connection.js').Connection;
var utils = require('./utils.js');
var types = require('./types.js');
var ControlConnection = require('./control-connection.js');
var RequestHandler = require('./request-handler.js');
var writers = require('./writers.js');
var clientOptions = require('./client-options.js');

/**
 * Represents a pool of connection to multiple hosts
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.options = clientOptions.extend(options);
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
 * @param {function} callback Executes callback(err, result) when finished
 */
Client.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  args.options = utils.extend({}, this.options.queryOptions, args.options);
  var self = this;
  this.connect(function (err) {
    if (err) return callback(err);
    if (args.options.prepare) {
      return self.executeAsPrepared(args.query, args.params, args.options, args.callback);
    }
    var request = new writers.QueryWriter(
      args.query,
      args.params,
      args.options.consistency,
      args.options.fetchSize,
      args.options.pageState);
    var handler = new RequestHandler(self, self.options);
    handler.send(request, args.options, args.callback);
  });
};

/**
 * Prepares (the first time) and executes the prepared query, retrying on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} params Array of params
 * @param {Object} [options]
 * @param {Function} callback Executes callback(err, result) when finished
 */
Client.prototype.executeAsPrepared = function (query, params, options, callback) {
  var self = this;
  async.waterfall([
    this.connect.bind(this),
    function (next) {
      self._getPrepared(query, next);
    },
    function (queryId, next) {
      var request = new writers.ExecuteWriter(
        queryId,
        params,
        options.consistency,
        options.fetchSize,
        options.pageState);
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
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.executeBatch = function () {
  var args = this._parseBatchArgs.apply(null, arguments);
  //Get stack trace before sending request
  var stackContainer = {};
  Error.captureStackTrace(stackContainer);
  var executeError;
  var retryCount = 0;
  var self = this;
  async.doWhilst(
    function iterator(next) {
      self._getAConnection(function(err, c) {
        executeError = err;
        if (err) {
          //exit the loop
          return next(err);
        }
        self.emit('log', 'info', util.format('connection #%d acquired, executing batch', c.indexInPool));
        c.executeBatch(args.queries, args.consistencies, args.options, function (err) {
          if (self._isServerUnhealthy(err)) {
            self._setUnhealthy(c);
          }
          executeError = err;
          next();
        });
      });
    },
    function condition() {
      retryCount++;
      //retry in case the node went down
      return self._isServerUnhealthy(executeError) && retryCount < self.options.maxExecuteRetries;
    },
    function loopFinished() {
      if (executeError) {
        utils.fixStack(stackContainer.stack, executeError);
      }
      args.callback(executeError, retryCount);
    }
  );
};

//noinspection JSValidateJSDoc,JSCommentMatchesSignature
/**
 * Prepares (the first time on each host), executes the prepared query and streams the last field of each row.
 * It executes the callback per each row as soon as the first chunk of the last field is received.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param {Number} [consistency] Consistency level
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
  this.executeAsPrepared(args.query, args.params, args.consistencies, args.options, args.callback);
};

/**
 * Parses and validates the arguments received by executeBatch
 */
Client.prototype._parseBatchArgs = function (queries, consistency, options, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (args.length < 2 || typeof args[args.length-1] !== 'function') {
    throw new Error('It should contain at least 2 arguments, with the callback as the last argument.');
  }
  if (!util.isArray(queries)) {
    throw new Error('The first argument must be an Array of queries.');
  }
  if (args.length < 4) {
    callback = args[args.length-1];
    options = null;
    if (args.length < 3) {
      consistency = null;
    }
  }
  args.queries = queries;
  args.consistencies = consistency;
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
    return callback(null, info.queryId);
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
    callback(null, info.queryId);
    info.emit('prepared', null, info.queryId);
  });
};

Client.prototype.log = function (type, info, furtherInfo) {
  this.emit('log', type, info, 'Client', furtherInfo || '');
};


Client.prototype._isServerUnhealthy = function (err) {
  return err && err.isServerUnhealthy;
};

Client.prototype._setUnhealthy = function (connection) {
  if (!connection.unhealthyAt) {
    this.emit('log', 'error', 'Connection #' + connection.indexInPool + ' is being set to Unhealthy');
    connection.unhealthyAt = new Date().getTime();
  }
};

Client.prototype._setHealthy = function (connection) {
  connection.unhealthyAt = 0;
  this.emit('log', 'info', 'Connection #' + connection.indexInPool + ' was set to healthy');
};

Client.prototype._canReconnect = function (connection) {
  var timePassed = new Date().getTime() - connection.unhealthyAt;
  return timePassed > this.options.staleTime;
};

/**
 * Determines if a connection can be used
*/
Client.prototype._isHealthy = function (connection) {
  return !connection.unhealthyAt;
};

/**
 * Closes all connections
 */
Client.prototype.shutdown = function (callback) {
  async.each(this.connections, function(c, eachCallback) {
    c.close(eachCallback);
  }, function() {
    if (callback) {
      callback();
    }
  });
};

/**
 * Represents a error while trying to connect the pool, all the connections failed.
 */
function PoolConnectionError(individualErrors) {
  this.name = 'PoolConnectionError';
  this.info = 'Represents a error while trying to connect the pool, all the connections failed.';
  this.individualErrors = individualErrors;
}
util.inherits(PoolConnectionError, Error);

module.exports = Client;