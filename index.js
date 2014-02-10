var events = require('events');
var util = require('util');
var async = require('async');
var Connection = require('./lib/connection.js').Connection;
var utils = require('./lib/utils.js');
var types = require('./lib/types.js');

var optionsDefault = {
  version: '3.0.0',
  //max simultaneous requests (before waiting for a response) (max=128) on each connection
  maxRequests: 32,
  //When the simultaneous requests has been reached, it determines the amount of milliseconds before retrying to get an available streamId
  maxRequestsRetry: 100,
  //Time that has to pass before trying to reconnect
  staleTime: 1000,
  //maximum amount of times an execute can be retried (using another connection) because of an unhealthy server response
  maxExecuteRetries: 3,
  //maximum time (in milliseconds) to get a healthy connection from the pool. It should be connection Timeout * n.
  getAConnectionTimeout: 3500,
  //number of connections to open for each host
  poolSize: 1
};
//Represents a pool of connection to multiple hosts
function Client(options) {
  events.EventEmitter.call(this);
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.options = utils.extend({}, optionsDefault, options);
  //current connection index
  this.connectionIndex = 0;
  //current connection index for prepared queries
  this.prepareConnectionIndex = 0;
  this.preparedQueries = {};
  
  this._createPool();
}

util.inherits(Client, events.EventEmitter);

/**
 * Creates the pool of connections suitable for round robin
 */
Client.prototype._createPool = function () {
  this.connections = [];
  for (var poolIndex = 0; poolIndex < this.options.poolSize; poolIndex++) {
    for (var i = 0; i < this.options.hosts.length; i++) {
      var host = this.options.hosts[i].split(':');
      var connOptions = utils.extend({}, this.options, {host: host[0], port: host[1] || 9042});
      var c = new Connection(connOptions);
      c.indexInPool = (this.options.poolSize * poolIndex) + i;
      this.connections.push(c);
    }
  }

  this.emit('log', 'info', this.connections.length + ' connections created across ' + this.options.hosts.length + ' hosts.');
};

/**
 * Connects to each host
 */
Client.prototype._connectAllHosts = function (connectCallback) {
  this.emit('log', 'info', 'Connecting to all hosts');
  var errors = [];
  this.connecting = true;
  var self = this;
  async.each(this.connections, 
    function (c, callback) {
      c.open(function (err) {
        if (err) {
          self._setUnhealthy(c);
          errors.push(err);
          self.emit('log', 'error', 'There was an error opening connection #' + c.indexInPool, err);
        }
        else {
          self.emit('log', 'info', 'Opened connection #' + c.indexInPool);
        }
        callback();
      });
    },
    function () {
      self.connecting = false;
      var error = null;
      if (errors.length === self.connections.length) {
        error = new PoolConnectionError(errors);
      }
      self.connected = !error;
      connectCallback(error);
      self.emit('connection', error);
    });
};

/** 
 * Connects to all hosts, in case the pool is disconnected.
 * @param {function} callback is called when the pool is connected (or at least 1 connected and the rest failed to connect) or it is not possible to connect 
 */
Client.prototype.connect = function (callback) {
  if (!callback) {
    callback = function () {};
  }
  if (this.connected || this.connectionError) {
    callback(this.connectionError);
    return;
  }
  if (this.connecting) {
    //queue while is connecting
    this.emit('log', 'info', 'Waiting for the pool to connect');
    this.once('connection', callback);
    return;
  }
  //it is the first time. Try to connect to all hosts
  var self = this;
  this.connecting = true;
  this._connectAllHosts(function (err) {
    if (err) {
      self.connectionError = err;
    }
    callback(err);
  });
};

/**
 * Gets a live connection
 * If there isn't an active connection available, it calls the callback with the error.
 */
Client.prototype._getAConnection = function (callback) {
  var self = this;
  self.connect(function (err) {
    if (err) {
      callback(err);
      return;
    }
    //go through the connections
    //watch out for infinite loops
    var startTime = Date.now();
    function checkNextConnection (callback) {
      self.emit('log', 'info', 'Checking next connection');
      self.connectionIndex = self.connectionIndex + 1;
      if (self.connectionIndex > self.connections.length-1) {
        self.connectionIndex = 0;
      }
      var c = self.connections[self.connectionIndex];
      if (self._isHealthy(c)) {
        callback(null, c);
      }
      else if (Date.now() - startTime > self.options.getAConnectionTimeout) {
        callback(new types.TimeoutError('Get a connection timed out'));
      }
      else if (!c.connecting && self._canReconnect(c)) {
        self.emit('log', 'info', 'Retrying to open #' + c.indexInPool);
        //try to reconnect
        c.open(function(err){
          if (err) {
            //This connection is still not good, go for the next one
            self._setUnhealthy(c);
            setTimeout(function () {
              checkNextConnection(callback);
            }, 10);
          }
          else {
            //this connection is now good
            self._setHealthy(c);
            callback(null, c);
          }
        });
      }
      else {
        //this connection is not good, try the next one
        setTimeout(function () {
          checkNextConnection(callback);
        }, 10);
      }
    }
    checkNextConnection(callback);
  });
};

/**
 * Executes a query on an available connection.
 * @param {String} query The query to execute
 * @param {Array} [param] Array of params to replace
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} callback Executes callback(err, result) when finished
 */
Client.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  //Get stack trace before sending request
  var stackContainer = {};
  Error.captureStackTrace(stackContainer);
  var executeError;
  var retryCount = 0;
  var self = this;
  var executionResult;
  async.doWhilst(
    function iterator(next) {
      self._getAConnection(function(err, c) {
        executeError = err;
        if (err) {
          //exit the loop
          return next(err);
        }
        self.emit('log', 'info', util.format('connection #%d acquired, executing query: %s', c.indexInPool, args.query));
        c.execute(args.query, args.params, args.consistency, function(err, result) {
          if (self._isServerUnhealthy(err)) {
            self._setUnhealthy(c);
          }
          executeError = err;
          executionResult = result;
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
        executeError.query = args.query;
      }
      args.callback(executeError, executionResult, retryCount);
    }
  );
};

/**
 * Prepares (the first time) and executes the prepared query, retrying on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} callback Executes callback(err, result) when finished
 */
Client.prototype.executeAsPrepared = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  var self = this;
  //Get stack trace before sending query so the user knows where errored
  //queries come from
  var stackContainer = {};
  Error.captureStackTrace(stackContainer);

  self._getPrepared(args.query, function preparedCallback(err, con, queryId) {
    if (self._isServerUnhealthy(err)) {
      //its a fatal error, the server died
      self._setUnhealthy(con);
      args.options = utils.extend({retryCount: 0}, args.options);
      if (args.options.retryCount === self.options.maxExecuteRetries) {
        return args.callback(err);
      }
      //retry: it will get another connection
      self.emit('log', 'info', 'Retrying to prepare "' + args.query + '"');
      args.options.retryCount = args.options.retryCount + 1;
      self.executeAsPrepared(args.query, args.params, args.consistency, args.options, args.callback);
    }
    else if (err) {
      //its syntax or other normal error
      utils.fixStack(stackContainer.stack, err);
      err.query = args.query;
      if (args.options && args.options.resultStream) {
        args.options.resultStream.emit('error', err);
      }
      args.callback(err);
    }
    else {
      //it is prepared on the connection
      self._executeOnConnection(con, args.query, queryId, args.params,args.consistency, args.options, args.callback);
    }
  });
};

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
  this.executeAsPrepared(args.query, args.params, args.consistency, args.options, args.callback);
};

/**
 * Prepares (the first time), executes the prepared query and calls rowCallback for each row as soon as they are received.
 * Calls endCallback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} rowCallback, executes callback(n, row) per each row received. (n = index)
 * @param {function} [endcallback], executes endCallback(err, totalCount) after all rows have been received.
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
  this.executeAsPrepared(args.query, args.params, args.consistency, args.options, args.callback);
};


/**
 * Prepares (the first time), executes the prepared query and pushes the rows to the result stream
 *  as soon as they received.
 * Calls callback after all rows have been sent, or when there is an error.
 * Retries on multiple hosts if needed.
 * @param {String} query The query to prepare and execute
 * @param {Array} [param] Array of params
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} [callback], executes callback(err) after all rows have been received or if there is an error
 * @returns {ResultStream}
 */
Client.prototype.stream = function () {
  var args = Array.prototype.slice.call(arguments);
  if (typeof args[args.length-1] !== 'function') {
    //the callback is not required
    args.push(function noop() {});
  }
  args = utils.parseCommonArgs.apply(null, args);
  var resultStream = new types.ResultStream({objectMode: 1})
  args.options = utils.extend({}, args.options, {resultStream: resultStream});
  this.executeAsPrepared(args.query, args.params, args.consistency, args.options, args.callback);
  return resultStream;
};

Client.prototype.streamRows = Client.prototype.eachRow;

/**
 * Executes batch of queries on an available connection.
 * If the Cassandra node does down before responding, it retries the batch.
 * @param {Array} queries The query to execute
 * @param {Number} [consistency] Consistency level
 * @param [options]
 * @param {function} callback Executes callback(err, result) when the batch was executed
 */
Client.prototype.executeBatch = function (queries, consistency, options, callback) {
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
        c.executeBatch(args.queries, args.consistency, args.options, function (err) {
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
  args.consistency = consistency;
  args.options = options;
  args.callback = callback;
  return args;
};

/**
 * Executes a prepared query on a given connection
 */
Client.prototype._executeOnConnection = function (c, query, queryId, params, consistency, options, callback) {
  this.emit('log', 'info', 'Executing prepared query "' + query + '"');
  var self = this;
  c.executePrepared(queryId, params, consistency, options, function(err, result1, result2) {
    if (self._isServerUnhealthy(err)) {
      //There is a problem with the connection/server that had a prepared query
      //forget about this connection for now
      self._setUnhealthy(c);
      //retry the whole thing, it will get another connection
      self.executeAsPrepared(query, params, consistency, options, callback);
    }
    else if (err && err.code === types.responseErrorCodes.unprepared) {
      //Query expired at the server
      //Clear the connection from prepared info and
      //trying to re-prepare query
      self.emit('log', 'info', 'Unprepared query "' + query + '"');
      var preparedInfo = self.preparedQueries[query];
      preparedInfo.removeConnectionInfo(c.indexInPool);
      self.executeAsPrepared(query, params, consistency, options, callback);
    }
    else {
      callback(err, result1, result2);
    }
  });
};

/**
 * It gets an active connection and prepares the query on it, queueing the callback in case its already prepared.
 * @param {String} query Query to prepare with ? as placeholders
 * @param {function} callback Executes callback(err, con, queryId) when there is a prepared statement on a connection or there is an error.
 */
Client.prototype._getPrepared = function (query, callback) {
  var preparedInfo = this.preparedQueries[query];
  if (!preparedInfo) {
    preparedInfo = new PreparedInfo(query);
    this.preparedQueries[query] = preparedInfo;
  }
  var self = this;
  this._getAConnection(function(err, con) {
    if (err) {
      return callback(err);
    }
    var conInfo = preparedInfo.getConnectionInfo(con.indexInPool);
    if (conInfo.queryId !== null) {
      //is already prepared on this connection
      return callback(null, con, conInfo.queryId);
    }
    else if (conInfo.preparing) {
      //Its being prepared, queue until finish
      return conInfo.once('prepared', callback);
    }
    //start preparing
    conInfo.preparing = true;
    conInfo.once('prepared', callback);
    return self._prepare(conInfo, con, query);
  });
};

/**
 * Prepares a query on a connection. If it fails (server unhealthy) it retries all the preparing process with a new connection.
 */
Client.prototype._prepare = function (conInfo, con, query) {
  this.emit('log', 'info', 'Preparing the query "' + query + '" on connection #' + con.indexInPool);
  var self = this;
  con.prepare(query, function (err, result) {
    conInfo.preparing = false;
    if (!err) {
      self._setAsPrepared(conInfo, query, result.id);
    }
    conInfo.emit('prepared', err, con, result ? result.id : null);
  });
};


Client.prototype._setAsPrepared = function (conInfo, query, queryId) {
  conInfo.queryId = queryId;
  var preparedOnConnection = this.preparedQueries["_" + conInfo.id];
  if (!preparedOnConnection) {
    preparedOnConnection = [];
    this.preparedQueries["_" + conInfo.id] = preparedOnConnection;
  }
  preparedOnConnection.push(query);
};
/**
 * Removes all previously stored queries assigned to a connection
 */
Client.prototype._removeAllPrepared = function (con) {
  var conKey = "_" + con.indexInPool;
  var preparedOnConnection = this.preparedQueries[conKey];
  if (!preparedOnConnection) {
    return;
  }
  for (var i = 0; i < preparedOnConnection.length; i++) {
    var query = preparedOnConnection[i];
    this.preparedQueries[query].removeConnectionInfo(con.indexInPool);
  }
  this.emit('log', 'info', 'Removed ' + preparedOnConnection.length + ' prepared queries for con #' + con.indexInPool);
  delete this.preparedQueries[conKey];
};

Client.prototype._isServerUnhealthy = function (err) {
  return err && err.isServerUnhealthy;
};

Client.prototype._setUnhealthy = function (connection) {
  if (!connection.unhealthyAt) {
    this.emit('log', 'error', 'Connection #' + connection.indexInPool + ' is being set to Unhealthy');
    connection.unhealthyAt = new Date().getTime();
    this._removeAllPrepared(connection);
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
 * Holds the information of the connections in which a query is prepared 
 */
function PreparedInfo(query) {
  this.query = query;
  //stores information for the prepared statement on a connection
  this._connectionData = {};
}

PreparedInfo.prototype.getConnectionInfo = function (conId) {
  conId = conId.toString();
  var info = this._connectionData[conId];
  if (!info) {
    info = new events.EventEmitter();
    info.setMaxListeners(0);
    info.preparing = false;
    info.queryId = null;
    info.id = conId;
    this._connectionData[conId] = info;
  }
  return info;
};

PreparedInfo.prototype.removeConnectionInfo = function (conId) {
  delete this._connectionData[conId];
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

exports.Client = Client;
exports.Connection = Connection;
exports.types = types;
