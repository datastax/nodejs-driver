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
}
//Represents a pool of connection to multiple hosts
function Client (options) {
  Client.super_.call(this);
  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  //create a connection foreach each host
  this.connections = [];
  this.options = utils.extend(options, optionsDefault);
  //current connection index
  this.connectionIndex = 0;
  //current connection index for prepared queries
  this.prepareConnectionIndex = 0;
  //an array containing the unhealthy connections
  this.unhealtyConnections = [];
  this.preparedQueries = {};
  
  var self = this;
  var connCount = 0;
  var poolSize = self.options.poolSize;
  while (connCount++ < poolSize) {

    options.hosts.forEach(function (hostPort, index){
      var host = hostPort.split(':');
      var connOptions = utils.extend(
        {host: host[0], port: isNaN(host[1]) ? 9042 : host[1]}, self.options
      );

      var c = new Connection(connOptions);
      c.indexInPool = ( (connCount-1) * poolSize) + index;
      self.connections.push(c);
    });

  };

  this.emit('log', 'info', this.connections.length + ' connections created across ' + options.hosts.length + ' hosts.');
}

util.inherits(Client, events.EventEmitter);

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
          self.setUnhealthy(c);
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
}

/** 
 * Connects to all hosts, in case the pool is disconnected. Callbacks.
 * @param {function} callback is called when the pool is connected (or at least 1 connected and the rest failed to connect) or it is not possible to connect 
 */
Client.prototype.connect = function (callback) {
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
}

/**
 * Gets a live connection
 * If there isn't an active connection available, it calls the callback with the error.
 */
Client.prototype.getAConnection = function (callback) {
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
      self.connectionIndex++;
      if (self.connectionIndex > self.connections.length-1) {
        self.connectionIndex = 0;
      }
      var c = self.connections[self.connectionIndex];
      if (self.isHealthy(c)) {
        callback(null, c);
      }
      else if (Date.now() - startTime > self.options.getAConnectionTimeout) {
        callback(new types.TimeoutError('Get a connection timed out'));
      }
      else if (self.canReconnect(c) && !c.connecting) {
        self.emit('log', 'info', 'Retrying to open #' + c.indexInPool);
        //try to reconnect
        c.open(function(err){
          if (err) {
            //This connection is still not good, go for the next one
            self.setUnhealthy(c);
            setImmediate(function () {
              checkNextConnection(callback);
            });
          }
          else {
            //this connection is now good
            self.setHealthy.call(self, c);
            callback(null, c);
          }
        });
      }
      else {
        //this connection is not good, try the next one
        setImmediate(function () {
          checkNextConnection(callback);
        });
      }
    }
    checkNextConnection(callback);
  });
}

/**
 * Gets a connection to prepare a query
 * If there is more than 1 healthy connection, 
 * it balances the amount of prepared queries per connection
 */
Client.prototype.getConnectionToPrepare = function (callback) {
  var self = this;
  self.connect(function (err) {
    if (err) {
      callback(err);
      return;
    }
    if (self.connections.length > self.unhealtyConnections.length) {
      var c = null;
      //closed loop
      while (c == null || !self.isHealthy(c)) {
        self.prepareConnectionIndex++;
        if (self.prepareConnectionIndex > self.connections.length-1) {
          self.prepareConnectionIndex = 0;
        }
        var c = self.connections[self.prepareConnectionIndex];
      }
      callback(null, c);
    }
    else {
      //the state of the pool is very bad, return the first healthy connection
      self.getAConnection(callback);
    }
  });
}

/**
 Executes a query in an available connection.
 @param {function} callback, executes callback(err, result) when finished
 */
Client.prototype.execute = function (query, args, consistency, callback) {
  if(typeof callback === 'undefined') {
    if (typeof consistency === 'undefined') {
      //only the query and the callback was specified
      callback = args;
      args = null;
    }
    else {
      callback = consistency;
      consistency = null;
      if (typeof args === 'number') {
        consistency = args;
        args = null;
      }
    }
  }
  var self = this;
  function tryAndRetry(retryCount) {
    retryCount = retryCount ? retryCount : 0;
    self.getAConnection(function(err, c) {
      if (err) {
        callback(err);
        return;
      }
      self.emit('log', 'info', 'connection #' + c.indexInPool + ' aquired, executing: ' + query);
      c.execute(query, args, consistency, function(err, result) {
        //Determine if its a fatal error
        if (self.isServerUnhealthy(err)) {
          //if its a fatal error, the server died
          self.setUnhealthy(c);
          if (retryCount === self.options.maxExecuteRetries) {
            callback(err, result, retryCount);
            return;
          }
          //retry: it will get another connection
          self.emit('log', 'error', 'There was an error executing a query, retrying execute (will get another connection)', err);
          tryAndRetry(retryCount+1);
        }
        else {
          //If the result is OK or there is error (syntax error or an unauthorized for example), callback
          callback(err, result);
        }
      });
    });
  };
  tryAndRetry(0);
}

Client.prototype.executeAsPrepared = function (query, args, consistency, callback) {
  if(typeof callback === 'undefined') {
    if (typeof consistency === 'undefined') {
      //only the query and the callback was specified
      callback = args;
      args = null;
    }
    else {
      callback = consistency;
      consistency = null;
      if (typeof args === 'number') {
        consistency = args;
        args = null;
      }
    }
  }
  var preparedInfo = this._getPreparedInfo(query);
  if (preparedInfo.queryId) {
    this.executeOnConnection(preparedInfo.connection, preparedInfo.query, preparedInfo.queryId, args, consistency, callback);
  }
  else {
    //The query is being prepared, queue it until is prepared.
    var self = this;
    preparedInfo.once('prepared', function (err) {
      if (err) {
        callback(err);
        return;
      }
      self.executeOnConnection.call(self, preparedInfo.connection, preparedInfo.query, preparedInfo.queryId, args, consistency, callback);
    });
  }
}

Client.prototype._getPreparedInfo = function (query) {
  var preparedInfo = this.preparedQueries[query.toLowerCase()];
  if (preparedInfo) {
    //it is prepared or preparing
    return preparedInfo;
  }
  preparedInfo = new events.EventEmitter();
  preparedInfo.setMaxListeners(0);
  preparedInfo.query = query;
  preparedInfo.queryId = null;
  this._setPrepared(preparedInfo);
  
  var self = this;
    //create info (just query)
    //when a connection is obtained => assign the connection info
    //when a query id is obtained => assign the queryId
  function tryAndRetryPrepare(retryCount) {
    self.emit('log', 'info', 'Preparing the query "' + query + '"');
    self.getConnectionToPrepare(function (err, c) {
      if (err) {
        preparedInfo.emit('prepared', err);
        self._removePrepared(preparedInfo);
        return;
      }
      preparedInfo.connection = c;
      self._setPrepared(preparedInfo);
      c.prepare(query, function (err, result) {
        if (self.isServerUnhealthy(err)) {
          //its a fatal error, the server died
          self.setUnhealthy(c);
          if (retryCount === self.options.maxExecuteRetries) {
            preparedInfo.emit('prepared', err);
            //It was already removed from context while it was set to unhealthy
            return;
          }
          //retry: it will get another connection
          self.emit('log', 'error', 'There was an error preparing a query, retrying execute (will get another connection)', err);
          tryAndRetryPrepare(retryCount+1);
        }
        else if (err) {
          //its syntax or other normal error
          preparedInfo.emit('prepared', err);
          self._removePrepared(preparedInfo);
        }
        else {
          preparedInfo.queryId = result.id;
          preparedInfo.emit('prepared', err);
        }
      });
    });
  }
  tryAndRetryPrepare(0);
  return preparedInfo;
}

/**
 * Executes a prepared query on a given connection
 */
Client.prototype.executeOnConnection = function (c, query, queryId, args, consistency, callback) {
  this.emit('log', 'info', 'Executing prepared query "' + query + '"');
  var self = this;
  c.executePrepared(queryId, args, consistency, function(err, result) {
    if (self.isServerUnhealthy(err)) {
      //There is a problem with the connection/server that had a prepared query
      //forget about this connection for now
      self.setUnhealthy(c);
      //retry the hole thing, it will get another connect
      self.executeAsPrepared(query, args, consistency, callback);
    }
    else {
      callback(err, result);
    }
  });
}

/**
 * Stored a prepared query information (queryId and connection) into the Client context
 * Indexed by query and connection (if available).
 */
Client.prototype._setPrepared = function (preparedInfo) {
  //index the object per connection and per query
  this.preparedQueries[preparedInfo.query.toLowerCase()] = preparedInfo;
  
  if (preparedInfo.connection) {
    var connectionKey = preparedInfo.connection.indexInPool.toString();
    if (!this.preparedQueries[connectionKey]) {
      this.preparedQueries[connectionKey] = [];
    }
    this.preparedQueries[connectionKey].push(preparedInfo);
  }
}

/**
 * Removes a prepared query
 */
Client.prototype._removePrepared = function (preparedInfo) {
  delete this.preparedQueries[query.toLowerCase()];
  if (prepareInfo.connection) {
    var connectionKey = prepareInfo.connection.indexInPool.toString();
    delete this.preparedQueries[connectionKey];
  }
}

/**
 * Removes all previously stored queries assigned to a connection
 */
Client.prototype._removeAllPrepared = function (connection) {
  var connectionKey = connection.indexInPool.toString()
  var preparedList = this.preparedQueries[connectionKey];
  if (!preparedList) {
    return;
  }
  preparedList.forEach(function (element){
    //remove by query
    delete this.preparedQueries[element.query.toLowerCase()];
  }, this);
  //remove by connection
  delete this.preparedQueries[connectionKey];
}

Client.prototype.isServerUnhealthy = function (err) {
  if (err && err.isServerUnhealthy) {
    return true;
  }
  else {
    return false;
  }
}

Client.prototype.setUnhealthy = function (connection) {
  connection.unhealthyAt = new Date().getTime();
  this.unhealtyConnections.push(connection);
  this._removeAllPrepared(connection);
  this.emit('log', 'error', 'Connection #' + connection.indexInPool + ' was set to Unhealthy');
}

Client.prototype.setHealthy = function (connection) {
  connection.unhealthyAt = 0;
  var i = this.unhealtyConnections.indexOf(connection);
  if (i >= 0) {
    this.unhealtyConnections.splice(i, 1);
  }
  this.emit('log', 'info', 'Connection #' + connection.indexInPool + ' was set to healthy');
}

Client.prototype.canReconnect = function (connection) {
  var timePassed = new Date().getTime() - connection.unhealthyAt;
  return timePassed > this.options.staleTime;
}

/**
 * Determines if a connection can be used
*/
Client.prototype.isHealthy = function (connection) {
  return !connection.unhealthyAt;
}

/**
 * Closes all connections
 */
Client.prototype.shutdown = function (callback) {
  async.each(this.connections, function(c, eachCallback) {
    c.close(eachCallback);
  },
    function() {
      callback();
    }
  );
}

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