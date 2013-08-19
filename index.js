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
  getAConnectionTimeout: 3500
}
//Represents a pool of connection to multiple hosts
function Client (options) {
  Client.super_.call(this);
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
  options.hosts.forEach(function (hostPort, index){
    var host = hostPort.split(':');
    var connOptions = utils.extend(
      {host: host[0], port: isNaN(host[1]) ? 9042 : host[1]}, self.options
    );
    var c = new Connection(connOptions);
    c.indexInPool = index;
    self.connections.push(c);
  });
}

util.inherits(Client, events.EventEmitter);

/**
 * Connects to each host
 */
Client.prototype.connect = function (connectCallback) {
  var errors = [];
  var self = this;
  self.emit('log', 'info', 'Connecting to all hosts');
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
      if (errors.length === self.connections.length) {
        var error = new PoolConnectionError(errors);
        connectCallback(error);
      }
      else {
        self.connected = true;
        connectCallback();
      }
    });
}

/** 
 * Ensure that the pool is connected.
 * @param {function} callback is called when all the connections in the pool are connected (or at least 1 connected and the rest failed to connect)
 */
Client.prototype.ensurePoolConnection = function (callback) {
  var self = this;
  if (!this.connected) {
    if (this.connecting && !self.connectionError) {
      async.whilst(
        function() {
          return self.connecting && !self.connectionError;
        },
        function(cb) {
          //let it snow until are connections are set
          setTimeout(function(){
            self.emit('log', 'info', 'Waiting for pool to connect');
            cb();
          }, 100);
        },
        function(err) {
          if (!err && self.connectionError) {
            //When there was a previous error connecting the pool, the method should always return an error
            err = new PoolConnectionError();
          }
          callback(err);
        }
      );
    }
    else {
      //avoid retrying
      self.connecting = true;
      this.connect(function(err){
        if (err) {
          self.connectionError = true;
        }
        callback(err);
      });
    }
  }
  else {
    callback();
  }
}

/**
 * Gets a live connection
 * If there isn't an active connection available, it calls the callback with the error.
 */
Client.prototype.getAConnection = function (callback) {
  var self = this;
  self.ensurePoolConnection(function (err) {
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
  self.ensurePoolConnection(function (err) {
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
  var queryId = null;
  //the connection to execute 
  var c = null;
  var self = this;
  //check if prepared
  var preparedInfo = self.getPrepared(query);
  if (!preparedInfo) {
    function tryAndRetryPrepare(retryCount) {
      self.getConnectionToPrepare(function (err, c) {
        if (err) {
          callback(err);
          return;
        }
        c.prepare(query, function (err, result) {
          if (self.isServerUnhealthy(err)) {
            //if its a fatal error, the server died
            self.setUnhealthy(c);
            if (retryCount === self.options.maxExecuteRetries) {
              callback(err, result, retryCount);
              return;
            }
            //retry: it will get another connection
            self.emit('log', 'error', 'There was an error executing a query, retrying execute (will get another connection)', err);
            tryAndRetryPrepare(retryCount+1);
          }
          else if (err) {
            //its syntax or other normal error
            callback(err);
          }
          else {
            queryId = result.id;
            self.setPrepared(c, query, queryId);
            self.executeOnConnection(c, queryId, args, consistency, callback);
          }
        });
      });
    }
    tryAndRetryPrepare(0);
  }
  else {
    //get the connection that executed the query
    var c = preparedInfo.connection;
    queryId = preparedInfo.queryId;
    self.executeOnConnection(c, queryId, args, consistency, callback);
  }
}

/**
 * Executes a prepared query on a given connection
 */
Client.prototype.executeOnConnection = function (c, queryId, args, consistency, callback) {
  //TODO: Retry
  //function tryAndRetryExecute (retryCount) {
    c.executePrepared(queryId, args, consistency, function(err, result) {
      if (err) {
        //TODO: check
        //there was an error on the connection that had a prepared query
        //clean the stored query
        //retry the hole thing
      }
      callback(err, result);
    });
  //}
  
  //tryAndRetryExecute(0);
}

/**
 * Stored a queryId, query and connection of a prepared query
 */
Client.prototype.setPrepared = function (connection, query, queryId) {
  var preparedInfo = {
    connection: connection,
    query: query,
    queryId: queryId};
  //index the object per connection and per query
  var connectionKey = connection.indexInPool.toString();
  if (!this.preparedQueries[connectionKey]) {
    this.preparedQueries[connectionKey] = [];
  }
  this.preparedQueries[connectionKey].push(preparedInfo);
  this.preparedQueries[query.toLowerCase()] = preparedInfo;
}

Client.prototype.getPrepared = function (query) {
  return this.preparedQueries[query.toLowerCase()];
}

Client.prototype.removePrepared = function (connection) {
  var connectionKey = connection.indexInPool.toString()
  var preparedList = this.preparedQueries[connectionKey];
  if (!preparedList) {
    return;
  }
  preparedList.forEach(function (element){
    //remove by query
    delete this.preparedQueries[preparedInfo.query.toLowerCase()];
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
  this.removePrepared(connection);
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