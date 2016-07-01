"use strict";
var util = require('util');
var events = require('events');

var Connection = require('./connection');
var utils = require('./utils');
var defaultOptions = require('./client-options').defaultOptions();

// Used to get the index of the connection with less in-flight requests
var connectionIndex = 0;
var connectionIndexOverflow = Math.pow(2, 15);

/**
 * Represents a pool of connections to a host
 * @param {Host} host
 * @param {Number} protocolVersion Initial protocol version
 * @extends EventEmitter
 * @constructor
 */
function HostConnectionPool(host, protocolVersion) {
  events.EventEmitter.call(this);
  this._address = host.address;
  this.options = host.options;
  this._newConnectionTimeout = null;
  this._creating = false;
  this.protocolVersion = protocolVersion;
  this.coreConnectionsLength = 1;
  /**
   * An immutable array of connections
   * @type {Array.<Connection>}
   */
  this.connections = utils.emptyArray;
  this.shuttingDown = false;
  this.setMaxListeners(0);
}

util.inherits(HostConnectionPool, events.EventEmitter);

/**
 * @param {Function} callback
 */
HostConnectionPool.prototype.borrowConnection = function (callback) {
  var self = this;
  this.create(false, function (err) {
    if (err) {
      return callback(err);
    }
    if (self.connections.length === 0) {
      //something happen in the middle between the creation pool and now.
      err = Error('No connection available');
      err.isSocketError = true;
      return callback(err);
    }
    var c = HostConnectionPool.minInFlight(self.connections);
    callback(null, c);
  });
};

/**
 * Gets the connection with the minimum number of in-flight requests.
 * Only checks for index + 1 and index, to avoid a loop through all the connections.
 * @param {Array.<Connection>} connections
 * @returns {Connection}
 */
HostConnectionPool.minInFlight = function(connections) {
  var length = connections.length;
  if (length === 1) {
    return connections[0];
  }
  var index = ++connectionIndex;
  if (connectionIndex >= connectionIndexOverflow) {
    connectionIndex = 0;
  }
  var current = connections[index % length];
  var previous = connections[(index - 1) % length];
  if (previous.getInFlight() < current.getInFlight()) {
    return previous;
  }
  return current;
};

/**
 * Create the min amount of connections, if the pool is empty.
 * @param {Boolean} warmup Determines if all connections must be created before invoking the callback
 * @param {Function} callback
 */
HostConnectionPool.prototype.create = function (warmup, callback) {
  // The value of this.coreConnectionsLength can change over time
  // when an existing pool is being resized (by setting the distance).
  if (this.connections.length >= this.coreConnectionsLength) {
    return callback();
  }
  if (!warmup && this.connections.length > 0) {
    // we already have a valid connection
    // let the connection grow continue in the background
    this.increaseSize();
    return callback();
  }
  this.once('creation', callback);
  if (this._creating) {
    // wait for the pool to be creating
    return;
  }
  this._creating = true;
  var connectionsToCreate = this.coreConnectionsLength;
  if (!warmup) {
    connectionsToCreate = 1;
  }
  var self = this;
  utils.whilst(
    function condition() {
      return self.connections.length < connectionsToCreate;
    },
    function iterator(next) {
      self._attemptNewConnection(next);
    }, function whilstEnded(err) {
      self._creating = false;
      if (err && self.connections.length === 0) {
        // there was an error and no connections could be successfully opened
        self.log('warning', util.format('Connection pool to host %s could not be created', self._address));
        return self.emit('creation', err);
      }
      self.log('info', util.format('Connection pool to host %s created with %d connection(s)',
        self._address, self.connections.length));
      self.emit('creation');
      self.increaseSize();
    });
};

/** @returns {Connection} */
HostConnectionPool.prototype._createConnection = function () {
  var c = new Connection(this._address, this.protocolVersion, this.options);
  var self = this;
  //Relay the event idleRequestError
  c.on('idleRequestError', function idleErrorCallback(err, connection) {
    //The pool will emit the event
    self.emit('idleRequestError', err, connection);
  });
  return c;
};

/**
 * Prevents reconnection timeout from triggering
 */
HostConnectionPool.prototype.clearNewConnectionAttempt = function () {
  if (!this._newConnectionTimeout) {
    return;
  }
  clearTimeout(this._newConnectionTimeout);
  this._newConnectionTimeout = null;
};

/**
 * @param {Function} callback
 */
HostConnectionPool.prototype._attemptNewConnection = function (callback) {
  if(this.shuttingDown) {
    callback(Error("Pool is shutting down"));
  }
  var c = this._createConnection();
  var self = this;
  this.once('open', callback);
  if (this._opening) {
    // wait for the event to fire
    return;
  }
  this._opening = true;
  c.open(function attemptOpenCallback(err) {
    self._opening = false;
    if (err) {
      self.log('warning', util.format('Connection to %s could not be created: %s', self._address, err), err);
      c.close();
      return self.emit('open', err);
    }
    // close the connection if pool was shutdown.
    if(self.shuttingDown) {
      c.close();
      return self.emit('open', Error("Pool is shutting down"));
    }
    // use a copy of the connections array
    var newConnections = self.connections.slice(0);
    newConnections.push(c);
    self.connections = newConnections;
    self.log('info', util.format('Connection to %s opened successfully', self._address));
    self.emit('open', null, c);
  });
};

/**
 * Closes the connection and removes a connection from the pool.
 * @param {Connection} connection
 */
HostConnectionPool.prototype.remove = function (connection) {
  // locating an object by position in the array is O(n), but normally there should be between 1 to 8 connections.
  var index = this.connections.indexOf(connection);
  if (index < 0) {
    // it was already removed from the connections and it's closing
    return;
  }
  // remove the connection from the pool, using an pool copy
  var newConnections = this.connections.slice(0);
  newConnections.splice(index, 1);
  this.connections = newConnections;
  // close the connection
  setImmediate(function removeClose() {
    connection.close();
  });
};

/**
 * @param {Number} delay
 */
HostConnectionPool.prototype.scheduleNewConnectionAttempt = function (delay) {
  if (this.shuttingDown) {
    return;
  }
  var self = this;
  this._newConnectionTimeout = setTimeout(function newConnectionTimeoutExpired() {
    self._newConnectionTimeout = null;
    if (self.connections.length >= self.coreConnectionsLength) {
      // new connection can be scheduled while a new connection is being opened
      // the pool has the appropriate size
      return;
    }
    self._attemptNewConnection(utils.noop);
  }, delay);
};

HostConnectionPool.prototype.hasScheduledNewConnection = function () {
  return !!this._newConnectionTimeout || this._opening;
};

/**
 * Increases the size of the connection pool in the background, if needed.
 */
HostConnectionPool.prototype.increaseSize = function () {
  if (this.connections.length < this.coreConnectionsLength && !this.hasScheduledNewConnection()) {
    // schedule the next connection in the background
    this.scheduleNewConnectionAttempt(0);
  }
};

/**
 * Gracefully waits for all in-flight requests to finish and closes the pool.
 */
HostConnectionPool.prototype.drainAndShutdown = function () {
  this.shuttingDown = true;
  this.clearNewConnectionAttempt();
  var self = this;
  if (this.connections.length === 0) {
    return this.setAsShutdown();
  }
  var connections = this.connections;
  this.connections = utils.emptyArray;
  var closedConnections = 0;
  this.log('info', util.format('Draining and closing %d connections to %s', connections.length, this._address));
  for (var i = 0; i < connections.length; i++) {
    var c = connections[i];
    if (c.getInFlight() === 0) {
      getDelayedClose(c)();
      continue;
    }
    c.emitDrain = true;
    c.once('drain', getDelayedClose(c));
  }

  var wasSetAsShutdown = false;
  var checkShutdownTimeout;
  function getDelayedClose(connection) {
    return (function delayedClose() {
      connection.close();
      connection.setAsClosed = true;
      if (++closedConnections < connections.length) {
        return;
      }
      if (wasSetAsShutdown) {
        return;
      }
      wasSetAsShutdown = true;
      if (checkShutdownTimeout) {
        clearTimeout(checkShutdownTimeout);
      }
      self.setAsShutdown();
    });
  }
  // check that after sometime (readTimeout + 2 secs) the connections have been drained
  var delay = (this.options.socketOptions.readTimeout || defaultOptions.socketOptions.readTimeout) + 2000;
  setTimeout(function checkShutdown() {
    if (wasSetAsShutdown) {
      return;
    }
    wasSetAsShutdown = true;
    self.setAsShutdown();
  }, delay);
};

HostConnectionPool.prototype.setAsShutdown = function () {
  if (this._creating) {
    var self = this;
    return this.once('creation', function onCreateShutdown() {
      // ensure all creation process finished
      process.nextTick(function nextTickCreateShutdown() {
        self.shuttingDown = false;
        self.emit('shutdown');
      });
    });
  }
  this.shuttingDown = false;
};

/**
 * @param {Function} callback
 */
HostConnectionPool.prototype.shutdown = function (callback) {
  this.clearNewConnectionAttempt();
  if (!this.connections.length) {
    return callback();
  }
  if (this.shuttingDown) {
    return this.once('shutdown', callback);
  }
  this.shuttingDown = true;
  var connections = this.connections;
  // point to an empty array
  this.connections = utils.emptyArray;
  this.log('info', util.format('Closing %d connections to %s', connections.length, this._address));
  var self = this;
  utils.each(connections, function closeEach(c, next) {
    c.close(function closedCallback() {
      //ignore errors
      next();
    });
  }, function poolShutdownEnded() {
    self.setAsShutdown();
    callback();
  });
};

HostConnectionPool.prototype.log = utils.log;

module.exports = HostConnectionPool;