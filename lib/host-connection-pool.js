"use strict";
const util = require('util');
const events = require('events');

const Connection = require('./connection');
const utils = require('./utils');
const errors = require('./errors');
const defaultOptions = require('./client-options').defaultOptions();

// Used to get the index of the connection with less in-flight requests
let connectionIndex = 0;
const connectionIndexOverflow = Math.pow(2, 15);

/**
 * Represents the possible states of the pool.
 * Possible state transitions:
 *  - From initial to closing: The pool must be closed because the host is ignored.
 *  - From initial to shuttingDown: The pool is being shutdown as a result of a client shutdown.
 *  - From closing to initial state: The pool finished closing connections (is now ignored) and it resets to
 *    initial state in case the host is marked as local/remote in the future.
 *  - From closing to shuttingDown (rare): It was marked as ignored, now the client is being shutdown.
 *  - From shuttingDown to shutdown: Finished shutting down, the pool should not be reused.
 * @private
 */
const state = {
  // Initial state: open / opening / ready to be opened
  initial: 0,
  // When the pool is being closed as part of a distance change
  closing: 1,
  // When the pool is being shutdown for good
  shuttingDown: 2,
  // When the pool has being shutdown
  shutDown: 4
};

/**
 * Represents a pool of connections to a host
 */
class HostConnectionPool extends events.EventEmitter {
  /**
   * Creates a new instance of HostConnectionPool.
   * @param {Host} host
   * @param {Number} protocolVersion Initial protocol version
   * @extends EventEmitter
   */
  constructor(host, protocolVersion) {
    super();
    this._address = host.address;
    this._newConnectionTimeout = null;
    this._creating = false;
    this._state = state.initial;
    this.responseCounter = 0;
    this.options = host.options;
    this.protocolVersion = protocolVersion;
    this.coreConnectionsLength = 1;
    /**
     * An immutable array of connections
     * @type {Array.<Connection>}
     */
    this.connections = utils.emptyArray;
    this.setMaxListeners(0);
    this.log = utils.log;
  }

  /**
   * Borrows a connection from the pool.
   * @param {String} keyspace
   * @param {Function} callback
   */
  createAndBorrowConnection(keyspace, callback) {
    this.create(false, err => {
      if (err) {
        return callback(err);
      }

      this.borrowConnection(keyspace, null, callback);
    });
  }

  getInFlight() {
    const length = this.connections.length;
    if (length === 1) {
      return this.connections[0].getInFlight();
    }

    let sum = 0;
    for (let i = 0; i < length; i++) {
      sum += this.connections[i].getInFlight();
    }
    return sum;
  }

  /**
   * Tries to borrow one of the existing connections from the pool.
   * @param {Connection} previousConnection When provided, the pool should try to provide a different connection.
   * @param {String} keyspace
   * @param {Function} callback
   */
  borrowConnection(keyspace, previousConnection, callback) {
    if (this.connections.length === 0) {
      return callback(new Error('No connection available'));
    }

    const maxRequests = this.options.pooling.maxRequestsPerConnection;
    const c = HostConnectionPool.minInFlight(this.connections, maxRequests, previousConnection);

    if (c.getInFlight() >= maxRequests) {
      return callback(new errors.BusyConnectionError(this._address, maxRequests, this.connections.length));
    }

    if (!keyspace || keyspace === c.keyspace) {
      // Connection is ready to be used
      return callback(null, c);
    }

    c.changeKeyspace(keyspace, (err) => {
      callback(err, c);
    });
  }

  /**
   * Gets the connection with the minimum number of in-flight requests.
   * Only checks for 2 connections (round-robin) and gets the one with minimum in-flight requests, as long as
   * the amount of in-flight requests is lower than maxRequests.
   * @param {Array.<Connection>} connections
   * @param {Number} maxRequests
   * @param {Connection} previousConnection
   * @returns {Connection}
   */
  static minInFlight(connections, maxRequests, previousConnection) {
    const length = connections.length;
    if (length === 1) {
      return connections[0];
    }

    // Use a single index for all hosts as a simplified way to balance the load between connections
    connectionIndex++;
    if (connectionIndex >= connectionIndexOverflow) {
      connectionIndex = 0;
    }

    let current;
    for (let index = connectionIndex; index < connectionIndex + length; index++) {
      current = connections[index % length];
      if (current === previousConnection) {
        // Increment the index and skip
        current = connections[(++index) % length];
      }

      let next = connections[(index + 1) % length];
      if (next === previousConnection) {
        // Skip
        next = connections[(index + 2) % length];
      }

      if (next.getInFlight() < current.getInFlight()) {
        current = next;
      }

      if (current.getInFlight() < maxRequests) {
        // Check as few connections as possible, as long as the amount of in-flight
        // requests is lower than maxRequests
        break;
      }
    }
    return current;
  }

  /**
   * Create the min amount of connections, if the pool is empty.
   * @param {Boolean} warmup Determines if all connections must be created before invoking the callback
   * @param {Function} callback
   */
  create(warmup, callback) {
    if (this.isClosing()) {
      return callback(new Error('Pool is being closed when calling create'));
    }
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
    let connectionsToCreate = this.coreConnectionsLength;
    if (!warmup) {
      connectionsToCreate = 1;
    }
    const self = this;
    utils.whilst(
      function condition() {
        return self.connections.length < connectionsToCreate;
      },
      function iterator(next) {
        self._attemptNewConnection(next);
      }, function whilstEnded(err) {
        self._creating = false;
        if (err) {
          if (self.isClosing()) {
            self.log('info', 'Connection pool created but it was being closed');
            self._closeAllConnections();
            err = new Error('Pool is being closed');
          }
          else {
            // there was an error and no connections could be successfully opened
            self.log('warning', util.format('Connection pool to host %s could not be created', self._address), err);
          }
          return self.emit('creation', err);
        }
        self.log('info', util.format('Connection pool to host %s created with %d connection(s)',
          self._address, self.connections.length));
        self.emit('creation');
        self.increaseSize();
      });
  }

  /** @returns {Connection} */
  _createConnection() {
    const c = new Connection(this._address, this.protocolVersion, this.options);
    c.on('responseDequeued', () => this.responseCounter++);

    const self = this;
    function connectionErrorCallback() {
      // The socket is not fully open / can not send heartbeat
      self.remove(c);
    }
    c.on('idleRequestError', connectionErrorCallback);
    c.on('socketClose', connectionErrorCallback);
    return c;
  }

  /**
   * Prevents reconnection timeout from triggering
   */
  clearNewConnectionAttempt() {
    if (!this._newConnectionTimeout) {
      return;
    }
    clearTimeout(this._newConnectionTimeout);
    this._newConnectionTimeout = null;
  }

  /**
   * @param {Function} callback
   */
  _attemptNewConnection(callback) {
    const c = this._createConnection();
    const self = this;
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
      if (self.isClosing()) {
        self.log('info', util.format('Connection to %s opened successfully but pool was being closed', self._address));
        c.close();
        return self.emit('open', new Error('Connection closed'));
      }
      // use a copy of the connections array
      const newConnections = self.connections.slice(0);
      newConnections.push(c);
      self.connections = newConnections;
      self.log('info', util.format('Connection to %s opened successfully', self._address));
      self.emit('open', null, c);
    });
  }

  attemptNewConnectionImmediate() {
    const self = this;
    function openConnection() {
      self.clearNewConnectionAttempt();
      self.scheduleNewConnectionAttempt(0);
    }
    if (this._state === state.initial) {
      return openConnection();
    }
    if (this._state === state.closing) {
      return this.once('close', openConnection);
    }
    // In the case the pool its being / has been shutdown for good
    // Do not attempt to create a new connection.
  }

  /**
   * Closes the connection and removes a connection from the pool.
   * @param {Connection} connection
   */
  remove(connection) {
    // locating an object by position in the array is O(n), but normally there should be between 1 to 8 connections.
    const index = this.connections.indexOf(connection);
    if (index < 0) {
      // it was already removed from the connections and it's closing
      return;
    }
    // remove the connection from the pool, using an pool copy
    const newConnections = this.connections.slice(0);
    newConnections.splice(index, 1);
    this.connections = newConnections;
    // close the connection
    setImmediate(function removeClose() {
      connection.close();
    });
    this.emit('remove');
  }

  /**
   * @param {Number} delay
   */
  scheduleNewConnectionAttempt(delay) {
    if (this.isClosing()) {
      return;
    }
    const self = this;
    this._newConnectionTimeout = setTimeout(function newConnectionTimeoutExpired() {
      self._newConnectionTimeout = null;
      if (self.connections.length >= self.coreConnectionsLength) {
        // new connection can be scheduled while a new connection is being opened
        // the pool has the appropriate size
        return;
      }
      self._attemptNewConnection(utils.noop);
    }, delay);
  }

  hasScheduledNewConnection() {
    return !!this._newConnectionTimeout || this._opening;
  }

  /**
   * Increases the size of the connection pool in the background, if needed.
   */
  increaseSize() {
    if (this.connections.length < this.coreConnectionsLength && !this.hasScheduledNewConnection()) {
      // schedule the next connection in the background
      this.scheduleNewConnectionAttempt(0);
    }
  }

  /**
   * Gets the amount of responses and resets the internal counter.
   * @returns {number}
   */
  getAndResetResponseCounter() {
    const temp = this.responseCounter;
    this.responseCounter = 0;
    return temp;
  }

  /**
   * Gets a boolean indicating if the pool is being closed / shutting down or has been shutdown.
   */
  isClosing() {
    return this._state !== state.initial;
  }

  /**
   * Gracefully waits for all in-flight requests to finish and closes the pool.
   */
  drainAndShutdown() {
    if (this.isClosing()) {
      // Its already closing / shutting down
      return;
    }
    this._state = state.closing;
    this.clearNewConnectionAttempt();
    const self = this;
    if (this.connections.length === 0) {
      return this._afterClosing();
    }
    const connections = this.connections;
    this.connections = utils.emptyArray;
    let closedConnections = 0;
    this.log('info', util.format('Draining and closing %d connections to %s', connections.length, this._address));
    let wasClosed = false;
    // eslint-disable-next-line prefer-const
    let checkShutdownTimeout;
    for (let i = 0; i < connections.length; i++) {
      const c = connections[i];
      if (c.getInFlight() === 0) {
        getDelayedClose(c)();
        continue;
      }
      c.emitDrain = true;
      c.once('drain', getDelayedClose(c));
    }
    function getDelayedClose(connection) {
      return (function delayedClose() {
        connection.close();
        if (++closedConnections < connections.length) {
          return;
        }
        if (wasClosed) {
          return;
        }
        wasClosed = true;
        if (checkShutdownTimeout) {
          clearTimeout(checkShutdownTimeout);
        }
        self._afterClosing();
      });
    }
    // Check that after sometime (readTimeout + 100ms) the connections have been drained
    const delay = (this.options.socketOptions.readTimeout || defaultOptions.socketOptions.readTimeout) + 100;
    checkShutdownTimeout = setTimeout(function checkShutdown() {
      wasClosed = true;
      connections.forEach(function connectionEach(c) {
        c.close();
      });
      self._afterClosing();
    }, delay);
  }

  _afterClosing() {
    const self = this;
    function resetState() {
      if (self._state === state.shuttingDown) {
        self._state = state.shutDown;
      }
      else {
        self._state = state.initial;
      }
      self.emit('close');
    }
    if (this._creating) {
      // The pool is being created, reset the state back to init once the creation finished (without any new connection)
      return this.once('creation', resetState);
    }
    if (this._opening) {
      // The pool is growing, reset the state back to init once the open finished (without any new connection)
      return this.once('open', resetState);
    }
    resetState();
  }

  /**
   * @param {Function} callback
   */
  shutdown(callback) {
    this.clearNewConnectionAttempt();
    if (!this.connections.length) {
      this._state = state.shutDown;
      return callback();
    }
    const previousState = this._state;
    this._state = state.shuttingDown;
    if (previousState === state.closing) {
      return this.once('close', callback);
    }
    this.once('shutdown', callback);
    if (previousState === state.shuttingDown) {
      // Its going to be emitted
      return;
    }
    const self = this;
    this._closeAllConnections(function closeAllCallback() {
      self._state = state.shutDown;
      self.emit('shutdown');
    });
  }

  /** @param {Function} [callback] */
  _closeAllConnections(callback) {
    callback = callback || utils.noop;
    const connections = this.connections;
    // point to an empty array
    this.connections = utils.emptyArray;
    if (connections.length === 0) {
      return callback();
    }
    this.log('info', util.format('Closing %d connections to %s', connections.length, this._address));
    utils.each(connections, function closeEach(c, next) {
      c.close(function closedCallback() {
        //ignore errors
        next();
      });
    }, callback);
  }
}

module.exports = HostConnectionPool;