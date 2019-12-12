/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const util = require('util');
const events = require('events');

const Connection = require('./connection');
const utils = require('./utils');
const promiseUtils = require('./promise-utils');
const errors = require('./errors');
const clientOptions = require('./client-options');

// Used to get the index of the connection with less in-flight requests
let connectionIndex = 0;
const connectionIndexOverflow = Math.pow(2, 15);

let defaultOptions;

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
    this._state = state.initial;
    this._opening = false;
    this._host = host;
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
   * Gets the least busy connection from the pool.
   * @param {Connection} [previousConnection] When provided, the pool should attempt to obtain a different connection.
   * @returns {Connection!}
   * @throws {Error}
   * @throws {BusyConnectionError}
   */
  borrowConnection(previousConnection) {
    if (this.connections.length === 0) {
      throw new Error('No connection available');
    }

    const maxRequests = this.options.pooling.maxRequestsPerConnection;
    const c = HostConnectionPool.minInFlight(this.connections, maxRequests, previousConnection);

    if (c.getInFlight() >= maxRequests) {
      throw new errors.BusyConnectionError(this._address, maxRequests, this.connections.length);
    }

    return c;
  }

  /**
   * Gets the connection with the minimum number of in-flight requests.
   * Only checks for 2 connections (round-robin) and gets the one with minimum in-flight requests, as long as
   * the amount of in-flight requests is lower than maxRequests.
   * @param {Array.<Connection>} connections
   * @param {Number} maxRequests
   * @param {Connection} previousConnection When provided, it will attempt to obtain a different connection.
   * @returns {Connection!}
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
   * Creates all the connections in the pool and switches the keyspace of each connection if needed.
   * @param {string} keyspace
   */
  async warmup(keyspace) {
    if (this.connections.length < this.coreConnectionsLength) {
      while (this.connections.length < this.coreConnectionsLength) {
        await this._attemptNewConnection();
      }

      this.log('info',
        `Connection pool to host ${this._address} created with ${this.connections.length} connection(s)`);
    } else {
      this.log('info', `Connection pool to host ${this._address} contains ${this.connections.length} connection(s)`);
    }

    if (keyspace) {
      try {
        for (const connection of this.connections) {
          await connection.changeKeyspace(keyspace);
        }
      } catch (err) {
        // Log it and move on, it could be a momentary schema mismatch failure
        this.log('warning', `Connection(s) to host ${this._address} could not be switched to keyspace ${keyspace}`);
      }
    }
  }

  /** @returns {Connection} */
  _createConnection() {
    const endpointOrServerName = !this.options.sni
      ? this._address : this._host.hostId.toString();

    const c = new Connection(endpointOrServerName, this.protocolVersion, this.options);
    this._addListeners(c);
    return c;
  }

  /** @param {Connection} c */
  _addListeners(c) {
    c.on('responseDequeued', () => this.responseCounter++);

    const self = this;
    function connectionErrorCallback() {
      // The socket is not fully open / can not send heartbeat
      self.remove(c);
    }
    c.on('idleRequestError', connectionErrorCallback);
    c.on('socketClose', connectionErrorCallback);
  }

  addExistingConnection(c) {
    this._addListeners(c);
    // Use a copy of the connections array
    this.connections = this.connections.slice(0);
    this.connections.push(c);
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
   * Tries to open a new connection.
   * If a connection is being opened, it will resolve when the existing open task completes.
   * @returns {Promise<void>}
   */
  async _attemptNewConnection() {
    if (this._opening) {
      // Wait for the event to fire
      return await promiseUtils.fromEvent(this, 'open');
    }

    this._opening = true;

    const c = this._createConnection();
    let err;

    try {
      await c.openAsync();
    } catch (e) {
      err = e;
      this.log('warning', `Connection to ${this._address} could not be created: ${err}`, err);
    }

    if (this.isClosing()) {
      this.log('info', `Connection to ${this._address} opened successfully but pool was being closed`);
      err = new Error('Connection closed');
    }

    if (!err) {
      // Append the connection to the pool.
      // Use a copy of the connections array.
      const newConnections = this.connections.slice(0);
      newConnections.push(c);
      this.connections = newConnections;
      this.log('info', `Connection to ${this._address} opened successfully`);
    } else {
      promiseUtils.toBackground(c.closeAsync());
    }

    // Notify that creation finished by setting the flag and emitting the event
    this._opening = false;
    this.emit('open', err, c);

    if (err) {
      // Opening failed
      throw err;
    }
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

      if (delay > 0 && self.options.sni) {
        // We use delay > 0 as an indication that it's a reconnection.
        // A reconnection schedule can use delay = 0 as well, but it's a good enough signal.
        promiseUtils.toBackground(self.options.sni.addressResolver.refresh().then(() => self._attemptNewConnection()));
        return;
      }

      promiseUtils.toBackground(self._attemptNewConnection());
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

    if (this.connections.length === 0) {
      return this._afterClosing();
    }

    const self = this;
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
    const delay = (this.options.socketOptions.readTimeout || getDefaultOptions().socketOptions.readTimeout) + 100;
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
      } else {
        self._state = state.initial;
      }

      self.emit('close');

      if (self._state === state.shutDown) {
        self.emit('shutdown');
      }
    }

    if (this._opening) {
      // The pool is growing, reset the state back to init once the open finished (without any new connection)
      return this.once('open', resetState);
    }

    resetState();
  }

  /**
   * @returns {Promise<void>}
   */
  async shutdown() {
    this.clearNewConnectionAttempt();

    if (!this.connections.length) {
      this._state = state.shutDown;
      return;
    }

    const previousState = this._state;
    this._state = state.shuttingDown;

    if (previousState === state.closing || previousState === state.shuttingDown) {
      // When previous state was closing, it will drain all connections and close them
      // When previous state was "shuttingDown", it will close all the connections
      // Once it's completed, shutdown event will be emitted
      return promiseUtils.fromEvent(this, 'shutdown');
    }

    await this._closeAllConnections();

    this._state = state.shutDown;
    this.emit('shutdown');
  }

  async _closeAllConnections() {
    const connections = this.connections;
    // point to an empty array
    this.connections = utils.emptyArray;
    if (connections.length === 0) {
      return;
    }

    this.log('info', util.format('Closing %d connections to %s', connections.length, this._address));

    await Promise.all(connections.map(c => c.closeAsync()));
  }
}

/** Lazily loads the default options */
function getDefaultOptions() {
  if (defaultOptions === undefined) {
    defaultOptions = clientOptions.defaultOptions();
  }
  return defaultOptions;
}

module.exports = HostConnectionPool;