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

const events = require('events');
const util = require('util');
const tls = require('tls');
const net = require('net');

const Encoder = require('./encoder.js');
const { WriteQueue } = require('./writers');
const requests = require('./requests');
const streams = require('./streams');
const utils = require('./utils');
const types = require('./types');
const errors = require('./errors');
const StreamIdStack = require('./stream-id-stack');
const OperationState = require('./operation-state');
const promiseUtils = require('./promise-utils');
const { ExecutionOptions } = require('./execution-options');

/**
 * Represents a connection to a Cassandra node
 */
class Connection extends events.EventEmitter {

  /**
   * Creates a new instance of Connection.
   * @param {String} endpoint An string containing ip address and port of the host
   * @param {Number|null} protocolVersion
   * @param {ClientOptions} options
   */
  constructor(endpoint, protocolVersion, options) {
    super();

    this.setMaxListeners(0);

    if (!options) {
      throw new Error('options is not defined');
    }

    /**
     * Gets the ip and port of the server endpoint.
     * @type {String}
     */
    this.endpoint = endpoint;

    /**
     * Gets the friendly name of the host, used to identify the connection in log messages.
     * With direct connect, this is the address and port.
     * With SNI, this will be the address and port of the proxy, plus the server name.
     * @type {String}
     */
    this.endpointFriendlyName = this.endpoint;

    if (options.sni) {
      this._serverName = endpoint;
      this.endpoint = `${options.sni.addressResolver.getIp()}:${options.sni.port}`;
      this.endpointFriendlyName = `${this.endpoint} (${this._serverName})`;
    }

    if (!this.endpoint || this.endpoint.indexOf(':') < 0) {
      throw new Error('EndPoint must contain the ip address and port separated by : symbol');
    }

    const portSeparatorIndex = this.endpoint.lastIndexOf(':');
    this.address = this.endpoint.substr(0, portSeparatorIndex);
    this.port = this.endpoint.substr(portSeparatorIndex + 1);

    Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});

    if (protocolVersion === null) {
      // Set initial protocol version
      protocolVersion = types.protocolVersion.maxSupported;
      if (options.protocolOptions.maxVersion) {
        // User provided the protocol version
        protocolVersion = options.protocolOptions.maxVersion;
      }
      // Allow to check version using this connection instance
      this._checkingVersion = true;
    }

    this.log = utils.log;
    this.protocolVersion = protocolVersion;
    this._operations = new Map();
    this._pendingWrites = [];
    this._preparing = new Map();

    /**
     * The timeout state for the idle request (heartbeat)
     */
    this._idleTimeout = null;
    this.timedOutOperations = 0;
    this._streamIds = new StreamIdStack(this.protocolVersion);
    this._metrics = options.metrics;

    this.encoder = new Encoder(protocolVersion, options);
    this.keyspace = null;
    this.emitDrain = false;
    /**
     * Determines if the socket is open and startup succeeded, whether the connection can be used to send requests /
     * receive events
     */
    this.connected = false;
    /**
     * Determines if the socket can be considered as open
     */
    this.isSocketOpen = false;

    this.send = util.promisify(this.sendStream);
    this.closeAsync = util.promisify(this.close);
    this.openAsync = util.promisify(this.open);
    this.prepareOnceAsync = util.promisify(this.prepareOnce);
  }

  /**
   * Binds the necessary event listeners for the socket
   */
  bindSocketListeners() {
    //Remove listeners that were used for connecting
    this.netClient.removeAllListeners('connect');
    this.netClient.removeAllListeners('timeout');
    // The socket is expected to be open at this point
    this.isSocketOpen = true;
    this.netClient.on('close', () => {
      this.log('info', `Connection to ${this.endpointFriendlyName} closed`);
      this.isSocketOpen = false;
      const wasConnected = this.connected;
      this.close();
      if (wasConnected) {
        // Emit only when it was closed unexpectedly
        this.emit('socketClose');
      }
    });

    this.protocol = new streams.Protocol({ objectMode: true });
    this.parser = new streams.Parser({ objectMode: true }, this.encoder);
    const resultEmitter = new streams.ResultEmitter({objectMode: true});
    resultEmitter.on('result', this.handleResult.bind(this));
    resultEmitter.on('row', this.handleRow.bind(this));
    resultEmitter.on('frameEnded', this.freeStreamId.bind(this));
    resultEmitter.on('nodeEvent', this.handleNodeEvent.bind(this));

    this.netClient
      .pipe(this.protocol)
      .pipe(this.parser)
      .pipe(resultEmitter);

    this.writeQueue = new WriteQueue(this.netClient, this.encoder, this.options);
  }

  /**
   * Connects a socket and sends the startup protocol messages.
   * Note that when open() callbacks in error, the caller should immediately call {@link Connection#close}.
   */
  open(callback) {
    const self = this;
    this.log('info', `Connecting to ${this.endpointFriendlyName}`);

    if (!this.options.sslOptions) {
      this.netClient = new net.Socket({ highWaterMark: this.options.socketOptions.coalescingThreshold });
      this.netClient.connect(this.port, this.address, function connectCallback() {
        self.log('verbose', `Socket connected to ${self.endpointFriendlyName}`);
        self.bindSocketListeners();
        self.startup(callback);
      });
    }
    else {
      // Use TLS
      const sslOptions = utils.extend({ rejectUnauthorized: false }, this.options.sslOptions);

      if (this.options.sni) {
        sslOptions.servername = this._serverName;
      }

      this.netClient = tls.connect(this.port, this.address, sslOptions, function tlsConnectCallback() {
        self.log('verbose', `Secure socket connected to ${self.endpointFriendlyName}`);
        self.bindSocketListeners();
        self.startup(callback);
      });

      // TLSSocket will validate for values from 512 to 16K (depending on the SSL protocol version)
      this.netClient.setMaxSendFragment(this.options.socketOptions.coalescingThreshold);
    }

    this.netClient.once('error', function socketError(err) {
      self.errorConnecting(err, false, callback);
    });

    this.netClient.once('timeout', function connectTimedOut() {
      const err = new types.DriverError('Connection timeout');
      self.errorConnecting(err, true, callback);
    });

    this.netClient.setTimeout(this.options.socketOptions.connectTimeout);

    // Improve failure detection with TCP keep-alives
    if (this.options.socketOptions.keepAlive) {
      this.netClient.setKeepAlive(true, this.options.socketOptions.keepAliveDelay);
    }

    this.netClient.setNoDelay(!!this.options.socketOptions.tcpNoDelay);
  }

  /**
   * Determines the protocol version to use and sends the STARTUP request
   * @param {Function} callback
   */
  startup(callback) {
    if (this._checkingVersion) {
      this.log('info', 'Trying to use protocol version 0x' + this.protocolVersion.toString(16));
    }

    const self = this;
    const request = new requests.StartupRequest({
      noCompact: this.options.protocolOptions.noCompact,
      clientId: this.options.id,
      applicationName: this.options.applicationName,
      applicationVersion: this.options.applicationVersion
    });

    this.sendStream(request, null, function responseCallback(err, response) {
      if (err && self._checkingVersion) {
        let invalidProtocol = (err instanceof errors.ResponseError &&
          err.code === types.responseErrorCodes.protocolError &&
          err.message.indexOf('Invalid or unsupported protocol version') >= 0);

        if (!invalidProtocol && types.protocolVersion.canStartupResponseErrorBeWrapped(self.protocolVersion)) {
          //For some versions of Cassandra, the error is wrapped into a server error
          //See CASSANDRA-9451
          invalidProtocol = (err instanceof errors.ResponseError &&
            err.code === types.responseErrorCodes.serverError &&
            err.message.indexOf('ProtocolException: Invalid or unsupported protocol version') > 0);
        }

        if (invalidProtocol) {
          // The server can respond with a message using the lower protocol version supported
          // or using the same version as the one provided
          let lowerVersion = self.protocol.version;

          if (lowerVersion === self.protocolVersion) {
            lowerVersion = types.protocolVersion.getLowerSupported(self.protocolVersion);
          } else if (!types.protocolVersion.isSupported(self.protocol.version)) {
            // If we have an unsupported protocol version or a beta version we need to switch
            // to something we can support.  Note that dseV1 and dseV2 are excluded from this
            // logic as they are supported.  Also note that any v5 and greater beta protocols
            // are included here since the beta flag was introduced in v5.
            self.log('info',`Protocol version ${self.protocol.version} not supported by this driver, downgrading`);
            lowerVersion = types.protocolVersion.getLowerSupported(self.protocol.version);
          }

          if (!lowerVersion) {
            return startupCallback(
              new Error('Connection was unable to STARTUP using protocol version ' + self.protocolVersion));
          }

          self.log('info', 'Protocol 0x' + self.protocolVersion.toString(16) + ' not supported, using 0x' + lowerVersion.toString(16));
          self.decreaseVersion(lowerVersion);

          // The host closed the connection, close the socket and start the connection flow again
          setImmediate(function decreasingVersionClosing() {
            self.close(function decreasingVersionOpening() {
              // Attempt to open with the correct protocol version
              self.open(callback);
            });
          });

          return;
        }
      }

      if (response && response.mustAuthenticate) {
        return self.startAuthenticating(response.authenticatorName, startupCallback);
      }

      startupCallback(err);
    });

    function startupCallback(err) {
      if (err) {
        return self.errorConnecting(err, false, callback);
      }
      //The socket is connected and the connection is authenticated
      return self.connectionReady(callback);
    }
  }

  errorConnecting(err, destroy, callback) {
    this.log('warning', `There was an error when trying to connect to the host ${this.endpointFriendlyName}`, err);
    if (destroy) {
      //there is a TCP connection that should be killed.
      this.netClient.destroy();
    }

    this._metrics.onConnectionError(err);

    callback(err);
  }

  /**
   * Sets the connection to ready/connected status
   */
  connectionReady(callback) {
    this.emit('connected');
    this.connected = true;
    // Remove existing error handlers as the connection is now ready.
    this.netClient.removeAllListeners('error');
    this.netClient.on('error', this.handleSocketError.bind(this));
    callback();
  }

  /** @param {Number} lowerVersion */
  decreaseVersion(lowerVersion) {
    // The response already has the max protocol version supported by the Cassandra host.
    this.protocolVersion = lowerVersion;
    this.encoder.setProtocolVersion(lowerVersion);
    this._streamIds.setVersion(lowerVersion);
  }

  /**
   * Handle socket errors, if the socket is not readable invoke all pending callbacks
   */
  handleSocketError(err) {
    this._metrics.onConnectionError(err);
    this.clearAndInvokePending(err);
  }

  /**
   * Cleans all internal state and invokes all pending callbacks of sent streams
   */
  clearAndInvokePending(innerError) {
    if (this._idleTimeout) {
      //Remove the idle request
      clearTimeout(this._idleTimeout);
      this._idleTimeout = null;
    }
    this._streamIds.clear();
    if (this.emitDrain) {
      this.emit('drain');
    }
    const err = new types.DriverError('Socket was closed');
    err.isSocketError = true;
    if (innerError) {
      err.innerError = innerError;
    }

    // Get all handlers
    const operations = Array.from(this._operations.values());
    // Clear pending operation map
    this._operations = new Map();

    if (operations.length > 0) {
      this.log('info', 'Invoking ' + operations.length + ' pending callbacks');
    }

    // Invoke all handlers
    utils.each(operations, function (operation, next) {
      operation.setResult(err);
      next();
    });

    const pendingWritesCopy = this._pendingWrites;
    this._pendingWrites = [];
    utils.each(pendingWritesCopy, function (operation, next) {
      operation.setResult(err);
      next();
    });
  }

  /**
   * Starts the SASL flow
   * @param {String} authenticatorName
   * @param {Function} callback
   */
  startAuthenticating(authenticatorName, callback) {
    if (!this.options.authProvider) {
      return callback(new errors.AuthenticationError('Authentication provider not set'));
    }
    const authenticator = this.options.authProvider.newAuthenticator(this.endpoint, authenticatorName);
    const self = this;
    authenticator.initialResponse(function initialResponseCallback(err, token) {
      // Start the flow with the initial token
      if (err) {
        return self.onAuthenticationError(callback, err);
      }
      self.authenticate(authenticator, token, callback);
    });
  }

  /**
   * Handles authentication requests and responses.
   * @param {Authenticator} authenticator
   * @param {Buffer} token
   * @param {Function} callback
   */
  authenticate(authenticator, token, callback) {
    const self = this;
    let request = new requests.AuthResponseRequest(token);
    if (this.protocolVersion === 1) {
      //No Sasl support, use CREDENTIALS
      if (!authenticator.username) {
        return self.onAuthenticationError(
          callback, new errors.AuthenticationError('Only plain text authenticator providers allowed under protocol v1'));
      }

      request = new requests.CredentialsRequest(authenticator.username, authenticator.password);
    }

    this.sendStream(request, null, function authResponseCallback(err, result) {
      if (err) {
        if (err instanceof errors.ResponseError && err.code === types.responseErrorCodes.badCredentials) {
          const authError = new errors.AuthenticationError(err.message);
          authError.additionalInfo = err;
          err = authError;
        }
        return self.onAuthenticationError(callback, err);
      }

      if (result.ready) {
        authenticator.onAuthenticationSuccess();
        return callback();
      }

      if (result.authChallenge) {
        return authenticator.evaluateChallenge(result.token, function evaluateCallback(err, t) {
          if (err) {
            return self.onAuthenticationError(callback, err);
          }
          //here we go again
          self.authenticate(authenticator, t, callback);
        });
      }

      callback(new errors.DriverInternalError('Unexpected response from Cassandra: ' + util.inspect(result)));
    });
  }

  onAuthenticationError(callback, err) {
    this._metrics.onAuthenticationError(err);
    callback(err);
  }

  /**
   * Executes a 'USE ' query, if keyspace is provided and it is different from the current keyspace
   * @param {?String} keyspace
   */
  async changeKeyspace(keyspace) {
    if (!keyspace || this.keyspace === keyspace) {
      return;
    }

    if (this.toBeKeyspace === keyspace) {
      // It will be invoked once the keyspace is changed
      return promiseUtils.fromEvent(this, 'keyspaceChanged');
    }

    this.toBeKeyspace = keyspace;

    const query = `USE "${keyspace}"`;

    try {
      await this.send(new requests.QueryRequest(query, null, null), null);
      this.keyspace = keyspace;
      this.emit('keyspaceChanged', null, keyspace);
    } catch (err) {
      this.log('error', `Connection to ${this.endpointFriendlyName} could not switch active keyspace: ${err}`, err);
      this.emit('keyspaceChanged', err);
      throw err;
    } finally {
      this.toBeKeyspace = null;
    }
  }

  /**
   * Prepares a query on a given connection. If its already being prepared, it queues the callback.
   * @param {String} query
   * @param {String} keyspace
   * @param {function} callback
   */
  prepareOnce(query, keyspace, callback) {
    const name = ( keyspace || '' ) + query;
    let info = this._preparing.get(name);

    if (info) {
      // Its being already prepared
      return info.once('prepared', callback);
    }

    info = new events.EventEmitter();
    info.setMaxListeners(0);
    info.once('prepared', callback);
    this._preparing.set(name, info);

    this.sendStream(new requests.PrepareRequest(query, keyspace), null, (err, response) => {
      info.emit('prepared', err, response);
      this._preparing.delete(name);
    });
  }

  /**
   * Queues the operation to be written to the wire and invokes the callback once the response was obtained or with an
   * error (socket error or OperationTimedOutError or serialization-related error).
   * @param {Request} request
   * @param {ExecutionOptions|null} execOptions
   * @param {function} callback Function to be called once the response has been received
   * @return {OperationState}
   */
  sendStream(request, execOptions, callback) {
    execOptions = execOptions || ExecutionOptions.empty();

    // Create a new operation that will contain the request, callback and timeouts
    const operation = new OperationState(request, execOptions.getRowCallback(), (err, response, length) => {
      if (!err || !err.isSocketError) {
        // Emit that a response was obtained when there is a valid response
        // or when the error is not a socket error
        this.emit('responseDequeued');
      }
      callback(err, response, length);
    });

    const streamId = this._getStreamId();

    // Start the request timeout without waiting for the request to be written
    operation.setRequestTimeout(execOptions, this.options.socketOptions.readTimeout, this.endpoint,
      () => this.timedOutOperations++,
      () => this.timedOutOperations--);

    if (streamId === null) {
      this.log('info',
        'Enqueuing ' +
        this._pendingWrites.length +
        ', if this message is recurrent consider configuring more connections per host or lowering the pressure');
      this._pendingWrites.push(operation);
      return operation;
    }
    this._write(operation, streamId);
    return operation;
  }

  /**
   * Pushes the item into the queue.
   * @param {OperationState} operation
   * @param {Number} streamId
   * @private
   */
  _write(operation, streamId) {
    operation.streamId = streamId;
    const self = this;
    this.writeQueue.push(operation, function writeCallback (err) {
      if (err) {
        // The request was not written.
        // There was a serialization error or the operation has already timed out or was cancelled
        self._streamIds.push(streamId);
        return operation.setResult(err);
      }
      self.log('verbose', 'Sent stream #' + streamId + ' to ' + self.endpointFriendlyName);
      if (operation.isByRow()) {
        self.parser.setOptions(streamId, { byRow: true });
      }
      self._setIdleTimeout();
      self._operations.set(streamId, operation);
    });
  }

  _setIdleTimeout() {
    if (!this.options.pooling.heartBeatInterval) {
      return;
    }
    const self = this;
    // Scheduling the new timeout before de-scheduling the previous performs significantly better
    // than de-scheduling first, see nodejs implementation: https://github.com/nodejs/node/blob/master/lib/timers.js
    const previousTimeout = this._idleTimeout;
    self._idleTimeout = setTimeout(() => self._idleTimeoutHandler(), self.options.pooling.heartBeatInterval);
    if (previousTimeout) {
      //remove the previous timeout for the idle request
      clearTimeout(previousTimeout);
    }
  }

  /**
   * Function that gets executed once the idle timeout has passed to issue a request to keep the connection alive
   */
  _idleTimeoutHandler() {
    if (this.sendingIdleQuery) {
      //don't issue another
      //schedule for next time
      this._idleTimeout = setTimeout(() => this._idleTimeoutHandler(), this.options.pooling.heartBeatInterval);
      return;
    }

    this.log('verbose', `Connection to ${this.endpointFriendlyName} idling, issuing a request to prevent disconnects`);
    this.sendingIdleQuery = true;
    this.sendStream(requests.options, null, (err) => {
      this.sendingIdleQuery = false;
      if (!err) {
        //The sending succeeded
        //There is a valid response but we don't care about the response
        return;
      }
      this.log('warning', 'Received heartbeat request error', err);
      this.emit('idleRequestError', err, this);
    });
  }

  /**
   * Returns an available streamId or null if there isn't any available
   * @returns {Number}
   */
  _getStreamId() {
    return this._streamIds.pop();
  }

  freeStreamId(header) {
    const streamId = header.streamId;

    if (streamId < 0) {
      // Event ids don't have a matching request operation
      return;
    }

    this._operations.delete(streamId);
    this._streamIds.push(streamId);

    if (this.emitDrain && this._streamIds.inUse === 0 && this._pendingWrites.length === 0) {
      this.emit('drain');
    }

    this._writeNext();
  }

  _writeNext() {
    if (this._pendingWrites.length === 0) {
      return;
    }
    const streamId = this._getStreamId();
    if (streamId === null) {
      // No streamId available
      return;
    }
    const self = this;
    let operation;
    while ((operation = this._pendingWrites.shift()) && !operation.canBeWritten()) {
      // Trying to obtain an pending operation that can be written
    }

    if (!operation) {
      // There isn't a pending operation that can be written
      this._streamIds.push(streamId);
      return;
    }

    // Schedule after current I/O callbacks have been executed
    setImmediate(function writeNextPending() {
      self._write(operation, streamId);
    });
  }

  /**
   * Returns the number of requests waiting for response
   * @returns {Number}
   */
  getInFlight() {
    return this._streamIds.inUse;
  }

  /**
   * Handles a result and error response
   */
  handleResult(header, err, result) {
    const streamId = header.streamId;
    if(streamId < 0) {
      return this.log('verbose', 'event received', header);
    }
    const operation = this._operations.get(streamId);
    if (!operation) {
      return this.log('error', 'The server replied with a wrong streamId #' + streamId);
    }
    this.log('verbose', 'Received frame #' + streamId + ' from ' + this.endpointFriendlyName);
    operation.setResult(err, result, header.bodyLength);
  }

  handleNodeEvent(header, event) {
    switch (event.eventType) {
      case types.protocolEvents.schemaChange:
        this.emit('nodeSchemaChange', event);
        break;
      case types.protocolEvents.topologyChange:
        this.emit('nodeTopologyChange', event);
        break;
      case types.protocolEvents.statusChange:
        this.emit('nodeStatusChange', event);
        break;
    }
  }

  /**
   * Handles a row response
   */
  handleRow(header, row, meta, rowLength, flags) {
    const streamId = header.streamId;
    if(streamId < 0) {
      return this.log('verbose', 'Event received', header);
    }
    const operation = this._operations.get(streamId);
    if (!operation) {
      return this.log('error', 'The server replied with a wrong streamId #' + streamId);
    }
    operation.setResultRow(row, meta, rowLength, flags, header);
  }

  /**
   * Closes the socket (if not already closed) and cancels all in-flight requests.
   * Multiple calls to this method have no additional side-effects.
   * @param {Function} [callback]
   */
  close(callback) {
    callback = callback || utils.noop;

    if (!this.connected && !this.isSocketOpen) {
      return callback();
    }

    this.connected = false;
    // Drain is never going to be emitted, once it is set to closed
    this.removeAllListeners('drain');
    this.clearAndInvokePending();

    if (!this.isSocketOpen) {
      return callback();
    }

    // Set the socket as closed now (before socket.end() is called) to avoid being invoked more than once
    this.isSocketOpen = false;
    this.log('verbose', `Closing connection to ${this.endpointFriendlyName}`);
    const self = this;

    // If server doesn't acknowledge the half-close within connection timeout, destroy the socket.
    const endTimeout = setTimeout(() => {
      this.log('info', `${this.endpointFriendlyName} did not respond to connection close within ` +
        `${this.options.socketOptions.connectTimeout}ms, destroying connection`);
      this.netClient.destroy();
    }, this.options.socketOptions.connectTimeout);

    this.netClient.once('close', function (hadError) {
      clearTimeout(endTimeout);
      if (hadError) {
        self.log('info', 'The socket closed with a transmission error');
      }
      setImmediate(callback);
    });

    // At this point, the error event can be triggered because:
    // - It's connected and writes haven't completed yet
    // - The server abruptly closed its end of the connection (ECONNRESET) as a result of protocol error / auth error
    // We need to remove any listeners and make sure we callback are pending writes
    this.netClient.removeAllListeners('error');
    this.netClient.on('error', err => this.clearAndInvokePending(err));

    // Half-close the socket, it will result in 'close' event being fired
    this.netClient.end();
  }

  /**
   * Gets the local IP address to which this connection socket is bound to.
   * @returns {String|undefined}
   */
  getLocalAddress() {
    if (!this.netClient) {
      return undefined;
    }

    return this.netClient.localAddress;
  }
}

module.exports = Connection;
