"use strict";
var net = require('net');
var events = require('events');
var util = require('util');
var tls = require('tls');

var Encoder = require('./encoder.js');
var writers = require('./writers');
var requests = require('./requests');
var streams = require('./streams');
var utils = require('./utils');
var types = require('./types');
var errors = require('./errors');
var StreamIdStack = require('./stream-id-stack');

/**  @const */
var idleQuery = 'SELECT key from system.local';
/**  @const */
var maxProtocolVersion = 4;
/**
 * Represents a connection to a Cassandra node
 * @param {String} endpoint An string containing ip address and port of the host
 * @param {Number} protocolVersion
 * @param {ClientOptions} options
 * @extends EventEmitter
 * @constructor
 */
function Connection(endpoint, protocolVersion, options) {
  events.EventEmitter.call(this);
  this.setMaxListeners(0);
  if (!endpoint || endpoint.indexOf(':') < 0) {
    throw new Error('EndPoint must contain the ip address and port separated by : symbol');
  }
  this.endpoint = endpoint;
  var portSeparatorIndex = endpoint.lastIndexOf(':');
  this.address = endpoint.substr(0, portSeparatorIndex);
  this.port = endpoint.substr(portSeparatorIndex + 1);
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  if (protocolVersion === null) {
    //Set initial protocol version
    protocolVersion = maxProtocolVersion;
    if (options.protocolOptions.maxVersion > 0 && options.protocolOptions.maxVersion < maxProtocolVersion) {
      //limit the protocol version
      protocolVersion = options.protocolOptions.maxVersion;
    }
    //Allow to check version using this connection instance
    this.checkingVersion = true;
  }
  this.protocolVersion = protocolVersion;
  this.streamHandlers = {};
  this.pendingWrites = [];
  this.preparing = {};
  /**
   * The timeout state for the idle request (heartbeat)
   */
  this.idleTimeout = null;
  this.timedOutHandlers = 0;
  this.streamIds = new StreamIdStack(this.protocolVersion);
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
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.log = utils.log;

/**
 * Binds the necessary event listeners for the socket
 */
Connection.prototype.bindSocketListeners = function() {
  //Remove listeners that were used for connecting
  this.netClient.removeAllListeners('connect');
  this.netClient.removeAllListeners('timeout');
  // The socket is expected to be open at this point
  this.isSocketOpen = true;
  var self = this;
  this.netClient.on('close', function() {
    self.log('info', 'Connection to ' + self.endpoint + ' closed');
    self.isSocketOpen = false;
    var wasConnected = self.connected;
    self.close();
    if (wasConnected) {
      // Emit only when it was closed unexpectedly
      self.emit('socketClose');
    }
  });

  this.protocol = new streams.Protocol({ objectMode: true });
  this.parser = new streams.Parser({ objectMode: true }, this.encoder);
  var resultEmitter = new streams.ResultEmitter({objectMode: true});
  resultEmitter.on('result', this.handleResult.bind(this));
  resultEmitter.on('row', this.handleRow.bind(this));
  resultEmitter.on('frameEnded', this.freeStreamId.bind(this));
  resultEmitter.on('nodeEvent', this.handleNodeEvent.bind(this));

  this.netClient
    .pipe(this.protocol)
    .pipe(this.parser)
    .pipe(resultEmitter);

  this.writeQueue = new writers.WriteQueue(this.netClient, this.encoder, this.options);
};

/**
 * Connects a socket and sends the startup protocol messages.
 * Note that when open() callbacks in error, the caller should immediately call {@link Connection#close}.
 */
Connection.prototype.open = function (callback) {
  var self = this;
  this.log('info', 'Connecting to ' + this.address + ':' + this.port);
  if (!this.options.sslOptions) {
    this.netClient = new net.Socket();
    this.netClient.connect(this.port, this.address, function connectCallback() {
      self.log('verbose', 'Socket connected to ' + self.address + ':' + self.port);
      self.bindSocketListeners();
      self.startup(callback);
    });
  }
  else {
    //use TLS
    var sslOptions = utils.extend({rejectUnauthorized: false}, this.options.sslOptions);
    this.netClient = tls.connect(this.port, this.address, sslOptions, function tlsConnectCallback() {
      self.log('verbose', 'Secure socket connected to ' + self.address + ':' + self.port);
      self.bindSocketListeners();
      self.startup(callback);
    });
  }
  this.netClient.once('error', function socketError(err) {
    self.errorConnecting(err, false, callback);
  });
  this.netClient.once('timeout', function connectTimedOut() {
    var err = new types.DriverError('Connection timeout');
    self.errorConnecting(err, true, callback);
  });
  this.netClient.setTimeout(this.options.socketOptions.connectTimeout);
  // Improve failure detection with TCP keep-alives
  if (this.options.socketOptions.keepAlive) {
    this.netClient.setKeepAlive(true, this.options.socketOptions.keepAliveDelay);
  }
  this.netClient.setNoDelay(!!this.options.socketOptions.tcpNoDelay);
};

/**
 * Determines the protocol version to use and sends the STARTUP request
 * @param {Function} callback
 */
Connection.prototype.startup = function (callback) {
  if (this.checkingVersion) {
    this.log('info', 'Trying to use protocol version ' + this.protocolVersion);
  }
  var self = this;
  this.sendStream(new requests.StartupRequest(), null, function responseCallback(err, response) {
    if (err && self.checkingVersion && self.protocolVersion > 1) {
      var invalidProtocol = (err instanceof errors.ResponseError &&
        err.code === types.responseErrorCodes.protocolError &&
        err.message.indexOf('Invalid or unsupported protocol version') >= 0);
      if (!invalidProtocol && self.protocolVersion > 3) {
        //For some versions of Cassandra, the error is wrapped into a server error
        //See CASSANDRA-9451
        invalidProtocol = (err instanceof errors.ResponseError &&
          err.code === types.responseErrorCodes.serverError &&
          err.message.indexOf('ProtocolException: Invalid or unsupported protocol version') > 0);
      }
      if (invalidProtocol) {
        self.log('info', 'Protocol v' + self.protocolVersion + ' not supported, using v' + self.protocol.version);
        self.decreaseVersion();
        //The host closed the connection, close the socket
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
};

Connection.prototype.errorConnecting = function (err, destroy, callback) {
  this.log('warning', 'There was an error when trying to connect to the host ' + this.address, err);
  if (destroy) {
    //there is a TCP connection that should be killed.
    this.netClient.destroy();
  }
  callback(err);
};

/**
 * Sets the connection to ready/connected status
 */
Connection.prototype.connectionReady = function (callback) {
  this.emit('connected');
  this.connected = true;
  // Remove existing error handlers as the connection is now ready.
  this.netClient.removeAllListeners('error');
  this.netClient.on('error', this.handleSocketError.bind(this));
  callback();
};

Connection.prototype.decreaseVersion = function () {
  // The response already has the max protocol version supported by the Cassandra host.
  this.protocolVersion = this.protocol.version;
  this.encoder.setProtocolVersion(this.protocolVersion);
  this.streamIds.setVersion(this.protocolVersion);
};

/**
 * Handle socket errors, if the socket is not readable invoke all pending callbacks
 */
Connection.prototype.handleSocketError = function (err) {
  this.clearAndInvokePending(err);
};

/**
 * Cleans all internal state and invokes all pending callbacks of sent streams
 */
Connection.prototype.clearAndInvokePending = function (innerError) {
  if (this.idleTimeout) {
    //Remove the idle request
    clearTimeout(this.idleTimeout);
    this.idleTimeout = null;
  }
  this.streamIds.clear();
  if (this.emitDrain) {
    this.emit('drain');
  }
  var err = new types.DriverError('Socket was closed');
  err.isSocketError = true;
  if (innerError) {
    err.innerError = innerError;
  }
  //copy all handlers
  var handlers = utils.objectValues(this.streamHandlers);
  //remove it from the map
  this.streamHandlers = {};
  if (handlers.length > 0) {
    this.log('info', 'Invoking ' + handlers.length + ' pending callbacks');
  }
  var self = this;
  //invoke all handlers
  utils.each(handlers, function (item, next) {
    self.invokeCallback(item, err);
    next();
  });

  var pendingWritesCopy = this.pendingWrites;
  this.pendingWrites = [];
  utils.each(pendingWritesCopy, function (item, next) {
    if (!item.callback) {
      return;
    }
    item.callback(err);
    next();
  });
};

/**
 * Starts the SASL flow
 * @param {String} authenticatorName
 * @param {Function} callback
 */
Connection.prototype.startAuthenticating = function (authenticatorName, callback) {
  if (!this.options.authProvider) {
    return callback(new errors.AuthenticationError('Authentication provider not set'));
  }
  var authenticator = this.options.authProvider.newAuthenticator(this.endpoint, authenticatorName);
  var self = this;
  authenticator.initialResponse(function initialResponseCallback(err, token) {
    // Start the flow with the initial token
    if (err) {
      return callback(err);
    }
    self.authenticate(authenticator, token, callback);
  });
};

/**
 * Handles authentication requests and responses.
 * @param {Authenticator} authenticator
 * @param {Buffer} token
 * @param {Function} callback
 */
Connection.prototype.authenticate = function(authenticator, token, callback) {
  var self = this;
  var request = new requests.AuthResponseRequest(token);
  if (this.protocolVersion === 1) {
    //No Sasl support, use CREDENTIALS
    //noinspection JSUnresolvedVariable
    if (!authenticator.username) {
      return callback(new errors.AuthenticationError('Only plain text authenticator providers allowed under protocol v1'));
    }
    //noinspection JSUnresolvedVariable
    request = new requests.CredentialsRequest(authenticator.username, authenticator.password);
  }
  this.sendStream(request, null, function authResponseCallback(err, result) {
    if (err) {
      if (err instanceof errors.ResponseError && err.code === types.responseErrorCodes.badCredentials) {
        var authError = new errors.AuthenticationError(err.message);
        authError.additionalInfo = err;
        err = authError;
      }
      return callback(err);
    }
    if (result.ready) {
      authenticator.onAuthenticationSuccess();
      return callback();
    }
    if (result.authChallenge) {
      return authenticator.evaluateChallenge(result.token, function evaluateCallback(err, t) {
        if (err) {
          return callback(err);
        }
        //here we go again
        self.authenticate(authenticator, t, callback);
      });
    }
    callback(new errors.DriverInternalError('Unexpected response from Cassandra: ' + util.inspect(result)));
  });
};

/**
 * Executes a 'USE ' query, if keyspace is provided and it is different from the current keyspace
 * @param {?String} keyspace
 * @param {Function} callback
 */
Connection.prototype.changeKeyspace = function (keyspace, callback) {
  if (!keyspace || this.keyspace === keyspace) {
    return callback();
  }
  this.once('keyspaceChanged', callback);
  if (this.toBeKeyspace === keyspace) {
    // It will be invoked once the keyspace is changed
    return;
  }
  this.toBeKeyspace = keyspace;
  var query = util.format('USE "%s"', keyspace);
  var self = this;
  this.sendStream(
    new requests.QueryRequest(query, null, null),
    null,
    function changeKeyspaceResponseCallback(err) {
      if (!err) {
        self.keyspace = keyspace;
      }
      self.toBeKeyspace = null;
      self.emit('keyspaceChanged', err, keyspace);
    });
};

/**
 * Prepares a query on a given connection. If its already being prepared, it queues the callback.
 * @param {String} query
 * @param {function} callback
 */
Connection.prototype.prepareOnce = function (query, callback) {
  var name = ( this.keyspace || '' ) + query;
  var info = this.preparing[name];
  if (this.preparing[name]) {
    //Its being already prepared
    return info.once('prepared', callback);
  }
  info = new events.EventEmitter();
  info.setMaxListeners(0);
  info.once('prepared', callback);
  this.preparing[name] = info;
  var self = this;
  this.sendStream(new requests.PrepareRequest(query), null, function (err, response) {
    info.emit('prepared', err, response);
    delete self.preparing[name];
  });
};

/**
 * Uses the frame writer to write into the wire
 * @param request
 * @param {QueryOptions|null} options
 * @param {function} callback Function to be called once the response has been received
 */
Connection.prototype.sendStream = function (request, options, callback) {
  var self = this;
  var streamId = this.getStreamId();
  if (streamId === null) {
    self.log('info',
        'Enqueuing ' +
        this.pendingWrites.length +
        ', if this message is recurrent consider configuring more connections per host or lowering the pressure');
    return this.pendingWrites.push({request: request, options: options, callback: callback});
  }
  this.log('verbose', 'Sending stream #' + streamId);
  request.streamId = streamId;
  request.version = this.protocolVersion;
  this.writeQueue.push(request, this.getWriteCallback(request, options, callback));
};

/**
 * Returns the write callback
 * @param request
 * @param options
 * @param {Function} callback
 * @returns {Function}
 */
Connection.prototype.getWriteCallback = function (request, options, callback) {
  var self = this;
  return (function writeCallback (err) {
    if (err) {
      return callback(err);
    }
    self.log('verbose', 'Sent stream #' + request.streamId + ' to ' + self.endpoint);
    if (options && options.rowCallback) {
      if (request instanceof requests.ExecuteRequest || request instanceof requests.QueryRequest) {
        self.parser.setOptions(request.streamId, { byRow: true });
      }
    }
    if (self.options.pooling.heartBeatInterval) {
      // scheduling the new timeout before de-scheduling the previous performs significantly better
      // than de-scheduling first, see nodejs implementation: https://github.com/nodejs/node/blob/master/lib/timers.js
      var previousTimeout = self.idleTimeout;
      self.idleTimeout = setTimeout(function () {
        self.idleTimeoutHandler();
      }, self.options.pooling.heartBeatInterval);
      if (previousTimeout) {
        //remove the previous timeout for the idle request
        clearTimeout(previousTimeout);
      }
    }
    //noinspection JSUnresolvedVariable
    self.streamHandlers[request.streamId] = {
      callback: callback,
      rowCallback: options && options.rowCallback,
      options: options,
      timeout: self.setRequestTimeout(request, options)
    };
  });
};

/**
 * Function that gets executed once the idle timeout has passed to issue a request to keep the connection alive
 */
Connection.prototype.idleTimeoutHandler = function () {
  var self = this;
  if (this.sendingIdleQuery) {
    //don't issue another
    //schedule for next time
    this.idleTimeout = setTimeout(function () {
      self.idleTimeoutHandler();
    }, this.options.pooling.heartBeatInterval);
    return;
  }
  this.log('verbose', 'Connection idling, issuing a Request to prevent idle disconnects');
  this.sendingIdleQuery = true;
  this.sendStream(new requests.QueryRequest(idleQuery), utils.emptyObject, function (err) {
    self.sendingIdleQuery = false;
    if (!err) {
      //The sending succeeded
      //There is a valid response but we don't care about the response
      return;
    }
    self.log('warning', 'Received heartbeat request error', err);
    self.emit('idleRequestError', err, self);
  });
};

/**
 * Returns an available streamId or null if there isn't any available
 * @returns {Number}
 */
Connection.prototype.getStreamId = function() {
  return this.streamIds.pop();
};

Connection.prototype.freeStreamId = function(header) {
  var streamId = header.streamId;
  if (streamId < 0) {
    return;
  }
  delete this.streamHandlers[streamId];
  this.streamIds.push(streamId);
  if (this.emitDrain && this.streamIds.inUse === 0 && this.pendingWrites.length === 0) {
    this.emit('drain');
  }
  this.writeNext();
  this.log('verbose', 'Done receiving frame #' + streamId);
};

Connection.prototype.writeNext = function () {
  var self = this;
  setImmediate(function writeNextPending() {
    var pending = self.pendingWrites.shift();
    if (!pending) {
      return;
    }
    self.sendStream(pending.request, pending.options, pending.callback);
  });
};

/**
 * Returns the number of requests waiting for response
 * @returns {Number}
 */
Connection.prototype.getInFlight = function () {
  return this.streamIds.inUse;
};

/**
 * Handles a result and error response
 */
Connection.prototype.handleResult = function (header, err, result) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.log('verbose', 'event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.log('error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.log('verbose', 'Received frame #' + streamId + ' from ' + this.endpoint);
  this.invokeCallback(handler, err, result);
};

Connection.prototype.handleNodeEvent = function (header, event) {
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
};

/**
 * Handles a row response
 */
Connection.prototype.handleRow = function (header, row, meta, rowLength, flags) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.log('verbose', 'Event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.log('error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.log('verbose', 'Received streaming frame #' + streamId);
  if (handler.timeout) {
    //It started receiving, clear the read timeout
    clearTimeout(handler.timeout);
    handler.timeout = null;
  }
  if (!handler.rowCallback) {
    return this.invokeCallback(
      handler,
      new errors.DriverInternalError('RowCallback not found for streaming frame handler'));
  }
  handler.rowIndex = handler.rowIndex || 0;
  handler.rowCallback(handler.rowIndex++, row, rowLength);
  if (handler.rowIndex === rowLength) {
    this.invokeCallback(handler, null, { rowLength: rowLength, meta: meta, flags: flags });
  }
};

/**
 * Invokes the handler callback and clears the callback and timers
 * @param {{callback, timeout}} handler
 * @param {Error} err
 * @param [response]
 */
Connection.prototype.invokeCallback = function (handler, err, response) {
  var callback = handler.callback;
  //Prevent chained invocations
  handler.callback = null;
  if (handler.timeout) {
    clearTimeout(handler.timeout);
    handler.timeout = null;
  }
  if (callback) {
    callback(err, response);
  }
};

/**
 * Creates a timeout for the given request and returns the timeout object.
 * @param request
 * @param {QueryOptions} options
 * @returns {Object} The timeout object
 */
Connection.prototype.setRequestTimeout = function (request, options) {
  var millis = (options && options.readTimeout !== undefined) ? options.readTimeout : this.options.socketOptions.readTimeout;
  if (!(millis > 0)) {
    //read timeout disabled
    return null;
  }
  var self = this;
  return setTimeout(function () {
    self.onTimeout(request.streamId, millis);
  }, millis);
};

/**
 * Executed on request timeout, it callbacks with OperationTimedOutError and clears the closures
 */
Connection.prototype.onTimeout = function (streamId, millis) {
  var handler = this.streamHandlers[streamId];
  if (!handler || !handler.callback) {
    //it's being cleared, don't mind
    return;
  }
  this.timedOutHandlers++;
  var originalCallback = handler.callback;
  var self = this;
  //clear callback closures
  handler.callback = function replyAfterTimeout() {
    //if replies, remove from timedOutQueries
    self.timedOutHandlers--;
  };
  if (handler.rowCallback) {
    handler.rowCallback = function noop() {};
  }
  var message = util.format('The host %s did not reply before timeout %d ms', this.endpoint, millis);
  originalCallback(new errors.OperationTimedOutError(message));
};

/**
 * Closes the socket (if not already closed) and cancels all in-flight requests.
 * Multiple calls to this method have no additional side-effects.
 * @param {Function} [callback]
 */
Connection.prototype.close = function (callback) {
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
  this.log('verbose', 'Closing connection to ' + this.endpoint);
  var self = this;
  this.netClient.once('close', function (hadError) {
    if (hadError) {
      self.log('info', 'The socket closed with a transmission error');
    }
    setImmediate(callback);
  });
  // Prevent 'error' listener to be executed before 'close' listener
  this.netClient.removeAllListeners('error');
  // Add a noop handler for 'error' event to prevent Socket to throw the error
  this.netClient.on('error', utils.noop);
  // Half-close the socket, it will result in 'close' event being fired
  this.netClient.end();
};

module.exports = Connection;
