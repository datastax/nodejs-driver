var net = require('net');
var events = require('events');
var util = require('util');
var async = require('async');
var tls = require('tls');

var Encoder = require('./encoder.js');
var writers = require('./writers');
var requests = require('./requests');
var streams = require('./streams');
var utils = require('./utils');
var types = require('./types');
var errors = require('./errors');

/**
 * @const
 */
var idleQuery = 'SELECT key from system.local';
/**
 * Represents a connection to a Cassandra node
 * @param {String} address An string containing the name or ip address of the host
 * @param {Number} protocolVersion
 * @param {ClientOptions} options
 * @constructor
 */
function Connection(address, protocolVersion, options) {
  events.EventEmitter.call(this);
  if (!address) {
    throw new Error('Address can not be null')
  }
  this.address = address;
  this.port = options.protocolOptions.port;
  if (address.indexOf(":") > 0) {
    var hostAndPort = address.split(':');
    this.address = hostAndPort[0];
    this.port = hostAndPort[1];
  }
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  if (protocolVersion === null) {
    //Set initial protocol version
    protocolVersion = 2;
    //Allow to check version using this connection instance
    this.checkingVersion = true;
  }
  this.protocolVersion = protocolVersion;
  this.streamHandlers = {};
  this.pendingWrites = [];
  this.preparing = {};
  this.maxRequests = 128;
  /**
   * The timeout state for the idle request (heartbeat)
   */
  this.idleTimeout = null;
  //To save some memory, in the future availableStreamIds could be a custom FIFO queue class
  this.availableStreamIds = [];
  for(var i = 0; i < this.maxRequests; i++) {
    this.availableStreamIds.push(i);
  }
  this.encoder = new Encoder(protocolVersion, options);
  this.setMaxListeners(0);
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.log = utils.log;

/**
 * Binds the necessary event handlers for the (net) stream
 */
Connection.prototype.chainEvents = function() {
  this.writeQueue = new writers.WriteQueue(this.netClient, this.encoder);
  var protocol = new streams.Protocol({objectMode: true});
  this.parser = new streams.Parser({objectMode: true}, this.encoder);
  var resultEmitter = new streams.ResultEmitter({objectMode: true});

  resultEmitter.on('result', this.handleResult.bind(this));
  resultEmitter.on('row', this.handleRow.bind(this));
  resultEmitter.on('frameEnded', this.freeStreamId.bind(this));
  resultEmitter.on('nodeEvent', this.handleNodeEvent.bind(this));

  this.netClient.removeAllListeners('error');
  this.netClient.removeAllListeners('connect');
  this.netClient.removeAllListeners('timeout');
  var self = this;
  this.netClient.on('close', function() {
    self.log('info', 'Connection to ' + self.address + ':' + self.port + ' closed');
    self.connected = false;
    self.connecting = false;
    self.clearAndInvokePending();
  });
  this.netClient
    .pipe(protocol)
    .pipe(this.parser)
    .pipe(resultEmitter);
};

/**
 * Connects a socket and sends the startup protocol messages.
 */
Connection.prototype.open = function (callback) {
  var self = this;
  this.log('info', 'Connecting to ' + this.address + ':' + this.port);
  this.connecting = true;
  if (!this.options.sslOptions) {
    this.netClient = new net.Socket();
    this.netClient.connect(this.port, this.address, function connectCallback() {
      self.log('verbose', 'Socket connected to ' + self.address + ':' + self.port);
      self.chainEvents();
      self.startup(callback);
    });
  }
  else {
    //use TLS
    var sslOptions = utils.extend({rejectUnauthorized: false}, this.options.sslOptions);
    this.netClient = tls.connect(this.port, this.address, sslOptions, function tlsConnectCallback() {
      self.log('verbose', 'Socket connected to ' + self.address + ':' + self.port);
      self.chainEvents();
      self.startup(callback);
    });
  }
  this.netClient.once('error', function (err) {
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
  this.sendStream(new requests.StartupRequest(), null, function (err, response) {
    if (err && self.checkingVersion && self.protocolVersion > 1) {
      if (err instanceof errors.ResponseError &&
          err.code === types.responseErrorCodes.protocolError &&
          err.message.indexOf('Invalid or unsupported protocol version') >= 0
        ) {
        self.log('info', 'Protocol v' + self.protocolVersion + ' not supported, using v' + (self.protocolVersion-1));
        self.decreaseVersion();
        //The host closed the connection, close the socket
        self.close(function () {
          //Retry
          self.open(callback);
        });
        return;
      }
    }
    if (response && response.mustAuthenticate) {
      return self.authenticate(null, null, startupCallback);
    }
    startupCallback(err);
  });

  function startupCallback(err) {
    if (err) {
      return self.errorConnecting(err, true, callback);
    }
    //The socket is connected and the connection is authenticated
    return self.connectionReady(callback);
  }
};


Connection.prototype.errorConnecting = function (err, destroy, callback) {
  this.connecting = false;
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
  this.connecting = false;
  this.netClient.on('error', this.handleSocketError.bind(this));
  callback();
};

Connection.prototype.decreaseVersion = function () {
  this.protocolVersion--;
  this.encoder.setProtocolVersion(this.protocolVersion);
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
  var err = new types.DriverError('Socket was closed');
  err.isServerUnhealthy = true;
  if (innerError) {
    err.innerError = innerError;
  }
  //invoke all in-flight
  var handlers = [];
  for (var streamId in this.streamHandlers) {
    if (this.streamHandlers.hasOwnProperty(streamId)) {
      handlers.push(this.streamHandlers[streamId]);
    }
  }
  this.streamHandlers = {};
  if (handlers.length > 0) {
    this.log('info', 'Invoking ' + handlers.length + ' pending callbacks');
  }
  async.each(handlers, function (item, next) {
    if (!item.callback) return;
    item.callback(err);
    next();
  });

  var pendingWritesCopy = this.pendingWrites;
  this.pendingWrites = [];
  async.each(pendingWritesCopy, function (item, next) {
    if (!item.callback) return;
    item.callback(err);
    next();
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
  if (authenticator === null) {
    //initial token
    if (!this.options.authProvider) {
      return callback(new errors.AuthenticationError('Authentication provider not set'));
    }
    authenticator = this.options.authProvider.newAuthenticator();
    authenticator.initialResponse(function (err, t) {
      //let's start again with the correct args
      if (err) return callback(err);
      self.authenticate(authenticator, t, callback);
    });
    return;
  }
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
  this.sendStream(request, null, function (err, result) {
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
      authenticator.evaluateChallenge(result.token, function (err, t) {
        if (err) {
          return callback(err);
        }
        //here we go again
        self.authenticate(authenticator, t, callback);
      });
    }
    callback(new errors.DriverInternalError('Unexpected response from Cassandra: ' + util.inspect(result)))
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
  if (this.toBeKeyspace === keyspace) {
    return this.once('keyspaceChanged', callback);
  }
  this.toBeKeyspace = keyspace;
  var query = util.format('USE "%s"', keyspace);
  var self = this;
  this.sendStream(
    new requests.QueryRequest(query, null, null),
    null,
    function (err) {
      if (!err) {
        self.keyspace = keyspace;
      }
      callback(err);
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
 * @param options
 * @param {function} callback
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
  if (!callback) {
    callback = function noop () {};
  }
  this.log('verbose', 'Sending stream #' + streamId);
  request.streamId = streamId;
  request.version = this.protocolVersion;
  this.writeQueue.push(request, this.getWriteCallback(request, options, callback));
};

  Connection.prototype.getWriteCallback = function (frameWriter, options, callback){
  var self = this;
  return (function writeCallback (err) {
    if (err) {
      if (!(err instanceof TypeError)) {
        //TypeError is raised when there is a serialization issue
        //If it is not a serialization issue is a socket issue
        err.isServerUnhealthy = true;
      }
      return callback(err);
    }
    if (frameWriter instanceof requests.ExecuteRequest || frameWriter instanceof requests.QueryRequest) {
      if (options && options.byRow) {
        self.parser.setOptions(frameWriter.streamId, {byRow: true, streamField: options.streamField});
      }
    }
    self.log('verbose', 'Sent stream #' + frameWriter.streamId);
    if (self.options.pooling.heartBeatInterval) {
      if (self.idleTimeout) {
        //remove the previous timeout for the idle request
        clearTimeout(self.idleTimeout);
      }
      self.idleTimeout = setTimeout(self.idleTimeoutHandler.bind(self), self.options.pooling.heartBeatInterval);
    }
    self.streamHandlers[frameWriter.streamId] = {
      callback: callback,
      options: options};
  });
};

/**
 * Function that gets executed once the idle timeout has passed to issue a request to keep the connection alive
 */
Connection.prototype.idleTimeoutHandler = function () {
  this.log('verbose', 'Connection idling, issuing a Request to prevent idle disconnects');
  var self = this;
  this.sendStream(new requests.QueryRequest(idleQuery), {}, function (err) {
    if (!err) {
      //The sending succeeded
      //There is a valid response but we don't care about the response
      return;
    }
    self.log('warning', 'Received heartbeat request error', err);
    self.emit('idleRequestError', err);
  });
};

/**
 * Returns an available streamId or null if there isn't any available
 * @returns {Number}
 */
Connection.prototype.getStreamId = function() {
  if (this.availableStreamIds.length === 0) {
    return null;
  }
  return this.availableStreamIds.shift();
};

Connection.prototype.freeStreamId = function(header) {
  var streamId = header.streamId;
  if (streamId < 0) {
    return;
  }
  var handler = this.streamHandlers[streamId];
  delete this.streamHandlers[streamId];
  this.availableStreamIds.push(streamId);
  this.writeNext();
  if(handler && handler.callback) {
    //This could be moved to the handleRow method
    handler.callback(null, {rowLength: handler.rowLength, meta: handler.meta});
  }
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
  return this.maxRequests - this.availableStreamIds.length;
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
  this.log('verbose', 'Received frame #' + streamId);
  var callback = handler.callback;
  //set the callback to null to avoid it being called when freed
  handler.callback = null;
  callback(err, result);
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
Connection.prototype.handleRow = function (header, row, meta, fieldStream, rowLength) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.log('verbose', 'Event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.log('error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.log('verbose', 'Received streaming frame #' + streamId);
  handler.rowLength = rowLength;
  handler.meta = meta;
  handler.rowIndex = handler.rowIndex || 0;
  var rowCallback = handler.options && handler.options.rowCallback;
  if (rowCallback) {
    rowCallback(handler.rowIndex++, row, fieldStream, rowLength);
  }
};

Connection.prototype.close = function (callback) {
  var self = this;
  this.log('verbose', 'disconnecting');
  this.clearAndInvokePending();
  if(!callback) {
    callback = function () {};
  }
  if (!this.netClient) {
    callback();
    return;
  }
  if (!this.connected) {
    this.netClient.destroy();
    callback();
    return;
  }
  this.netClient.on('close', function (hadError) {
    if (hadError) {
      self.log('info', 'The socket closed with a transmission error');
    }
    callback();
  });

  this.netClient.end();

  this.streamHandlers = {};
};

module.exports = Connection;
