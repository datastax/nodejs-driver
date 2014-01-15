var net = require('net');
var events = require('events');
var util = require('util');
var writers = require('./writers.js');
var streams = require('./streams.js');
var utils = require('./utils.js');
var types = require('./types.js');

var optionsDefault = {
  port:  9042,
  version: '3.0.0',
  //max simultaneous requests (before waiting for a response) (max=128)
  maxRequests: 32,
  //When the simultaneous requests has been reached, it determines the amount of milliseconds before retrying to get an available streamId
  maxRequestsRetry: 100,
  //Connect timeout: time to wait when trying to connect to a host,
  connectTimeout: 2000
};
function Connection(options) {
  events.EventEmitter.call(this);

  this.streamHandlers = {};
  this.options = utils.extend({}, optionsDefault, options);
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.createSocket = function() {
  var self = this;
  if (this.netClient) {
    this.netClient.destroy();
  }
  this.netClient = new net.Socket();
  this.writeQueue = new writers.WriteQueue(this.netClient);
  var protocol = new streams.Protocol({objectMode: true});
  this.parser = new streams.Parser({objectMode: true});
  var resultEmitter = new streams.ResultEmitter({objectMode: true});
  this.netClient
    .pipe(protocol)
    .pipe(this.parser)
    .pipe(resultEmitter);

  resultEmitter.on('result', this.handleResult.bind(this));
  resultEmitter.on('row', this.handleRow.bind(this));
  resultEmitter.on('frameEnded', this.freeStreamId.bind(this));
  
  this.netClient.on('close', function() {
    self.emit('log', 'info', 'Socket disconnected');
    self.connected = false;
    self.connecting = false;
    self.invokePendingCallbacks();
  });
};

/** 
 * Connects a socket and sends the startup protocol messages, including authentication and the keyspace used. 
 */
Connection.prototype.open = function (callback) {
  var self = this;
  self.emit('log', 'info', 'Connecting to ' + this.options.host + ':' + this.options.port);
  self.createSocket();
  self.connecting = true;
  function errorConnecting (err, destroy) {
    self.connecting = false;
    if (destroy) {
      //there is a TCP connection that should be killed.
      self.netClient.destroy();
    }
    callback(err);
  }
  this.netClient.once('error', errorConnecting);
  this.netClient.once('timeout', function connectTimeout() {
    var err = new types.DriverError('Connection timeout');
    errorConnecting(err, true);
  });
  this.netClient.setTimeout(this.options.connectTimeout);
  
  this.netClient.connect(this.options.port, this.options.host, function connectCallback() {
    self.emit('log', 'info', 'Socket connected to ' + self.options.host + ':' + self.options.port);
    self.netClient.removeListener('error', errorConnecting);
    self.netClient.removeAllListeners('connect');
    self.netClient.removeAllListeners('timeout');
    
    self.sendStream(new writers.StartupWriter(self.options.version), null, function (err, response) {
      if (response && response.mustAuthenticate) {
        return self.authenticate(startupCallback);
      }
      startupCallback(err);
    });
  });

  function startupCallback(err) {
    if (err) {
      return errorConnecting(err, true);
    }
    //The socket is connected and the connection is authenticated
    if (!self.options.keyspace) {
      return self.connectionReady(callback);
    }
    //Use the keyspace
    self.execute('USE ' + self.options.keyspace + ';', null, function (err) {
      if (err) {
        return errorConnecting(err, true);
      }
      self.connectionReady(callback);
    });
  }
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

/**
 * Handle socket errors, if the socket is not readable invoke all pending callbacks
 */
Connection.prototype.handleSocketError = function (err) {
  this.emit('log', 'error', 'Socket error ' + err, 'r/w:', this.netClient.readable, this.netClient.writable);
  this.invokePendingCallbacks(err);
};

/**
 * Invokes all pending callback of sent streams
 */
Connection.prototype.invokePendingCallbacks = function (innerError) {
  var err = new types.DriverError('Socket was closed');
  err.isServerUnhealthy = true;
  if (innerError) {
    err.innerError = innerError;
  }
  //invoke all pending callbacks
  var handlers = [];
  for (var streamId in this.streamHandlers) {
    if (this.streamHandlers.hasOwnProperty(streamId)) {
      handlers.push(this.streamHandlers[streamId]);
    }
  }
  this.streamHandlers = {};
  if (handlers.length > 0) {
    this.emit('log', 'info', 'Invoking ' + handlers.length + ' pending callbacks');
  }
  handlers.forEach(function (item) {
    if (!item.callback) return;
    item.callback(err);
  });
};

Connection.prototype.authenticate = function(callback) {
  if (!this.options.username) {
    return callback(new Error("Server needs authentication which was not provided"));
  }
  this.sendStream(new writers.CredentialsWriter(this.options.username, this.options.password), null, callback);
};
/**
 * Executes a query sending a QUERY stream to the host
 */
Connection.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'executing query: ' + args.query);
  this.sendStream(new writers.QueryWriter(args.query, args.params, args.consistency), null, args.callback);
};

/**
 * Executes a (previously) prepared statement and yields the rows into a ReadableStream
 * @returns {ResultStream}
 */
Connection.prototype.stream = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'Executing for streaming prepared query: 0x' + args.query.toString('hex'));

  var resultStream = new types.ResultStream({objectMode:true});
  this.sendStream(
    new writers.ExecuteWriter(args.query, args.params, args.consistency),
    utils.extend({}, args.options, {resultStream: resultStream}),
    args.callback);
  return resultStream;
};

/**
 * Executes a (previously) prepared statement with a given id
 * @param {Buffer} queryId
 * @param {Array} [params]
 * @param {Number} [consistency]
 * @param [options]
 * @param {function} callback
 */
Connection.prototype.executePrepared = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'Executing prepared query: 0x' + args.query.toString('hex'));
  //When using each row, the final (end) callback is optional
  if (args.options && args.options.byRow && !args.options.rowCallback) {
    args.options.rowCallback = args.callback;
    args.callback = null;
  }
  this.sendStream(
    new writers.ExecuteWriter(args.query, args.params, args.consistency),
    args.options,
    args.callback);
};

/**
 * Prepares a query on a host
 * @param {String} query
 * @param {function} callback
 */
Connection.prototype.prepare = function (query, callback) {
  this.emit('log', 'info', 'Preparing query: ' + query);
  this.sendStream(new writers.PrepareQueryWriter(query), null, callback);
};

Connection.prototype.register = function register (events, callback) {
  this.sendStream(new writers.RegisterWriter(events), null, callback);
};

/**
 * Uses the frame writer to write into the wire
 * @param frameWriter
 * @param [options]
 * @param {function} [callback]
 */
Connection.prototype.sendStream = function (frameWriter, options, callback) {
  var self = this;
  this.getStreamId(function (streamId) {
    this.emit('log', 'info', 'Sending stream #' + streamId);
    frameWriter.streamId = streamId;
    this.writeQueue.push(frameWriter, writeCallback);
  });
  if (!callback) {
    callback = function noop () {};
  }

  function writeCallback (err) {
    if (err) {
      if (!(err instanceof TypeError)) {
        //TypeError is raised when there is a serialization issue
        //If it is not a serialization issue is a socket issue
        err.isServerUnhealthy = true;
      }
      return callback(err);
    }
    if (frameWriter instanceof writers.ExecuteWriter) {
      if (options && options.byRow) {
        self.parser.setOptions(frameWriter.streamId, {byRow: true, streamField: options.streamField});
      }
      else if (options && options.resultStream) {
        self.parser.setOptions(frameWriter.streamId, {resultStream: options.resultStream});
      }
    }
    self.emit('log', 'info', 'Sent stream #' + frameWriter.streamId);
    self.streamHandlers[frameWriter.streamId] = {
      callback: callback,
      options: options};
  }
};

Connection.prototype.getStreamId = function(callback) {
  var self = this;
  if (!this.availableStreamIds) {
    this.availableStreamIds = [];
    if (this.options.maxRequests > 128) {
      throw new Error('Max requests can not be greater than 128');
    }
    for(var i = 0; i < this.options.maxRequests; i++) {
      this.availableStreamIds.push(i);
    }
    this.getStreamQueue = new types.QueueWhile(function () {
      return self.availableStreamIds.length === 0;
    }, self.options.maxRequestsRetry);
  }
  this.getStreamQueue.push(function () {
    var streamId = self.availableStreamIds.shift();
    callback.call(self, streamId);
  });
};

Connection.prototype.freeStreamId = function(header) {
  var streamId = header.streamId;
  var handler = this.streamHandlers[streamId];
  delete this.streamHandlers[streamId];
  this.availableStreamIds.push(streamId);
  if(handler && handler.callback) {
    handler.callback(null, handler.rowLength);
  }
  this.emit('log', 'info', 'Done receiving frame #' + streamId);
};

/**
 * Handles a result and error response
 */
Connection.prototype.handleResult = function (header, err, result) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.emit('log', 'info', 'event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.emit('log', 'info', 'Received frame #' + streamId);
  var callback = handler.callback;
  callback(err, result);
  //set the callback to null to avoid it being called when freed
  handler.callback = null;
};

/**
 * Handles a row response
 */
Connection.prototype.handleRow = function (header, row, fieldStream, rowLength) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.emit('log', 'info', 'Event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.emit('log', 'info', 'Received streaming frame #' + streamId);
  handler.rowLength = rowLength;
  handler.rowIndex = handler.rowIndex || 0;
  var rowCallback = handler.options && handler.options.rowCallback;
  if (rowCallback) {
    rowCallback(handler.rowIndex++, row, fieldStream, rowLength);
  }
};

Connection.prototype.close = function disconnect (callback) {
  this.emit('log', 'info', 'disconnecting');
  if(callback) {
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
      var err = hadError ? new types.DriverError('The socket was closed due to a transmission error') : null;
      callback(err);
    });
  }

  this.netClient.end();

  this.streamHandlers = {};
};

exports.Connection = Connection;
