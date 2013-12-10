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
  maxRequestsRetry: 100
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
  resultEmitter.on('row', this.handleStreamingFrame.bind(this));
  resultEmitter.on('frameEnded', this.freeStreamId.bind(this));
  
  this.netClient.on('close', function() {
    self.emit('log', 'info', 'Socket disconnected');
    self.connected = false;
    self.connecting = false;
    self.invokePendingCallbacks();
  });
  this.netClient.on('error', this.handleSocketError.bind(this));
};

/** 
 * Connects a socket and sends the startup protocol messages, including authentication and the keyspace used. 
 */
Connection.prototype.open = function (callback) {
  var self = this;
  self.emit('log', 'info', 'Connecting to ' + this.options.host + ':' + this.options.port);
  self.createSocket();
  self.connecting = true;
  function errorConnecting (err) {
    self.connecting = false;
    callback(err);
  }
  this.netClient.once('error', errorConnecting);
  
  this.netClient.connect(this.options.port, this.options.host, function connectCallback() {
    self.emit('log', 'info', 'Socket connected to ' + self.options.host + ':' + self.options.port);
    self.netClient.removeListener('error', errorConnecting);
    self.netClient.removeAllListeners('connect');
    
    self.sendStream(new writers.StartupWriter(self.options.version), true, function (err, response) {
      if (err) {
        return errorConnecting(err);
      }
      if (response.mustAuthenticate) {
        return self.authenticate(startupCallback);
      }
      startupCallback();
    });
  });
  
  function startupCallback() {
    if (self.options.keyspace) {
      self.execute('USE ' + self.options.keyspace + ';', null, function (err) {
        if (err) {
          //there is a TCP connection that should be killed.
          self.netClient.end();
          callback(err);
          return;
        }
        connectionReadyCallback();
      });
    }
    else {
      connectionReadyCallback();
    }
  }

  function connectionReadyCallback() {
    self.emit('connected');
    self.connected = true;
    self.connecting = false;
    callback();
  }
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
    handlers.push(this.streamHandlers[streamId]);
  }
  this.streamHandlers = {};
  if (handlers.length > 0) {
    this.emit('log', 'info', 'Invoking ' + handlers.length + ' pending callbacks');
  }
  handlers.forEach(function (item) {
    item.callback(err);
  });
};

Connection.prototype.authenticate = function(callback) {
    if (!this.options.username) {
      throw new Error("Server needs authentication which was not provided");
    }
    else {
      this.sendStream(new writers.CredentialsWriter(this.options.username, this.options.password), false, callback);
    }
};
/**
 * Executes a query sending a QUERY stream to the host
 */
Connection.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'executing query: ' + args.query);
  this.sendStream(new writers.QueryWriter(args.query, args.params, args.consistency), false, args.callback);
};

/**
 * Executes a (previously) prepared statement with a given id
 */
Connection.prototype.executePrepared = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'executing prepared query: 0x' + args.query.toString('hex'));
  this.sendStream(new writers.ExecuteWriter(args.query, args.params, args.consistency),
    false, args.callback, args.options);
};

Connection.prototype.prepare = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'preparing query: ' + args.query);
  this.sendStream(new writers.PrepareQueryWriter(args.query), false, args.callback);
};

Connection.prototype.register = function register (events, callback) {
  this.sendStream(new writers.RegisterWriter(events), false, callback);
};

Connection.prototype.sendStream = function sendStream (frameWriter, rawCallback, callback, options) {
  var self = this;
  this.getStreamId(function (streamId) {
    this.emit('log', 'info', 'sending stream #' + streamId);
    frameWriter.streamId = streamId;
    this.writeQueue.push(frameWriter, writeCallback);
  });
  
  function writeCallback (err) {
    if (err) {
      err.isServerUnhealthy = true;
      callback(err);
      return;
    }
    if (options && options.streamRows) {
      self.parser.setStreaming(frameWriter.streamId, options.streamField);
    }
    self.emit('log', 'info', 'sent stream #' + frameWriter.streamId);
    self.streamHandlers[frameWriter.streamId] = {
      callback: callback,
      rawCallback:rawCallback,
      callOnEnd: options && options.streamRows && !options.streamField,
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
  if(handler && handler.callOnEnd) {
    handler.callback(null, null);
  }
  this.emit('log', 'info', 'Done receiving frame #' + streamId);
};

/**
 * Handles a response frame
 */
Connection.prototype.handleResult = function (header, err, result) {
  var streamId = header.streamId;
  if(streamId >= 0) {
    var handler = this.streamHandlers[streamId];
    this.emit('log', 'info', 'received frame #' + streamId + ';total available currently: ' + (this.availableStreamIds.length));
    if (handler) {
      //TODO: handler.rawCallback ?
      handler.callback(err, result);
    }
    else {
      this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
    }
  }
  else {
    this.emit('log', 'info', 'event received', header);
  }
};

Connection.prototype.handleStreamingFrame = function (err, header, row, fieldStream) {
  var streamId = header.streamId;
  this.emit('log', 'info', 'received streaming frame #' + streamId);
  var handler = this.streamHandlers[streamId];
  if (handler) {
    if(err || !row) {
      handler.callOnEnd = false;
    }
    handler.callback(err, row, fieldStream);
  }
  else {
    this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
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