var net = require('net');
var events = require('events');
var util = require('util');
var async = require('async');
var ResponseHandlers = require('./readers.js');
var Writers = require('./writers.js');
var utils = require('./utils.js');
var ResponseHandler = ResponseHandlers.ResponseHandler;
var EmptyResponseHandler = ResponseHandlers.EmptyResponseHandler;
var QueryWriter = Writers.QueryWriter;
var StartupWriter = Writers.StartupWriter;
var CredentialsWriter = Writers.CredentialsWriter;

var optionsDefault = {
  port:  9042,
  version: '3.0.0',
  //max simultaneous requests (before waiting for a response) (max=128)
  maxRequests: 32,
  //When the simultaneous requests has been reached, it determines the amount of milliseconds before retrying to get an available streamId
  maxRequestsRetry: 100
};
function Connection(options) {
  Connection.super_.call(this);

  this.streamCallbacks = {}; 
  this.options = utils.extend(options, optionsDefault);
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.createSocket = function() {
  var self = this;
  self.netClient = new net.Socket();
  self.netClient.on('data', self.handleData.bind(self));
  self.netClient.on('error', function() {
    self.emit('log', 'error', 'Error connecting');
  });

  self.netClient.on('end', function() {
    self.connected = false;
  });
  self.netClient.on('close', function() {
    self.connected = false;
  });
}

/** 
 * Connects a socket and sends the startup protocol messages, including authentication and the keyspace used. 
 */
Connection.prototype.open = function (callback) {
  var self = this;
  self.emit('log', 'info', 'connecting to ' + this.options.host + ':' + this.options.port);
  //TODO: Close previous connection
  self.createSocket();
  self.connecting = true;
  function errorConnecting (err) {
    self.removeListener('error', errorConnecting);
    self.connecting = false;
    callback(err);
  }
  this.netClient.on('error', errorConnecting);
  
  this.netClient.connect(this.options.port, this.options.host, function() {
    self.removeListener('error', errorConnecting);
    function startupCallback() {
      if (self.options.keySpace) {
        self.execute('USE ' + self.options.keySpace + ';', null, connectionReadyCallback);
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
    self.sendStream(new StartupWriter(self.options.version), new ResponseHandler(startupCallback, (self.authenticate).bind(self)));
  });
}

Connection.prototype.authenticate = function(callback) {
    if (!this.options.username) {
      //TODO: Callback
      throw new Error("Server needs authentication which was not provided");
    }
    else {
      this.sendStream(new CredentialsWriter(this.options.username, this.options.password), new EmptyResponseHandler(callback));
    }
}

Connection.prototype.execute = function (query, args, consistency, callback) {
  if(typeof callback == 'undefined') {
    callback = consistency;
    consistency = null;
  }
  query = utils.queryParser.parse(query, args);
  this.emit('log', 'info', 'executing query: ' + query);
  this.sendStream(new QueryWriter(query, consistency), new ResponseHandler(callback));
}

Connection.prototype.register = function register(events, callback) {
  this.sendStream(new RegisterWriter(events), new EmptyResponseHandler(callback));
}

Connection.prototype.sendStream = function sendStream(frameWriter, callback) {
  if(typeof callback == 'undefined') {
    callback = function() {};
  }
  this.getStreamId(function(streamId) {
    this.streamCallbacks[streamId] = callback;
    this.emit('log', 'info', 'sending stream #' + streamId);
    this.netClient.write(frameWriter.write(streamId));
  });
}

Connection.prototype.getStreamId = function(callback) {
  if (!this.availableStreamIds) {
    this.availableStreamIds = [];
    if (this.options.maxRequests > 128) {
      throw new Error('Max requests can not be greater than 128');
    }
    for(var i = 0; i < this.options.maxRequests; i++) {
      this.availableStreamIds.push(i);
    }
  }
  var self = this;
  async.whilst(
    function() {
      return self.availableStreamIds.length === 0;
    },
    function(cb) {
      //there is no stream id available, retry in a while
      setTimeout(cb, self.options.maxRequestsRetry);
    },
    function() {
      var streamId = self.availableStreamIds.pop();
      callback.call(self,streamId);
    }
  );
}

Connection.prototype.freeStreamId = function(streamId) {
  this.availableStreamIds.push(streamId);
}

Connection.prototype.handleData = function handleData(data) {
  var streamId = data.readUInt8(2);

  if(streamId >= 0) {
    this.emit('log', 'info', 'receiving stream:#' + streamId + ';total available currently: ' + (this.availableStreamIds.length));
    var callback = this.streamCallbacks[streamId];
    delete this.streamCallbacks[streamId];
    this.freeStreamId(streamId);
    if(callback) {
      if (callback instanceof EmptyResponseHandler || callback instanceof ResponseHandler) {
        callback.handle(data);
      }
      else {
        callback(data)
      }
    }
    else  {
      this.emit('log', 'error', 'The server replied with a wrong streamId.');
    }
  } 
  else {
    console.log('---------------------------------------event');
    ResponseHandlers.eventResponseHandler(data, this);
  }
}

Connection.prototype.close = function disconnect(callback) {
  this.emit('log', 'info', 'disconnecting');
  if(callback) {
    this.netClient.on('end', callback);
  }

  this.netClient.end();

  this.availableStreamIds = null;
  this.streamCallbacks = {};
}

exports.Connection = Connection;