var net = require('net');
var events = require('events');
var util = require('util');
var async = require('async');
var Readers = require('./readers.js');
var Writers = require('./writers.js');
var utils = require('./utils.js');
var ResponseHandler = Readers.ResponseHandler;
var EmptyResponseHandler = Readers.EmptyResponseHandler;

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

  this.streamHandlers = {}; 
  this.options = utils.extend(options, optionsDefault);
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.createSocket = function() {
  var self = this;
  self.netClient = new net.Socket();
  self.writeQueue = new Writers.WriteQueue(self.netClient);
  var protocol = new Readers.ProtocolParser({objectMode: true});
  var frameParser = new Readers.FrameParser({objectMode: true});
  var frameStreamingParser = new Readers.FrameStreamingParser({objectMode: true});
  self.netClient.pipe(protocol);
  protocol.pipe(frameParser);
  protocol.pipe(frameStreamingParser);
  
  frameParser.on('readable', function (){
    var frame = null;
    while ((frame = frameParser.read()) !== null) {
      self.handleFrame(frame);
    }
  });
  self.netClient.on('error', function() {
    self.emit('log', 'error', 'TCP error');
  });
  self.netClient.on('end', function() {
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
      if (self.options.keyspace) {
        self.execute('USE ' + self.options.keyspace + ';', null, connectionReadyCallback);
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
    self.sendStream(new Writers.StartupWriter(self.options.version), new ResponseHandler(startupCallback, (self.authenticate).bind(self)));
  });
}

Connection.prototype.authenticate = function(callback) {
    if (!this.options.username) {
      //TODO: Callback
      throw new Error("Server needs authentication which was not provided");
    }
    else {
      this.sendStream(new Writers.CredentialsWriter(this.options.username, this.options.password), new EmptyResponseHandler(callback));
    }
}
/**
 * Executes a query sending a QUERY stream to the host
 */
Connection.prototype.execute = function (query, params, consistency, callback) {
  if(typeof callback === 'undefined') {
    if (typeof consistency === 'undefined') {
      //only the query and the callback was specified
      callback = params;
      params = null;
    }
    else {
      callback = consistency;
      consistency = null;
      if (typeof params === 'number') {
        consistency = params;
        params = null;
      }
    }
  }
  query = utils.queryParser.parse(query, params);
  this.emit('log', 'info', 'executing query: ' + query);
  this.sendStream(new Writers.QueryWriter(query, consistency), new ResponseHandler(callback));
}

/**
 * Executes a (previously) prepared statement with a given id
 */
Connection.prototype.executePrepared = function (queryId, params, consistency, callback) {
  this.emit('log', 'info', 'executing prepared query: ' + queryId);
  this.sendStream(new Writers.ExecuteWriter(queryId, params, consistency), new ResponseHandler(callback));
}

Connection.prototype.executeToStream = function (query, params, consistency, callback) {
  this.sendStream(new Writers.QueryWriter(query, consistency), new ResponseHandler(callback));
}

Connection.prototype.prepare = function (query, callback) {
  this.emit('log', 'info', 'preparing query: ' + query);
  this.sendStream(new Writers.PrepareQueryWriter(query), new ResponseHandler(callback));
}

Connection.prototype.register = function register(events, callback) {
  this.sendStream(new Writers.RegisterWriter(events), new EmptyResponseHandler(callback));
}

Connection.prototype.sendStream = function sendStream(frameWriter, callback) {
  if(typeof callback == 'undefined') {
    callback = function() {};
  }
  this.getStreamId(function(streamId) {
    this.streamHandlers[streamId] = callback;
    this.emit('log', 'info', 'sending stream #' + streamId);
    this.writeQueue.push(frameWriter.write(streamId));
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

/**
 * Handles a response frame
 */
Connection.prototype.handleFrame = function(data) {
  var streamId = data.header.streamId;
  if(streamId >= 0) {
    var handler = this.streamHandlers[streamId];
    delete this.streamHandlers[streamId];
    this.freeStreamId(streamId);
    this.emit('log', 'info', 'received stream:#' + streamId + ';total available currently: ' + (this.availableStreamIds.length));
    if (handler) {
      if (handler.handle) {
        handler.handle(data);
      }
      else {
        handler(data);
      }
    }
    else {
      this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
    }
  } 
  else {
    this.emit('log', 'info', 'event received', data);
    Readers.readEvent(data, this);
  }
}

Connection.prototype.close = function disconnect(callback) {
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
    this.netClient.on('close', callback);
  }

  this.netClient.end();

  this.availableStreamIds = null;
  this.streamHandlers = {};
}

exports.Connection = Connection;