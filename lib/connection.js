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

  this.streamHandlers = {}; 
  this.options = utils.extend(options, optionsDefault);
}

util.inherits(Connection, events.EventEmitter);

Connection.prototype.createSocket = function() {
  var self = this;
  self.partialStreams = new Array();
  self.netClient = new net.Socket();
  self.writeQueue = new WriteQueue(self.netClient);
  self.netClient.on('data', function (data) {
    setImmediate(function dataHandling(){
      self.handleData.call(self, data);
      data = null;
    });
  });
  //self.netClient.on('data', self.handleData.bind(self));
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

Connection.prototype.handleData = function handleData(data) {
  var headerReader = new ResponseHandlers.FrameHeaderReader(data);
  if (headerReader.isCompleteFrame()) {
    //normal
    this.handleFrame(data, headerReader);
  }
  else if (headerReader.isFrameResponseStart()) {
    //Start partial
    this.partialStreams.push({parts: [data], header: headerReader});
    this.emit('log', 'info', 'Received - start partial #' + headerReader.streamId + ';bytes expected: ' + (headerReader.bodyLength+8) + '; bytes received in the first: ' + data.length);
  }
  else {
    //partial continue, not a valid frame header
    headerReader = null;
    //First in first out
    var partial = this.partialStreams.shift();
    if (!partial) {
      //something is going wrong
      var error = new Error('Unrecognized stream');
      this.emit('log', 'error', error);
    }
    //add the new part to the partial
    //Check if the length matches
    var totalLength = partial.parts[0].length;
    for (var i=1; i<partial.parts.length; i++) {
      totalLength += partial.parts[i].length;
    }
    totalLength += data.length;
    if (totalLength < partial.header.bodyLength+8) {
      //just keep swimming
      partial.parts.push(data);
      this.partialStreams.unshift(partial);
      this.emit('log', 'info', 'Received partial continue #' + partial.header.streamId+ ';bytes expected: ' + (partial.header.bodyLength+8) + '; bytes received so far: ' + totalLength);
      partial = null;
    }
    else if (totalLength === partial.header.bodyLength+8) {
      partial.parts.push(data);
      this.emit('log', 'info', 'Partial end #' + partial.header.streamId);
      this.handleFrame(Buffer.concat(partial.parts, totalLength), partial.header);
      partial = null;
    }
    else{
      //the size is greater than expected.
      //Identify were the next message starts and slice it
      this.emit('log', 'info', 'Receiving end  #' + partial.header.streamId + ' and next message tl:' + totalLength + ' bl:' + partial.header.bodyLength);
      var nextStart = ResponseHandlers.FrameHeaderReader.getNextStart(data);
      if (nextStart.index === -1) {
        //this is not a chunk of this message
        this.emit('log', 'info', 'Frame header could not be found. Total partials: ' + this.partialStreams.length + '; data length: ' + data.length);
        throw new Error('Frame header could not be found');
      }
      else {
        var previousMessageEnd = new Buffer(nextStart.index);
        var nextMessageStart = new Buffer(data.length-nextStart.index);
        data.copy(previousMessageEnd, 0, 0, nextStart.index);
        data.copy(nextMessageStart, 0, nextStart.index, data.length);
        
        //start next message partial
        this.partialStreams.push({parts: [nextMessageStart], header: nextStart.header});
        this.emit('log', 'info', 'Start partial (as next message) #' + nextStart.header.streamId);
        
        //end previous message
        partial.parts.push(previousMessageEnd);
        this.emit('log', 'info', 'Partial end #' + partial.header.streamId);
        this.handleFrame(Buffer.concat(partial.parts, totalLength), partial.header);
        partial = null;
        previousMessageEnd = null;
      }
    }
  }
}

/**
 * Handles a response frame
 */
Connection.prototype.handleFrame = function(data, headerReader) {
  var streamId = headerReader.streamId;
  if(streamId >= 0) {
    this.emit('log', 'info', 'received stream:#' + streamId + ';total available currently: ' + (this.availableStreamIds.length));
    var handler = this.streamHandlers[streamId];
    delete this.streamHandlers[streamId];
    this.freeStreamId(streamId);
    if(handler) {
      if (handler instanceof EmptyResponseHandler || handler instanceof ResponseHandler) {
        handler.handle(data);
        data = null;
      }
      else {
        handler(data);
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
  this.streamHandlers = {};
}

/**
 * Represents a queue that process one write at a time (FIFO).
 */
function WriteQueue (netClient) {
  this.isRunning = false;
  this.queue = [];
  this.netClient = netClient;
}
/**
 * Pushes / enqueues
 */
WriteQueue.prototype.push = function (data) {
  this.queue.push(data);
  this.run();
}

WriteQueue.prototype.run = function () {
  if (!this.isRunning) {
    this.process();
  }
}

WriteQueue.prototype.process = function () {
  var self = this;
  async.whilst(
    function () {
      return self.queue.length > 0;
    },
    function (callback) {
      self.isRunning = true;
      var data = self.queue.shift();
      self.netClient.write(data, function(){
        //it is better to queue it up on the event loop
        //if the call is sync, the order of incoming packets
        setTimeout(callback, 0);
      });
    },
    function (err) {
      //the queue is empty
      self.isRunning = false;
    }
  );
}

exports.Connection = Connection;