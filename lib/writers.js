var async = require('async');
var events = require('events');
var util = require('util');

var encoder = require('./encoder.js');
var types = require('./types.js');
var utils = require('./utils.js');
var FrameHeader = types.FrameHeader;
/**
 * FrameWriter: Contains the logic to write all the different types to the frame.
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameBuilder.js
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
function FrameWriter(opcodename, streamId) {
  if (!opcodename) {
    throw new Error('Opcode not provided');
  }
  this.streamId = streamId;
  this.buffers = [];
  this.opcode = types.opcodes[opcodename.toString().toLowerCase()];
}

FrameWriter.prototype.writeShort = function(num) {
  var buf = new Buffer(2);
  buf.writeUInt16BE(num, 0);
  this.buffers.push(buf);
};

FrameWriter.prototype.writeInt = function(num) {
  var buf = new Buffer(4);
  buf.writeInt32BE(num, 0);
  this.buffers.push(buf);
};

FrameWriter.prototype.writeBytes = function(bytes) {
  if(bytes === null) {
    this.writeInt(-1);
  } 
  else {
    this.writeInt(bytes.length);
    this.buffers.push(bytes);
  }
};

FrameWriter.prototype.writeShortBytes = function(bytes) {
  if(bytes === null) {
    this.writeShort(-1);
  } 
  else {
    this.writeShort(bytes.length);

    this.buffers.push(bytes);
  }
};

FrameWriter.prototype.writeString = function(str) {
  if (typeof str === "undefined") {
    throw new Error("can not write undefined");
  }
  var len = Buffer.byteLength(str, 'utf8');
  var buf = new Buffer(2 + len);
  buf.writeUInt16BE(len, 0);
  buf.write(str, 2, buf.length-2, 'utf8');
  this.buffers.push(buf);
};

FrameWriter.prototype.writeLString = function(str) {
  var len = Buffer.byteLength(str, 'utf8');
  var buf = new Buffer(4 + len);
  buf.writeInt32BE(len, 0);
  buf.write(str, 4, buf.length-4, 'utf8');
  this.buffers.push(buf);
};

FrameWriter.prototype.writeStringList = function(strings) {
  this.writeShort (strings.length);
  var self = this;
  strings.forEach(function(str) {
      self.writeString(str);
  });
};

FrameWriter.prototype.writeStringMap = function (map) {
  var keys = [];
  for (var k in map) {
    if (map.hasOwnProperty(k)) {
      keys.push(k);
    }
  }

  this.writeShort(keys.length);

  for(var i = 0; i < keys.length; i++) {
    var key = keys[i];
    this.writeString(key);
    this.writeString(map[key]);
  }
};

FrameWriter.prototype.write = function() {
  var body = Buffer.concat(this.buffers);
  this.streamId = parseInt(this.streamId, 10);
  if (!(this.streamId >= 0 &&  this.streamId < 128)) {
    throw new types.DriverError('streamId must be a number from 0 to 127');
  }
  var header = new FrameHeader({streamId: this.streamId, opcode: this.opcode, bodyLength: body.length});

  return Buffer.concat([header.toBuffer(), body], body.length + FrameHeader.size);
};

function QueryWriter(query, params, consistency) {
  this.query = query;
  this.params = params;
  this.consistency = consistency;
  this.streamId = null;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.getDefault();
  }
}

QueryWriter.prototype.write = function () {
  var frameWriter = new FrameWriter('QUERY', this.streamId);
  var query = types.queryParser.parse(this.query, this.params, encoder.stringifyValue);
  frameWriter.writeLString(query);
  frameWriter.writeShort(this.consistency);
  return frameWriter.write();
};

function PrepareQueryWriter(query) {
  this.streamId = null;
  this.query = query;
}

PrepareQueryWriter.prototype.write = function () {
  var frameWriter = new FrameWriter('PREPARE', this.streamId);
  frameWriter.writeLString(this.query);
  return frameWriter.write();
};

function StartupWriter(cqlVersion) {
  this.cqlVersion = cqlVersion;
  this.streamId = null;
}

StartupWriter.prototype.write = function() {
  var frameWriter = new FrameWriter('STARTUP', this.streamId);
  frameWriter.writeStringMap({
    CQL_VERSION: this.cqlVersion
  });
  return frameWriter.write();
};

function RegisterWriter(events) {
  this.events = events;
  this.streamId = null;
}

RegisterWriter.prototype.write = function() {
  var frameWriter = new FrameWriter('REGISTER', this.streamId);
  frameWriter.writeStringList(this.events);
  return frameWriter.write();
};

function CredentialsWriter(username, password) {
  this.username = username;
  this.password = password;
  this.streamId = null;
}

CredentialsWriter.prototype.write = function() {
  var frameWriter = new FrameWriter('CREDENTIALS', this.streamId);
  frameWriter.writeStringMap({username:this.username,password:this.password});
  return frameWriter.write();
};
/**
 * Writes a execute query (given a prepared queryId)
 */
function ExecuteWriter(queryId, params, consistency) {
  this.queryId = queryId;
  this.params = params ? params : [];
  this.consistency = consistency;
  this.streamId = null;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.getDefault();
  }
}

ExecuteWriter.prototype.write = function () {
  var frameWriter = new FrameWriter('EXECUTE', this.streamId);
  frameWriter.writeShortBytes(this.queryId);
  frameWriter.writeShort(this.params.length);
  for (var i=0; i<this.params.length; i++) {
    frameWriter.writeBytes(encoder.encode(this.params[i]));
  }
  frameWriter.writeShort(this.consistency);
  return frameWriter.write();
};
/**
 * Represents a queue that process one write at a time (FIFO).
 */
function WriteQueue (netClient) {
  WriteQueue.super_.call(this);
  this.isRunning = false;
  this.queue = [];
  this.netClient = netClient;
}

util.inherits(WriteQueue, events.EventEmitter);
/**
 * Pushes / enqueues
 */
WriteQueue.prototype.push = function (writer, callback) {
  this.queue.push({writer: writer, callback: callback});
  this.run();
};

WriteQueue.prototype.run = function () {
  if (!this.isRunning) {
    this.process();
  }
};

WriteQueue.prototype.process = function () {
  var self = this;
  async.whilst(
    function () {
      return self.queue.length > 0;
    },
    function (next) {
      self.isRunning = true;
      var writeItem = self.queue.shift();
      var data = null;
      var startTime = process.hrtime();
      try {
        data = writeItem.writer.write();
        self.emit('perf', 'serialize', process.hrtime(startTime));
      }
      catch (err) {
        writeCallback(err);
        return;
      }
      self.netClient.write(data, writeCallback);
      
      function writeCallback(err) {
        writeItem.callback(err);
        //it is better to queue it up on the event loop
        //to allow IO between writes
        setImmediate(next);
      }
    },
    function () {
      //the queue is empty
      self.isRunning = false;
    }
  );
};

exports.CredentialsWriter = CredentialsWriter;
exports.PrepareQueryWriter = PrepareQueryWriter;
exports.QueryWriter = QueryWriter;
exports.RegisterWriter = RegisterWriter;
exports.StartupWriter = StartupWriter;
exports.ExecuteWriter = ExecuteWriter;
exports.WriteQueue = WriteQueue;
