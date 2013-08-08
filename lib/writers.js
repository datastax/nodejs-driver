/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameBuilder.js
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var types = require('./types.js');
var async = require('async');
/**
 * FrameWriter: Contains the logic to write all the different types to the frame
 */
function FrameWriter(opcodename) {
  if (!opcodename) {
    throw new Error('Opcode not provided');
  }
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
  if(bytes == null) {
      this.writeInt(-1);
  } else {
      this.writeInt(bytes.length);
      this.buffers.push(bytes);
  }
};

FrameWriter.prototype.writeShortBytes = function(bytes) {
  if(bytes == null) {
      this.writeShort(-1);
  } else {
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
  var bufs = [];

  this.writeShort(strings.length);
  var self = this;
  strings.forEach(function(str) {
      self.writeString(str);
  });
};

FrameWriter.prototype.writeStringMap = function(map) {
  var num = 0;
  for(var i in map) {
      num++;
  }

  this.writeShort(num);

  var bufs = [];
  for(var key in map) {
    this.writeString(key);
    this.writeString(map[key]);
  }
};

FrameWriter.prototype.write = function(streamId) {
  var body = Buffer.concat(this.buffers);
  //version and flags
  var head = new Buffer([0x01, 0, streamId, [this.opcode]]);
  var length = new Buffer(4);
  length.writeUInt32BE(body.length, 0);

  return Buffer.concat([head, length, body]);
};

function QueryWriter(query, consistency) {
  this.query = query;
  this.consistency = consistency;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.quorum;
  }
}

QueryWriter.prototype.write = function (streamId) {
  var frameWriter = new FrameWriter('QUERY');
  frameWriter.writeLString(this.query);
  frameWriter.writeShort(this.consistency);
  return frameWriter.write(streamId);
}

function PrepareQueryWriter(query) {
  this.query = query;
}

PrepareQueryWriter.prototype.write = function (streamId) {
  var frameWriter = new FrameWriter('PREPARE');
  frameWriter.writeLString(this.query);
  return frameWriter.write(streamId);
}

function StartupWriter(cqlVersion) {
  this.cqlVersion = cqlVersion;
}

StartupWriter.prototype.write = function(streamId) {
  var frameWriter = new FrameWriter('STARTUP');
  frameWriter.writeStringMap({
    CQL_VERSION: this.cqlVersion
  });
  return frameWriter.write(streamId);
}

function RegisterWriter(events) {
  this.events = events;
}

RegisterWriter.prototype.write = function(streamId) {
  var frameWriter = new FrameWriter('REGISTER');
  frameWriter.writeStringList(this.events);
  return frameWriter.write(streamId);
}

function CredentialsWriter(username, password) {
  this.username = username;
  this.password = password;
}

CredentialsWriter.prototype.write = function(streamId) {
  var frameWriter = new FrameWriter('CREDENTIALS');
  frameWriter.writeStringMap({username:this.username,password:this.password});
  return frameWriter.write(streamId);
}
/**
 * Writes a execute query (given a prepared queryId)
 */
function ExecuteWriter(queryId, params, consistency) {
  this.queryId = queryId;
  this.params = params ? params : [];
  this.consistency = consistency;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.quorum;
  }
}

ExecuteWriter.prototype.write = function (streamId) {
  var frameWriter = new FrameWriter('EXECUTE');
  frameWriter.writeShortBytes(this.queryId);
  frameWriter.writeShort(0);
  //frameWriter.writeShort(params.length);
  //for (var i=0;i<params.length;i++) {
  //  frameWriter.writeBytes();
  //}
  frameWriter.writeShort(this.consistency);
  return frameWriter.write(streamId);
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
        //to allow IO between writes
        setImmediate(callback);
      });
    },
    function (err) {
      //the queue is empty
      self.isRunning = false;
    }
  );
}

exports.CredentialsWriter = CredentialsWriter;
exports.PrepareQueryWriter = PrepareQueryWriter;
exports.QueryWriter = QueryWriter;
exports.RegisterWriter = RegisterWriter;
exports.StartupWriter = StartupWriter;
exports.ExecuteWriter = ExecuteWriter;
exports.WriteQueue = WriteQueue;
