var async = require('async');
var events = require('events');
var util = require('util');

var encoder = require('./encoder.js');
var types = require('./types.js');
var utils = require('./utils.js');
var FrameHeader = types.FrameHeader;

/**
 *  Options for the execution of the query / prepared statement
 */
var queryFlag = {
  values:                 0x01,
  skipMetadata:           0x02,
  pageSize:               0x04,
  withPagingState:        0x08,
  withSerialConsistency:  0x10
};

/**
 * FrameWriter: Contains the logic to write all the different types to the frame.
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameBuilder.js
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
function FrameWriter(opcode, streamId) {
  if (!opcode) {
    throw new Error('Opcode not provided');
  }
  this.streamId = streamId;
  this.buffers = [];
  this.opcode = opcode;
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

/**
 * Writes bytes according to Cassandra <int byteLength><bytes>
 * @param {Buffer} bytes
 */
FrameWriter.prototype.writeBytes = function(bytes) {
  if(bytes === null) {
    this.writeInt(-1);
  } 
  else {
    this.writeInt(bytes.length);
    this.buffers.push(bytes);
  }
};

/**
 * Writes "short bytes" according to Cassandra protocol <short byteLength><bytes>
 * @param {Buffer} bytes
 */
FrameWriter.prototype.writeShortBytes = function(bytes) {
  if(bytes === null) {
    this.writeShort(-1);
  } 
  else {
    this.writeShort(bytes.length);
    this.buffers.push(bytes);
  }
};

/**
 * Writes a single byte
 * @param {Number} num Value of the byte, a number between 0 and 255.
 */
FrameWriter.prototype.writeByte = function (num) {
  this.buffers.push(new Buffer([num]));
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

/**
 * Writes a plain text credentials.
 */
FrameWriter.prototype.writePlainCredentials = function(username, password) {
  //It would be great to move this out of the writer into an authProvider.
  this.writeInt(Buffer.byteLength(username) + Buffer.byteLength(password) + 2);
  this.buffers.push(new Buffer([0]));
  this.buffers.push(new Buffer(username, 'utf8'));
  this.buffers.push(new Buffer([0]));
  this.buffers.push(new Buffer(password, 'utf8'));
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

function QueryWriter(query, params, consistency, pageSize, pageState) {
  this.query = query;
  this.params = params || [];
  this.consistency = consistency;
  this.pageSize = pageSize;
  this.pageState = pageState;
  this.streamId = null;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.getDefault();
  }
}

QueryWriter.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.query, this.streamId);
  //var query = types.queryParser.parse(this.query, this.params, encoder.stringifyValue);
  frameWriter.writeLString(this.query);
  frameWriter.writeShort(this.consistency);
  //flags
  var flags = 0;
  flags += (this.params.length > 0 || 0) && queryFlag.values;
  //only supply page size when there is no page state
  flags += (this.pageSize && !this.pageState) ? queryFlag.pageSize : 0;
  flags += this.pageState ? queryFlag.pageState : 0;
  frameWriter.writeByte(flags);
  if (this.params.length) {
    frameWriter.writeShort(this.params.length);
    for (var i = 0; i < this.params.length; i++) {
      frameWriter.writeBytes(encoder.encode(this.params[i]));
    }
  }
  if (this.pageSize && !this.pageState) {
    frameWriter.writeInt(this.pageSize);
  }
  if (this.pageState) {
    frameWriter.writeBytes(this.pageState);
  }

  return frameWriter.write();
};

function PrepareQueryWriter(query) {
  this.streamId = null;
  this.query = query;
}

PrepareQueryWriter.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.prepare, this.streamId);
  frameWriter.writeLString(this.query);
  return frameWriter.write();
};

function StartupWriter(cqlVersion) {
  this.cqlVersion = cqlVersion;
  this.streamId = null;
}

StartupWriter.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.startup, this.streamId);
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
  var frameWriter = new FrameWriter(types.opcodes.register, this.streamId);
  frameWriter.writeStringList(this.events);
  return frameWriter.write();
};

function CredentialsWriter(username, password) {
  this.username = username;
  this.password = password;
  this.streamId = null;
}

CredentialsWriter.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.auth_response, this.streamId);
  frameWriter.writePlainCredentials(this.username,this.password);
  return frameWriter.write();
};
/**
 * Writes a execute query (given a prepared queryId)
 */
function ExecuteWriter(queryId, params, consistency) {
  this.queryId = queryId;
  this.params = params || [];
  this.consistency = consistency;
  this.streamId = null;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.getDefault();
  }
}

ExecuteWriter.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.execute, this.streamId);
  frameWriter.writeShortBytes(this.queryId);
  frameWriter.writeShort(this.consistency);
  //flags: with values
  frameWriter.writeByte(0x01);
  frameWriter.writeShort(this.params.length);
  for (var i=0; i<this.params.length; i++) {
    frameWriter.writeBytes(encoder.encode(this.params[i]));
  }
  return frameWriter.write();
};

/**
 * Writes a batch request
 * @param {Array} queries Array of objects with the properties query and params
 * @constructor
 */
function BatchWriter(queries, consistency, options) {
  this.queries = queries;
  options = utils.extend({atomic: true}, options);
  this.type = options.atomic ? 0 : 1;
  this.type = options.counter ? 2 : this.type;
  this.consistency = consistency;
  this.streamId = null;
  if (consistency === null || typeof consistency === 'undefined') {
    this.consistency = types.consistencies.getDefault();
  }
}

BatchWriter.prototype.write = function () {
  if (!this.queries || !(this.queries.length > 0)) {
    throw new TypeError(util.format('Invalid queries provided %s', this.queries));
  }
  var frameWriter = new FrameWriter(types.opcodes.batch, this.streamId);
  frameWriter.writeByte(this.type);
  frameWriter.writeShort(this.queries.length);
  this.queries.forEach(function (item, index) {
    var query = item && item.query;
    var params = item && item.params;
    if (typeof item === 'string') {
      query = item;
    }
    if (!query) {
      throw new TypeError(util.format('Invalid query at index %d', index));
    }
    //kind flag for not prepared
    frameWriter.writeByte(0);
    frameWriter.writeLString(query);
    params = item.params || [];
    frameWriter.writeShort(params.length);
    params.forEach(function (param) {
      frameWriter.writeBytes(encoder.encode(param));
    }, this);
  }, this);

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
exports.BatchWriter = BatchWriter;
exports.WriteQueue = WriteQueue;
