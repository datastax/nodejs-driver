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
function FrameWriter(opcode) {
  if (!opcode) {
    throw new Error('Opcode not provided');
  }
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

/**
 * @param {Number} version
 * @param {Number} streamId
 * @returns {Buffer}
 */
FrameWriter.prototype.write = function(version, streamId) {
  var body = Buffer.concat(this.buffers);
  this.streamId = parseInt(this.streamId, 10);
  if (this.streamId === null) {
    throw new types.DriverError('streamId not set');
  }
  var header = new FrameHeader({
    version: version,
    streamId: streamId,
    opcode: this.opcode,
    bodyLength: body.length});
  return Buffer.concat([header.toBuffer(), body], body.length + FrameHeader.size);
};

function QueryWriter(query, params, options) {
  this.streamId = null;
  this.query = query;
  this.params = params;
  options = options || {};
  this.consistency = options.consistency || types.consistencies.one;
  this.fetchSize = options.fetchSize;
  this.pageState = options.pageState;
  this.hints = options.hints || [];
}

util.inherits(QueryWriter, ExecuteWriter);

QueryWriter.prototype.write = function () {
  //v1: <query><consistency>
  //v2: <query>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  var frameWriter = new FrameWriter(types.opcodes.query);
  frameWriter.writeLString(this.query);
  if (this.version === 1) {
    frameWriter.writeShort(this.consistency);
  }
  else {
    //Use the same fields as the execute writer
    this.writeQueryParameters(frameWriter);
  }
  return frameWriter.write(this.version, this.streamId);
};
/**
 * Writes a execute query (given a prepared queryId)
 */
function ExecuteWriter(queryId, params, options) {
  this.streamId = null;
  this.queryId = queryId;
  this.params = params;
  options = options || {};
  this.consistency = options.consistency || types.consistencies.one;
  this.fetchSize = options.fetchSize;
  this.pageState = options.pageState;
  this.hints = options.hints || [];
}

ExecuteWriter.prototype.write = function () {
  //v1: <queryId>
  //      <n><value_1>....<value_n><consistency>
  //v2: <queryId>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  var frameWriter = new FrameWriter(types.opcodes.execute);
  frameWriter.writeShortBytes(this.queryId);
  this.writeQueryParameters(frameWriter);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Writes v1 and v2 execute query parameters
 * @param {FrameWriter} frameWriter
 */
ExecuteWriter.prototype.writeQueryParameters = function (frameWriter) {
  //v1: <n><value_1>....<value_n><consistency>
  //v2: <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  if (this.version > 1) {
    var flags = 0;
    flags += (this.params && this.params.length) ? queryFlag.values : 0;
    //only supply page size when there is no page state
    flags += (this.fetchSize > 0) ? queryFlag.pageSize : 0;
    flags += this.pageState ? queryFlag.withPagingState : 0;
    frameWriter.writeShort(this.consistency);
    frameWriter.writeByte(flags);
  }

  if (this.params && this.params.length) {
    frameWriter.writeShort(this.params.length);
    for (var i = 0; i < this.params.length; i++) {
      frameWriter.writeBytes(encoder.encode(this.params[i], this.hints[i]));
    }
  }
  if (this.version === 1) {
    if (!this.params || !this.params.length) {
      //zero parameters
      frameWriter.writeShort(0);
    }
    frameWriter.writeShort(this.consistency);
    return;
  }
  if (this.fetchSize > 0) {
    frameWriter.writeInt(this.fetchSize);
  }
  if (this.pageState) {
    frameWriter.writeBytes(this.pageState);
  }
};



function PrepareQueryWriter(query) {
  this.streamId = null;
  this.query = query;
}

PrepareQueryWriter.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.prepare);
  frameWriter.writeLString(this.query);
  return frameWriter.write(this.version, this.streamId);
};

function StartupWriter(cqlVersion) {
  this.cqlVersion = cqlVersion || '3.0.0';
  this.streamId = null;
}

StartupWriter.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.startup);
  frameWriter.writeStringMap({
    CQL_VERSION: this.cqlVersion
  });
  return frameWriter.write(this.version, this.streamId);
};

function RegisterWriter(events) {
  this.events = events;
  this.streamId = null;
}

RegisterWriter.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.register);
  frameWriter.writeStringList(this.events);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Represents an AUTH_RESPONSE request
 * @param {Buffer} token
 * @constructor
 */
function AuthResponseRequest(token) {
  this.token = token;
  this.streamId = null;
}

AuthResponseRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.authResponse);
  frameWriter.writeBytes(this.token);
  return frameWriter.write(this.version, this.streamId);
};
/**
 *
 * Writes a batch request
 * @param {Array} queries Array of objects with the properties query and params
 * @param {Number} consistency
 * @param {Object} options
 * @constructor
 */
function BatchWriter(queries, consistency, options) {
  this.queries = queries;
  this.type = options.logged ? 0 : 1;
  this.type = options.counter ? 2 : this.type;
  this.consistency = consistency;
  this.streamId = null;
}

BatchWriter.prototype.write = function (version) {
  if (!this.queries || !(this.queries.length > 0)) {
    throw new TypeError(util.format('Invalid queries provided %s', this.queries));
  }
  var frameWriter = new FrameWriter(types.opcodes.batch);
  frameWriter.writeByte(this.type);
  frameWriter.writeShort(this.queries.length);
  this.queries.forEach(function (item, index) {
    if (!item) return;
    var query = item.query;
    if (typeof item === 'string') {
      query = item;
    }
    if (!query) {
      throw new TypeError(util.format('Invalid query at index %d', index));
    }
    //kind flag for not prepared
    frameWriter.writeByte(0);
    frameWriter.writeLString(query);
    var params = item.params || [];
    frameWriter.writeShort(params.length);
    params.forEach(function (param) {
      //TODO: Use hints
      frameWriter.writeBytes(encoder.encode(param));
    }, this);
  }, this);

  frameWriter.writeShort(this.consistency);
  return frameWriter.write(this.version, this.streamId);
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
      try {
        data = writeItem.writer.write();
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

exports.AuthResponseRequest = AuthResponseRequest;
exports.PrepareQueryWriter = PrepareQueryWriter;
exports.QueryWriter = QueryWriter;
exports.RegisterWriter = RegisterWriter;
exports.StartupWriter = StartupWriter;
exports.ExecuteWriter = ExecuteWriter;
exports.BatchWriter = BatchWriter;
exports.WriteQueue = WriteQueue;
