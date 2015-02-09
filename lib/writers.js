var async = require('async');
var events = require('events');
var util = require('util');

var types = require('./types');
var utils = require('./utils.js');
var FrameHeader = types.FrameHeader;

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
 * Writes a buffer according to Cassandra protocol: bytes.length (2) + bytes
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

/**
 * Represents a queue that process one write at a time (FIFO).
 * @param {Socket} netClient
 * @param {Encoder} encoder
 */
function WriteQueue (netClient, encoder) {
  WriteQueue.super_.call(this);
  this.netClient = netClient;
  this.encoder = encoder;
  this.isRunning = false;
  this.queue = [];
}

util.inherits(WriteQueue, events.EventEmitter);
/**
 * Enqueues a new request
 * @param {Request} request
 * @param {Function} callback
 */
WriteQueue.prototype.push = function (request, callback) {
  this.queue.push({ request: request, callback: callback});
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
      /** @type {{request: Request, callback: Function}} */
      var writeItem = self.queue.shift();
      var data = null;
      try {
        data = writeItem.request.write(self.encoder);
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

exports.WriteQueue = WriteQueue;
exports.FrameWriter = FrameWriter;
