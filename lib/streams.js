'use strict';
var util = require('util');
var stream = require('stream');
var Transform = stream.Transform;
var Writable = stream.Writable;

var types = require('./types');
var utils = require('./utils');
var errors = require('./errors');
var FrameHeader = types.FrameHeader;
var FrameReader = require('./readers').FrameReader;

/**
 * Transforms chunks, emits data objects {header, chunk}
 * @param options Stream options
 */
function Protocol (options) {
  Transform.call(this, options);
  this.header = null;
  this.bodyLength = 0;
  this.clearHeaderChunks();
  this.version = 0;
  this.headerSize = 0;
}

util.inherits(Protocol, Transform);

Protocol.prototype._transform = function (chunk, encoding, callback) {
  var error = null;
  try {
    this.readItems(chunk);
  }
  catch (err) {
    error = err;
  }
  callback(error);
};

/**
 * Parses the chunk into frames (header and body).
 * Emits (push) complete frames or frames with incomplete bodies. Following chunks containing the rest of the body will
 * be emitted using the same frame.
 * It buffers incomplete headers.
 * @param {Buffer} chunk
 */
Protocol.prototype.readItems = function (chunk) {
  if (!chunk || chunk.length === 0) {
    return;
  }
  if (this.version === 0) {
    //The server replies the first message with the max protocol version supported
    this.version = FrameHeader.getProtocolVersion(chunk);
    this.headerSize = FrameHeader.size(this.version);
  }
  var offset = 0;
  var currentHeader = this.header;
  this.header = null;
  if (this.headerChunks.byteLength !== 0) {
    //incomplete header was buffered try to read the header from the buffered chunks
    this.headerChunks.parts.push(chunk);
    if (this.headerChunks.byteLength + chunk.length < this.headerSize) {
      this.headerChunks.byteLength += chunk.length;
      return;
    }
    currentHeader = FrameHeader.fromBuffer(Buffer.concat(this.headerChunks.parts, this.headerSize));
    offset = this.headerSize - this.headerChunks.byteLength;
    this.clearHeaderChunks();
  }
  var items = [];
  while (true) {
    if (!currentHeader) {
      if (this.headerSize > chunk.length - offset) {
        if (chunk.length - offset <= 0) {
          break;
        }
        //the header is incomplete, buffer it until the next chunk
        var headerPart = chunk.slice(offset, chunk.length);
        this.headerChunks.parts.push(headerPart);
        this.headerChunks.byteLength = headerPart.length;
        break;
      }
      //read header
      currentHeader = FrameHeader.fromBuffer(chunk, offset);
      offset += this.headerSize;
    }
    //parse body
    var remaining = chunk.length - offset;
    if (currentHeader.bodyLength <= remaining + this.bodyLength) {
      items.push({ header: currentHeader, chunk: chunk, offset: offset, frameEnded: true });
      offset += currentHeader.bodyLength - this.bodyLength;
      //reset the body length
      this.bodyLength = 0;
    }
    else if (remaining >= 0) {
      //the body is not fully contained in this chunk
      //will continue later
      this.header = currentHeader;
      this.bodyLength += remaining;
      if (remaining > 0) {
        //emit if there is at least a byte to emit
        items.push({ header: currentHeader, chunk: chunk, offset: offset, frameEnded: false });
      }
      break;
    }
    currentHeader = null;
  }
  for (var i = 0; i < items.length; i++) {
    this.push(items[i]);
  }
};

Protocol.prototype.clearHeaderChunks = function () {
  this.headerChunks = { byteLength: 0, parts: [] };
};

/**
 * A stream that gets reads header + body chunks and transforms them into header + (row | error)
 * @param {Object} streamOptions Node.js Stream options
 * @param {Encoder} encoder Encoder instance for the parser to use
 */
function Parser (streamOptions, encoder) {
  Transform.call(this, streamOptions);
  //frames that are streaming, indexed by id
  this.frames = {};
  this.encoder = encoder;
}

util.inherits(Parser, Transform);

Parser.prototype._transform = function (item, encoding, callback) {
  var frameInfo = this.frameState(item);

  var error = null;
  try {
    this.parseBody(frameInfo, item);
  }
  catch (err) {
    error = err;
  }
  callback(error);

  if (item.frameEnded) {
    //all the parsing finished and it was streamed down
    //emit an item that signals it
    this.push({ header: frameInfo.header, frameEnded: true});
  }
};

/**
 * @param frameInfo
 * @param {{header: FrameHeader, chunk: Buffer, offset: Number}} item
 */
Parser.prototype.parseBody = function (frameInfo, item) {
  if (!this.handleFrameBuffers(frameInfo, item)) {
    return;
  }
  var reader = new FrameReader(item.header, item.chunk, item.offset);
  if (frameInfo.buffer) {
    reader.unshift(frameInfo.buffer);
    frameInfo.buffer = null;
  }
  //All the body for most operations is already buffered at this stage
  //Except for RESULT
  switch (item.header.opcode) {
    case types.opcodes.result:
      return this.parseResult(frameInfo, reader);
    case types.opcodes.ready:
    case types.opcodes.authSuccess:
      return this.push({ header: frameInfo.header, ready: true });
    case types.opcodes.authChallenge:
      return this.push({ header: frameInfo.header, authChallenge: true, token: reader.readBytes()});
    case types.opcodes.authenticate:
      return this.push({ header: frameInfo.header, mustAuthenticate: true, authenticatorName: reader.readString()});
    case types.opcodes.error:
      return this.push({ header: frameInfo.header, error: reader.readError()});
    case types.opcodes.supported:
      return this.push({ header: frameInfo.header });
    case types.opcodes.event:
      return this.push({ header: frameInfo.header, event: reader.readEvent()});
    default:
      return this.push({ header: frameInfo.header, error: new Error('Received invalid opcode: ' + item.header.opcode) });
  }
};

/**
 * Buffers if needed and returns true if it has all the necessary data to continue parsing the frame.
 * @param frameInfo
 * @param {{header: FrameHeader, chunk: Buffer, offset: Number}} item
 * @returns {Boolean}
 */
Parser.prototype.handleFrameBuffers = function (frameInfo, item) {
  if (!frameInfo.byRow || item.header.opcode !== types.opcodes.result) {
    //Only RESULT operations are allowed to avoid buffering
    var currentLength = (frameInfo.bufferLength || 0) + item.chunk.length - item.offset;
    if (currentLength < item.header.bodyLength) {
      //buffer until the frame is completed
      this.addFrameBuffer(frameInfo, item);
      return false;
    }
    //We have received the full frame body
    if (frameInfo.buffers) {
      item.chunk = this.getFrameBuffer(frameInfo, item);
      item.offset = 0;
    }
    return true;
  }
  if (frameInfo.missingBytes) {
    // Avoid quadratic buffer copying by accumulating chunks for a large cell
    // value in an array until enough bytes are available to read past the
    // cell value.
    if (frameInfo.buffers) {
      frameInfo.buffers.push(item.chunk);
    }
    else if (frameInfo.buffer) {
      frameInfo.buffers = [frameInfo.buffer, item.chunk];
    }
    else {
      frameInfo.buffers = [item.chunk];
    }
    if (item.chunk.length < frameInfo.missingBytes) {
      frameInfo.missingBytes -= item.chunk.length;
      // Don't continue parsing until we have collected enough bytes in the
      // buffer.
      return false;
    }
    else {
      // Now we have enough bytes in the buffers. Concat all chunks into a
      // single buffer & proceed with normal parsing.
      item.chunk = Buffer.concat(frameInfo.buffers);
      frameInfo.missingBytes = null;
      frameInfo.buffer = null;
      frameInfo.buffers = null;
    }
  }
  return true;
};

/**
 * Adds this chunk to the frame buffers.
 * @param frameInfo
 * @param {{header: FrameHeader, chunk: Buffer, offset: Number}} item
 */
Parser.prototype.addFrameBuffer = function (frameInfo, item) {
  if (!frameInfo.buffers) {
    frameInfo.buffers = [ item.chunk.slice(item.offset) ];
    frameInfo.bufferLength = item.chunk.length - item.offset;
    return;
  }
  if (item.offset > 0) {
    throw new errors.DriverInternalError('Following buffers can not have an offset greater than zero');
  }
  frameInfo.buffers.push(item.chunk);
  frameInfo.bufferLength += item.chunk.length;
};

/**
 * Adds the last chunk and concatenates the frame buffers
 * @param frameInfo
 * @param {{header: FrameHeader, chunk: Buffer, offset: Number}} item
 */
Parser.prototype.getFrameBuffer = function (frameInfo, item) {
  frameInfo.buffers.push(item.chunk);
  var result = Buffer.concat(frameInfo.buffers, frameInfo.bodyLength);
  frameInfo.buffers = null;
  return result;
};

/**
 * Tries to read the result in the body of a message
 * @param frameInfo Frame information, header / metadata
 * @param {FrameReader} reader
 */
Parser.prototype.parseResult = function (frameInfo, reader) {
  var originalOffset = reader.offset;
  try {
    if (!frameInfo.meta) {
      frameInfo.flagsInfo = reader.readFlagsInfo();
      frameInfo.kind = reader.readInt();
      if (frameInfo.kind === types.resultKind.prepared) {
        frameInfo.preparedId = utils.copyBuffer(reader.readShortBytes());
      }
      else if (frameInfo.kind === types.resultKind.setKeyspace) {
        frameInfo.keyspace = reader.readString();
      }
      if (frameInfo.kind === types.resultKind.rows ||
          frameInfo.kind === types.resultKind.prepared) {
        frameInfo.meta = reader.readMetadata(frameInfo.kind);
      }
    }
  }
  catch (e) {
    if (e instanceof RangeError) {
      //A controlled error, the kind / metadata is not available to be read yet
      return this.bufferResultCell(frameInfo, reader, originalOffset);
    }
    throw e;
  }
  switch (frameInfo.kind) {
    case types.resultKind.setKeyspace:
      return this.push({ header: frameInfo.header, keyspaceSet: frameInfo.keyspace});
    case types.resultKind.voidResult:
      return this.push({ header: frameInfo.header, id: frameInfo.preparedId, flags: frameInfo.flagsInfo});
    case types.resultKind.schemaChange:
      //it contains additional info that it is not parsed
      if (frameInfo.emitted) {
        return;
      }
      frameInfo.emitted = true;
      return this.push({ header: frameInfo.header, schemaChange: true });
    case types.resultKind.prepared:
      //it contains result metadata that it is not parsed
      if (frameInfo.emitted) {
        return;
      }
      frameInfo.emitted = true;
      return this.push({ header: frameInfo.header, id: frameInfo.preparedId, meta: frameInfo.meta, flags: frameInfo.flagsInfo});
  }
  //it contains rows
  if (reader.remainingLength() > 0) {
    this.parseRows(frameInfo, reader);
  }
};

/**
 * @param frameInfo
 * @param {FrameReader} reader
 */
Parser.prototype.parseRows = function (frameInfo, reader) {
  if (typeof frameInfo.rowLength === 'undefined') {
    try {
      frameInfo.rowLength = reader.readInt();
    }
    catch (e) {
      if (e instanceof RangeError) {
        //there is not enough data to read this row
        this.bufferResultCell(frameInfo, reader);
        return;
      }
      throw e;
    }
  }
  if (frameInfo.rowLength === 0) {
    return this.push({ header: frameInfo.header, result: { rows: utils.emptyArray, meta: frameInfo.meta, flags: frameInfo.flagsInfo}});
  }
  var meta = frameInfo.meta;
  frameInfo.rowIndex = frameInfo.rowIndex || 0;
  var stopReading = false;
  for (var i = frameInfo.rowIndex; i < frameInfo.rowLength; i++) {
    var rowOffset = reader.offset;
    var row = new types.Row(meta.columns);
    for (var j = 0; j < meta.columns.length; j++ ) {
      var c = meta.columns[j];
      try {
        row[c.name] = this.encoder.decode(reader.readBytes(), c.type);
      }
      catch (e) {
        if (e instanceof RangeError) {
          //there is not enough data to read this row
          this.bufferResultCell(frameInfo, reader, rowOffset, i, e.missingBytes);
          stopReading = true;
          break;
        }
        throw e;
      }
    }
    if (stopReading) {
      break;
    }
    this.push({
      header: frameInfo.header,
      row: row,
      meta: frameInfo.meta,
      byRow: frameInfo.byRow,
      length: frameInfo.rowLength,
      flags: frameInfo.flagsInfo
    });
  }
};

/**
 * Sets parser options (ie: how to yield the results as they are parsed)
 * @param {Number} id Id of the stream
 * @param options
 */
Parser.prototype.setOptions = function (id, options) {
  if (this.frames[id.toString()]) {
    throw new types.DriverError('There was already state for this frame');
  }
  this.frames[id.toString()] = options;
};

/**
 * Gets the frame info from the internal state.
 * In case it is not there, it creates it.
 * In case the frame ended
 */
Parser.prototype.frameState = function (item) {
  var frameInfo = this.frames[item.header.streamId];
  if (!frameInfo) {
    frameInfo = {};
    if (!item.frameEnded) {
      //store it in the frames
      this.frames[item.header.streamId] = frameInfo;
    }
  }
  else if (item.frameEnded) {
    //if it was already stored, remove it
    delete this.frames[item.header.streamId];
  }
  frameInfo.header = item.header;
  return frameInfo;
};

/**
 * Buffers for later use as there isn't enough data to read
 * @param frameInfo
 * @param {FrameReader} reader
 * @param {Number} [originalOffset]
 * @param {Number} [rowIndex]
 * @param {Number} [missingBytes]
 */
Parser.prototype.bufferResultCell = function (frameInfo, reader, originalOffset, rowIndex, missingBytes) {
  if (!originalOffset && originalOffset !== 0) {
    originalOffset = reader.offset;
  }
  frameInfo.rowIndex = rowIndex;
  frameInfo.buffer = reader.slice(originalOffset);
  // Keep track of missing bytes in a cell, so that we can efficiently
  // accumulate chunks for it without quadratic buffer copying.
  frameInfo.missingBytes = missingBytes;
  reader.toEnd();
};

/**
 * Represents a writable streams that emits results
 */
function ResultEmitter(options) {
  Writable.call(this, options);
  /**
   * Stores the rows for frames that needs to be yielded as one result with many rows
   */
  this.rowBuffer = {};
}

util.inherits(ResultEmitter, Writable);

ResultEmitter.prototype._write = function (item, encoding, callback) {
  var error = null;
  try {
    this.each(item);
  }
  catch (err) {
    error = err;
  }
  callback(error);
};


/**
 * Analyzes the item and emit the corresponding event
 */
ResultEmitter.prototype.each = function (item) {
  if (item.error || item.result) {
    //Its either an error or an empty array rows
    //no transformation needs to be made
    return this.emit('result', item.header, item.error, item.result);
  }
  if (item.frameEnded) {
    return this.emit('frameEnded', item.header);
  }
  if (item.byRow) {
    //it should be yielded by row
    return this.emit('row', item.header, item.row, item.meta, item.length, item.flags);
  }
  if (item.row) {
    //it should be yielded as a result
    //it needs to be buffered to an array of rows
    return this.bufferAndEmit(item);
  }
  if (item.event) {
    //its an event from Cassandra
    return this.emit('nodeEvent', item.header, item.event);
  }
  //its a raw response (object with flags)
  return this.emit('result', item.header, null, item);
};

/**
 * Buffers the rows until the result set is completed and emits the result event.
 */
ResultEmitter.prototype.bufferAndEmit = function (item) {
  var rows = this.rowBuffer[item.header.streamId];
  if (!rows) {
    rows = this.rowBuffer[item.header.streamId] = [];
  }
  rows.push(item.row);
  if (rows.length === item.length) {
    this.emit('result', item.header, null, { rows: rows, meta: item.meta, flags: item.flags});
    delete this.rowBuffer[item.header.streamId];
  }
};

function ParserError(err, rowIndex, colIndex) {
  types.DriverError.call(this, err.message, this.constructor);
  this.rowIndex = rowIndex;
  this.colIndex = colIndex;
  this.innerError = err;
  this.info = 'Represents an Error while parsing the result';
}

util.inherits(ParserError, types.DriverError);

exports.Protocol = Protocol;
exports.Parser = Parser;
exports.ResultEmitter = ResultEmitter;
