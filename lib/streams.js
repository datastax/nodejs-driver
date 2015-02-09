var util = require('util');
var stream = require('stream');
var Transform = stream.Transform;
var Writable = stream.Writable;

var types = require('./types');
var utils = require('./utils');
var FrameHeader = types.FrameHeader;
var FrameReader = require('./readers').FrameReader;

/**
 * Transforms chunks, emits data objects {header, chunk}
 */
function Protocol (options) {
  Transform.call(this, options);
  this.header = null;
  this.headerChunks = [];
  this.bodyLength = 0;
}

util.inherits(Protocol, Transform);

Protocol.prototype._transform = function (chunk, encoding, callback) {
  var error = null;
  try {
    this.transformChunk(chunk);
  }
  catch (err) {
    error = err;
  }
  callback(error);
};

Protocol.prototype.transformChunk = function (chunk) {
  var bodyChunk = chunk;

  if (this.header === null) {
    this.headerChunks.push(chunk);
    var length = utils.totalLength(this.headerChunks);
    if (length < FrameHeader.size) {
      return;
    }
    var chunksGrouped = Buffer.concat(this.headerChunks, length);
    this.header = new FrameHeader(chunksGrouped);
    if (length >= FrameHeader.size) {
      bodyChunk = chunksGrouped.slice(FrameHeader.size);
    }
  }

  this.bodyLength += bodyChunk.length;
  var frameEnded = this.bodyLength >= this.header.bodyLength;
  var header = this.header;

  var nextChunk = null;

  if (this.bodyLength > this.header.bodyLength) {
    //We received more than a complete frame
    var previousBodyLength = (this.bodyLength - bodyChunk.length);

    var nextStart = this.header.bodyLength - previousBodyLength;
    if (nextStart > bodyChunk.length) {
      throw new Error('Tried to slice a received chunk outside boundaries');
    }
    nextChunk = bodyChunk.slice(nextStart);
    bodyChunk = bodyChunk.slice(0, nextStart);
    this.clear();

    //close loop: parse next chunk before emitting
    this.transformChunk(nextChunk);
  }
  else if (this.bodyLength === this.header.bodyLength) {
    this.clear();
  }

  this.push({header: header, chunk: bodyChunk, frameEnded: frameEnded});
};

Protocol.prototype.clear = function () {
  this.header = null;
  this.bodyLength = 0;
  this.headerChunks = [];
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
    this.emitItem(frameInfo, {frameEnded: true});
  }
};

/**
 * Pushes the item with the header and the provided props to the consumer
 */
Parser.prototype.emitItem = function (frameInfo, props) {
  //push the header and props to be read by consumers
  this.push(utils.extend({header: frameInfo.header}, props));
};

Parser.prototype.parseBody = function (frameInfo, item) {
  if (frameInfo.missingBytes) {
    // Avoid quadratic buffer copying by accumulating chunks for a large cell
    // value in an array until enough bytes are available to read past the
    // cell value.
    if (frameInfo.buffers) {
      frameInfo.buffers.push(item.chunk);
    } else if (frameInfo.buffer) {
        frameInfo.buffers = [frameInfo.buffer, item.chunk];
    } else {
        frameInfo.buffers = [item.chunk];
    }

    if (item.chunk.length < frameInfo.missingBytes) {
      frameInfo.missingBytes -= item.chunk.length;
      // Don't continue parsing until we have collected enough bytes in the
      // buffer.
      return;
    } else {
      // Now we have enough bytes in the buffers. Concat all chunks into a
      // single buffer & proceed with normal parsing.
      item.chunk = Buffer.concat(frameInfo.buffers);
      frameInfo.missingBytes = null;
      frameInfo.buffer = null;
      frameInfo.buffers = null;
    }
  }

  var reader = new FrameReader(item.header, item.chunk);
  if (frameInfo.buffer) {
    reader.unshift(frameInfo.buffer);
    frameInfo.buffer = null;
  }
  switch (item.header.opcode) {
    case types.opcodes.result:
      return this.parseResult(frameInfo, reader);
    case types.opcodes.ready:
    case types.opcodes.authSuccess:
      return this.emitItem(frameInfo, {ready: true});
    case types.opcodes.authChallenge:
      return this.parseAuthChallenge(frameInfo, reader);
    case types.opcodes.authenticate:
      return this.emitItem(frameInfo, {mustAuthenticate: true});
    case types.opcodes.error:
      return this.parseError(frameInfo, reader);
    case types.opcodes.supported:
      return this.emitItem(frameInfo);
    case types.opcodes.event:
      return this.parseEvent(frameInfo, reader);
    default:
      return this.emitItem(frameInfo, {error: new Error('Received invalid opcode: ' + item.header.opcode)});
  }
};

/**
 * Tries to read the error code and message.
 * If there is enough data to read, it pushes the header and error. If there isn't, it buffers it.
 * @param frameInfo information of the frame being parsed
 * @param {FrameReader} reader
 */
Parser.prototype.parseError = function (frameInfo, reader) {
  try {
    this.emitItem(frameInfo, {error: reader.readError()});
  }
  catch (e) {
    if (e instanceof RangeError) {
      frameInfo.buffer = reader.getBuffer();
      return;
    }
    throw e;
  }
};

/**
 * Tries to read the event from Cassandra
 * If there is enough data to read, it pushes the header and body. If there isn't, it buffers it.
 * @param frameInfo information of the frame being parsed
 * @param {FrameReader} reader
 */
Parser.prototype.parseEvent = function (frameInfo, reader) {
  try {
    this.emitItem(frameInfo, {event: reader.readEvent()});
  }
  catch (e) {
    if (e instanceof RangeError) {
      frameInfo.buffer = reader.getBuffer();
      return;
    }
    throw e;
  }
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
      frameInfo.kind = reader.readInt();

      if (frameInfo.kind === types.resultKind.prepared) {
        frameInfo.preparedId = utils.copyBuffer(reader.readShortBytes());
      }
      else if (frameInfo.kind === types.resultKind.setKeyspace) {
        frameInfo.keyspace = reader.readString();
      }
      if (frameInfo.kind === types.resultKind.rows ||
          frameInfo.kind === types.resultKind.prepared) {
        frameInfo.meta = reader.readMetadata();
      }
    }
  }
  catch (e) {
    if (e instanceof RangeError) {
      //A controlled error, the kind / metadata is not available to be read yet
      return this.bufferForLater(frameInfo, reader, originalOffset);
    }
    throw e;
  }
  if (frameInfo.kind === types.resultKind.setKeyspace) {
    return this.emitItem(frameInfo, {keyspaceSet: frameInfo.keyspace});
  }
  if (frameInfo.kind === types.resultKind.schemaChange) {
    return this.emitItem(frameInfo, {schemaChange: true});
  }
  if (frameInfo.kind !== types.resultKind.rows) {
    return this.emitItem(frameInfo, {id: frameInfo.preparedId, meta: frameInfo.meta});
  }
  if (frameInfo.streamField) {
    frameInfo.streamingColumn = frameInfo.meta.columns[frameInfo.meta.columns.length-1].name;
  }
  //it contains rows
  if (reader.remainingLength() > 0) {
    this.parseRows(frameInfo, reader);
  }
};

Parser.prototype.parseRows = function (frameInfo, reader) {
  if (typeof frameInfo.rowLength === 'undefined') {
    try {
      frameInfo.rowLength = reader.readInt();
    }
    catch (e) {
      if (e instanceof RangeError) {
        //there is not enough data to read this row
        this.bufferForLater(frameInfo, reader);
        return;
      }
      throw e;
    }
  }
  if (frameInfo.rowLength === 0) {
    return this.emitItem(frameInfo, {result: {rows: []}});
  }
  var meta = frameInfo.meta;
  frameInfo.rowIndex = frameInfo.rowIndex || 0;
  var stopReading = false;
  for (var i = frameInfo.rowIndex; i < frameInfo.rowLength && !stopReading; i++) {
    this.emit('log', 'info', 'Reading row ' + i);
    if (frameInfo.fieldStream) {
      this.streamField(frameInfo, reader, null, i);
      stopReading = reader.remainingLength() === 0;
      continue;
    }
    var row = new types.Row(meta.columns);
    var rowOffset = reader.offset;
    for(var j = 0; j < meta.columns.length; j++ ) {
      var col = meta.columns[j];
      this.emit('log', 'info', 'Reading cell value for ' + col.name);
      if (col.name !== frameInfo.streamingColumn) {
        var bytes = null;
        try {
          bytes = reader.readBytes();
        }
        catch (e) {
          if (e instanceof RangeError) {
            //there is not enough data to read this row
            this.bufferForLater(frameInfo, reader, rowOffset, i, e.missingBytes);
            stopReading = true;
            break;
          }
          throw e;
        }
        try
        {
          row[col.name] = this.encoder.decode(bytes, col.type);
          bytes = null;
        }
        catch (e) {
          throw new ParserError(e, i, j);
        }
        if (j === meta.columns.length -1) {
          //the is no field to stream, emit that the row has been parsed
          this.emitItem(frameInfo, {
            row: row,
            meta: frameInfo.meta,
            byRow: frameInfo.byRow,
            length: frameInfo.rowLength
          });
        }
      }
      else {
        var couldRead = this.streamField(frameInfo, reader, row, i);
        if (couldRead && reader.remainingLength() > 0) {
          //could be next field/row
          continue;
        }
        if (!couldRead) {
          this.bufferForLater(frameInfo, reader, rowOffset, frameInfo.rowIndex);
        }
        stopReading = true;
      }
    }
  }
};

/**
 * @param frameInfo
 * @param {FrameReader} reader
 */
Parser.prototype.parseAuthChallenge = function (frameInfo, reader) {
  try {
    //receives a token (can be empty or null)
    this.emitItem(frameInfo, {authChallenge: true, token: reader.readBytes()});
  }
  catch (e) {
    if (e instanceof RangeError) {
      frameInfo.buffer = reader.getBuffer();
      return;
    }
    throw e;
  }
};

/**
 * Streams the content of a field
 * @returns {Boolean} true if read from the reader
 */
Parser.prototype.streamField = function (frameInfo, reader, row, rowIndex) {
  this.emit('log', 'info', 'Streaming field');
  var fieldStream = frameInfo.fieldStream;
  if (!fieldStream) {
    try {
      frameInfo.fieldLength = reader.readInt();
    }
    catch (e) {
      if (e instanceof RangeError) {
        return false;
      }
      throw e;
    }
    if (frameInfo.fieldLength < 0) {
      //null value
      this.emitItem(frameInfo, {
        row: row,
        meta: frameInfo.meta,
        byRow: true,
        length: frameInfo.rowLength
      });
      return true;
    }
    fieldStream = new types.ResultStream();
    frameInfo.streamedSoFar = 0;
    frameInfo.rowIndex = rowIndex;
    frameInfo.fieldStream = fieldStream;
    this.emitItem(frameInfo, {
      row: row,
      meta: frameInfo.meta,
      fieldStream: fieldStream,
      byRow: true,
      length: frameInfo.rowLength
    });
  }
  var availableChunk = reader.read(frameInfo.fieldLength - frameInfo.streamedSoFar);

  //push into the stream
  fieldStream.add(availableChunk);
  frameInfo.streamedSoFar += availableChunk.length;
  //check if finishing
  if (frameInfo.streamedSoFar === frameInfo.fieldLength) {
    //EOF - Finished streaming this
    fieldStream.push(null);
    frameInfo.fieldStream = null;
  }
  return true;
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
    frameInfo = this.frames[item.header.streamId] = {};
  }
  if (item.frameEnded) {
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
Parser.prototype.bufferForLater = function (frameInfo, reader, originalOffset, rowIndex, missingBytes) {
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
    //no transformation needs to be made
    return this.emit('result', item.header, item.error, item.result);
  }
  if (item.frameEnded) {
    return this.emit('frameEnded', item.header);
  }
  if (item.byRow) {
    //it should be yielded by row
    return this.emit('row', item.header, item.row, item.meta, item.fieldStream, item.length);
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
  //its a raw result (object with flags)
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
    this.emit('result', item.header, null, {rows: rows, meta: item.meta});
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
