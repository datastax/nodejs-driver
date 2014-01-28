var util = require('util');
var stream = require('stream');
var Transform = stream.Transform;
var Writable = stream.Writable;

var encoder = require('./encoder.js');
var types = require('./types.js');
var utils = require('./utils.js');
var FrameHeader = types.FrameHeader;
var FrameReader = require('./readers.js').FrameReader;

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
 */
function Parser (options) {
  Transform.call(this, options);
  //frames that are streaming, indexed by id
  this.frames = {};
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
  //flag that determines if it needs push down the header and props to the consumer
  var pushDown = true;
  if (frameInfo.resultStream) {
    //emit rows into the specified stream
    if (props.row) {
      frameInfo.resultStream.add(props.row);
      pushDown = false;
    }
    else if (props.frameEnded) {
      frameInfo.resultStream.add(null);
    }
    else if (props.error) {
      //Cassandra sent a response error
      frameInfo.resultStream.emit('error', props.error)
    }
  }
  if (pushDown) {
    //push the header and props to be read by consumers
    this.push(utils.extend({header: frameInfo.header}, props));
  }

};

Parser.prototype.parseBody = function (frameInfo, item) {
  var reader = new FrameReader(item.header, item.chunk);
  if (frameInfo.buffer) {
    reader.unshift(frameInfo.buffer);
    frameInfo.buffer = null;
  }
  switch (item.header.opcode) {
    case types.opcodes.ready:
    case types.opcodes.auth_success:
      return this.emitItem(frameInfo, {ready: true});
    case types.opcodes.authenticate:
      return this.emitItem(frameInfo, {mustAuthenticate: true});
    case types.opcodes.error:
      return this.parseError(frameInfo, reader);
    case types.opcodes.result:
      return this.parseResult(frameInfo, reader);
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
            this.bufferForLater(frameInfo, reader, rowOffset, i);
            stopReading = true;
            break;
          }
          throw e;
        }
        try
        {
          row[col.name] = encoder.decode(bytes, col.type);
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
 */
Parser.prototype.bufferForLater = function (frameInfo, reader, originalOffset, rowIndex) {
  if (!originalOffset && originalOffset !== 0) {
    originalOffset = reader.offset;
  }
  frameInfo.rowIndex = rowIndex;
  frameInfo.buffer = reader.slice(originalOffset);
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
    return this.emit('row', item.header, item.row, item.fieldStream, item.length);
  }
  if (item.row) {
    //it should be yielded as a result
    //it needs to be buffered to an array of rows
    return this.bufferAndEmit(item);
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