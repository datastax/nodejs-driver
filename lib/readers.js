/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js 
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils.js');
var types = require('./types.js');
var Int64 = require('node-int64');
var events = require('events');
var stream = require('stream')
var Transform = stream.Transform;
var Writable = stream.Writable;
var FrameHeader = types.FrameHeader;

/**
 * Buffer forward reader of CQL binary frames
 */
function FrameReader(header, body) {
  this.header = header;
  this.opcode = header.opcode;
  this.offset = 0;
  this.buf = body;
}

FrameReader.prototype.remainingLength = function () {
  return this.buf.length - this.offset;
}

/**
 * Slices the underlining buffer
 */
FrameReader.prototype.slice = function (begin, end) {
  if (typeof end === 'undefined') {
    end = this.buf.length;
  }
  return this.buf.slice(begin, end);
}

/**
 * Reads any number of bytes and moves the offset.
 * if length not provided or it's larger than the remaining bytes, reads to end.
 */
FrameReader.prototype.read = function (length) {
  var end = this.buf.length;
  if (typeof length !== 'undefined' && this.offset + length < this.buf.length) {
    end = this.offset + length;
  }
  var bytes = this.slice(this.offset, end);
  this.offset = end;
  return bytes;
}

FrameReader.prototype.readInt = function() {
  var result = this.buf.readInt32BE(this.offset);
  this.offset += 4;
  return result;
}

FrameReader.prototype.readShort = function () {
  var result = this.buf.readUInt16BE(this.offset);
  this.offset += 2;
  return result;
}

FrameReader.prototype.readByte = function () {
  var result = this.buf.readUInt8(this.offset);
  this.offset += 1;
  return result;
}

FrameReader.prototype.readString = function () {
  var length = this.readShort();
  this.checkOffset(length);
  var result = this.buf.toString('utf8', this.offset, this.offset+length);
  this.offset += length;
  return result;
}

/**
 * Checks that the new length to read is within the range of the buffer length. Throws a RangeError if not.
 */
FrameReader.prototype.checkOffset = function (newLength) {
  if (this.offset + newLength > this.buf.length) {
    throw new RangeError('Trying to access beyond buffer length');
  }
}

FrameReader.prototype.readUUID = function () {
  var octets = [];
  for(var i = 0; i < 16; i++) {
      octets.push(this.readByte());
  }

  var str = "";

  octets.forEach(function(octet) {
      str += octet.toString(16);
  });

  return str.slice(0, 8) + '-' + str.slice(8, 12) + '-' + str.slice(12, 16) + '-' + str.slice(16, 20) + '-' + str.slice(20);
}

FrameReader.prototype.readStringList = function () {
  var num = this.readShort();

  var list = [];

  for(var i = 0; i < num; i++) {
      list.push(this.readString());
  }

  return list;
}
/**
 * Reads the amount of bytes that the field has and returns them (slicing them).
 */
FrameReader.prototype.readBytes = function() {
  var length = this.readInt();
  if(length < 0) {
    return null;
  }
  this.checkOffset(length);
  var bytes = this.read(length);

  return bytes;
}

FrameReader.prototype.readShortBytes = function() {
  var length = this.readShort();
  if(length < 0) {
    return null;
  }
  var bytes = this.read(length);

  return bytes;
}

/* returns an array with two elements */
FrameReader.prototype.readOption = function () {
  var id = this.readShort();

  switch(id) {
    case 0x0000: 
        return [id, this.readString()];
    case 0x0001:
    case 0x0002:
    case 0x0003:
    case 0x0004:
    case 0x0005:
    case 0x0006:
    case 0x0007:
    case 0x0008:
    case 0x0009:
    case 0x000A:
    case 0x000B:
    case 0x000C:
    case 0x000D:
    case 0x000E:
    case 0x000F:
    case 0x0010:
        return [id, null];
    case 0x0020:
        return [id, this.readOption()];
    case 0x0021:
        return [id, [this.readOption(), this.readOption()]];
    case 0x0022:
        return [id, this.readOption()];
  }

  return [id, null];
}

/* returns an array of arrays */
FrameReader.prototype.readOptionList = function () {
  var num = this.readShort();
  var options = [];
  for(var i = 0; i < num; i++) {
      options.push(this.readOption());
  }
  return options;
}

FrameReader.prototype.readInet = function () {
  //TODO
}

FrameReader.prototype.readStringMap = function () {
  var num = this.readShort();
  var map = {};
  for(var i = 0; i < num; i++) {
      var key = this.readString();
      var value = this.readString();
      map[key] = value;
  }
  return map;
}

FrameReader.prototype.readStringMultimap = function () {
  var num = this.readShort();
  var map = {};
  for(var i = 0; i < num; i++) {
      var key = this.readString();
      var value = this.readStringList();
      map[key] = value;
  }
  return map;
}

FrameReader.prototype.readMetadata = function() {
  var meta = {};
  //as used in Rows and Prepared responses
  var flags = this.readInt();

  var columnCount = this.readInt();

  if(flags & 0x0001) { //global_tables_spec
      meta.global_tables_spec = true;
      meta.keyspace = this.readString();
      meta.table = this.readString();
  }

  meta.columns = [];

  for(var i = 0; i < columnCount; i++) {
    var spec = {};
    if(!meta.global_tables_spec) {
        spec.ksname = this.readString();
        spec.tablename = this.readString();
    }

    spec.name = this.readString();
    spec.type = this.readOption();
    meta.columns.push(spec);
    //Store the column index by name, to be able to find the column by name
    meta.columns['_col_' + spec.name] = i;
  }

  return meta;
}

/**
 * Parses the frame for streaming
 */
FrameReader.prototype.tryReadMetadata = function () {
  var kind = this.readInt();
  if (kind !== types.resultKind.rows) {
    throw new Error('Can not stream this kind of result: ' + kind);
  }
  //minimum length = 4 bytes for: flags and columnCount
  var minimumLength = 4 * 2;
  if (this.remainingLength() < minimumLength) {
    return null;
  }
  var meta = null;
  try {
    meta = this.readMetadata();
  }
  catch (e) {
    if (e instanceof RangeError) {
      //A controlled error, the metadata is not available to be read yet
    }
    else {
      throw e;
    }
  }
  
  return meta;
}

/**
 * Modifies the underlying buffer, it concatenates the given buffer with the original (internalBuffer = concat(bytes, internalBuffer)
 */
FrameReader.prototype.unshift = function (bytes) {
  if (this.offset > 0) {
    throw new Error('Can not modify the underlying buffer if already read');
  }
  this.buf = Buffer.concat([bytes, this.buf], bytes.length + this.buf.length);
}

FrameReader.prototype.parseResult = function () {
  var kind = this.readInt();

  switch (kind) {
    case types.resultKind.voidResult:
      return null;
    case types.resultKind.rows:
      return this.parseRows();
    case types.resultKind.setKeyspace:
      return this.readString();
    case types.resultKind.prepared:
      return {
        id: utils.copyBuffer(this.readShortBytes()),
        meta: this.readMetadata()
      };
    case types.resultKind.schemaChange:
      return {
        change: this.readString(),
        keyspace: this.readString(),
        table: this.readString()
      };
  }

  throw new Error('Unkown RESPONSE type: ' + kind + ' header: ' + util.inspect(this.header) + ';body: ' + this.buf.toString('hex'));
}

FrameReader.prototype.parseRows = function () {
  var meta = this.readMetadata();
  var rowCount = this.readInt();
  var rows = [];
  for(var i = 0; i < rowCount; i++) {
    var row = [];
    
    for(var col = 0; col < meta.columns.length; col++ ) {
      var spec = meta.columns[col];
      var cellValue = null;
      try {
        var bytes = this.readBytes();
        cellValue = types.typeEncoder.decode(bytes, spec.type);
        bytes = null;
      }
      catch (e) {
        throw new ParserError(e, i, col);
      }
      row[col] = cellValue;
      cellValue = null;
    }
    
    row.columns = meta.columns;
    row.get = getCellValueByName.bind(row);
    rows.push(row);
  }

  return {
    meta: meta,
    rows: rows
  };
}

function getCellValueByName(name) {
  var cellIndex = name;
  if (typeof cellIndex === 'string') {
    cellIndex = this.columns['_col_' + name];
  }
  return this[cellIndex];
}

FrameReader.prototype.readError = function () {
  var code = this.readInt();
  var message = this.readString();
  //determine if the server is unhealthy
  //if true, the client should not retry for a while
  var isServerUnhealthy = false;
  switch (code) {
    case types.responseErrorCodes.serverError:
    case types.responseErrorCodes.overloaded:
    case types.responseErrorCodes.isBootstrapping:
      isServerUnhealthy = true;
      break;
  }
  return new ResponseError(code, message, isServerUnhealthy);
}

/**
 * Transforms chunks, emits data objects {header, chunk}
 */
function ProtocolParser (options) {
  Transform.call(this, options);
  this.header = null;
  this.headerChunks = [];
  this.bodyLength = 0;
}

util.inherits(ProtocolParser, Transform);

ProtocolParser.prototype._transform = function (chunk, encoding, callback) {
  var error = null;
  try {
    this.transformChunk(chunk);
  }
  catch (err) {
    error = err;
  }
  callback(error);
}

ProtocolParser.prototype.transformChunk = function (chunk) {
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
  var finishedFrame = this.bodyLength >= this.header.bodyLength;
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
    
  this.push({header: header, chunk: bodyChunk, finishedFrame: finishedFrame});
}

ProtocolParser.prototype.clear = function () {
  this.header = null;
  this.bodyLength = 0;
  this.headerChunks = [];
}

/**
 * A stream that transforms partial frames into frames
 */
function FrameParser (options) {
  Transform.call(this, options);
  this.frames = {};
}

util.inherits(FrameParser, Transform);

FrameParser.prototype._transform = function (item, encoding, callback) {
  var error = null;
  try {
    this.transformPartialFrame(item);
  }
  catch (err) {
    error = err;
  }
  callback(error);
  if (item && item.finishedFrame) {
    //emit that all possible parsing for this streamId has finished
    this.emit('parsingFinished', item.header.streamId);
  }
}

FrameParser.prototype.transformPartialFrame = function (item) {
  var frameKey = item.header.streamId.toString();
  var frame = this.frames[frameKey];
  if (!frame) {
    frame = this.frames[frameKey] = {header: item.header, chunks: []};
  }
  frame.chunks.push(item.chunk);
  if (item.finishedFrame) {
    delete this.frames[frameKey];
    var body = Buffer.concat(frame.chunks, frame.header.bodyLength);
    this.push({header: frame.header, body: body});
  }
}

/**
 * A stream that gets partial frames and emits events for streaming
 */
function FrameStreamingParser (options) {
  Writable.call(this, options);
  //frames that are streaming, indexed by id
  this.frames = {};
}
util.inherits(FrameStreamingParser, Writable);

FrameStreamingParser.prototype._write = function (item, encoding, callback) {
  var error = null;
  try {
    this.streamFrame(item);
  }
  catch (err) {
    console.error(err);
    error = err;
  }
  callback(error);
  if (item && item.finishedFrame) {
    //emit that all possible parsing for this streamId has finished
    this.emit('parsingFinished', item.header.streamId);
  }
}

FrameStreamingParser.prototype.streamFrame = function (item) {
  var frameInfo = this.frames[item.header.streamId];
  //check if parse / stream
  if (!frameInfo) {
    return;
  }
  console.log('--FrameStreamingParser', '#' + item.header.streamId, item.chunk.length);
  var reader = null;
  if (!frameInfo.metadata) {
    if (!frameInfo.header) {
      frameInfo.header = item.header;
    }
    var buffer = item.chunk;
    if (frameInfo.buffer) {
      buffer = Buffer.concat([frameInfo.buffer, item.chunk], frameInfo.buffer.length + item.chunk.length);
    }
    reader = new FrameReader(frameInfo.header, buffer);
    frameInfo.metadata = reader.tryReadMetadata();
    if (!frameInfo.metadata) {
      //buffer until there is enough data to stream
      frameInfo.buffer = buffer;
      return;
    }
    frameInfo.buffer = null;
    console.log('--frameInfo metadata', frameInfo.metadata);
    frameInfo.streamingColumn = frameInfo.metadata.columns[frameInfo.metadata.columns.length-1].name;
  }
  
  if (reader === null) {
    reader = new FrameReader(item.header, item.chunk);
  }
  if (reader.remainingLength() > 0) {
    this.streamRows(frameInfo, reader);
  }
  
  //TODO: clearStreamId setIgnore for the other writeable stream
  //??: when to free the streamId to connection
}

FrameStreamingParser.prototype.streamRows = function (frameInfo, reader) {
  //TODO: emit events
    //- rowStartedStreaming: When the it finished parsing the initial fields
    //rowFinishedStreaming: When it finished streaming the fields in the row
    //finishedStreaming: When all the frame has been received and there will be no more streaming
  if (!frameInfo.rowLength) {
    try {
      frameInfo.rowLength = reader.readInt();
    }
    catch (e) {
      if (e instanceof RangeError) {
        //there is not enough data to read this row
        bufferForLater(reader.offset, null);
        return;
      }
      throw e;
    }
    if (frameInfo.rowLength === 0) {
      //TODO: check
      this.emit('finishedStreaming');
      return;
    }
  }
  //Could be
    //A: Already streaming a field
    //B: -Not enough data to start streaming the row
    //C: -Not started streaming this row
  if (frameInfo.buffer) {
    reader.unshift(frameInfo.buffer);
    frameInfo.buffer = null;
  }
  var meta = frameInfo.metadata;
  frameInfo.rowIndex = frameInfo.rowIndex ? frameInfo.rowIndex : 0;
  var stopReading = false;
  for (var i = frameInfo.rowIndex; i < frameInfo.rowLength && stopReading; i++) {
    row = [];
    row.columns = meta.columns;
    row.get = getCellValueByName.bind(row);
    var rowOffset = reader.offset;
    for(var j = 0; j < meta.columns.length; j++ ) {
      var col = meta.columns[j];
      if (col.name !== frameInfo.streamingColumn) {
        var bytes = null;
        try {
          bytes = reader.readBytes();
        }
        catch (e) {
          if (e instanceof RangeError) {
            //there is not enough data to read this row
            bufferForLater(rowOffset, i);
            stopReading = true;
            break;
          }
          throw e;
        }
        
        try
        {
          row[j] = types.typeEncoder.decode(bytes, col.type);
          bytes = null;
        }
        catch (e) {
          throw new ParserError(e, i, j);
        }
      }
      else {
        //row start streaming
        var fieldLength = 0;
        try {
          fieldLength = reader.readInt();
        }
        catch (e) {
          if (e instanceof RangeError) {
            bufferForLater(rowOffset, i);
            stopReading = true;
            break;
          }
        }
        var fieldStream = new stream.Readable();
        this.emit('rowStartedStreaming', row, fieldStream);
        var availableChunk = reader.read(fieldLength);
        fieldStream.push(availableChunk);
        //check if finishing: TODO: check previous
        if (availableChunk.length < fieldLength) {
          //store for later
        }
        else {
          //EOF
          fieldStream.push(null);
        }
      }
    }
    
    //TODO: emit
  }
  
  /**
   * Buffers for later use as there isn't enough data to read
   */
  function bufferForLater (originalOffset, rowIndex) {
    frameInfo.rowIndex = rowIndex;
    frameInfo.buffer = reader.slice(originalOffset);
  }
}

FrameStreamingParser.prototype.setStreaming = function (id) {
  this.frames[id.toString()] = {};
}

FrameStreamingParser.prototype.clearStreaming = function (id) {
  delete this.frames[id.toString()];
}

/**
 * Represents a handler that parses all types of responses
 */
function ResponseHandler(callback, authCallback, noBody) {
  this.callback = callback;
  this.authCallback = authCallback;
  this.noBody = noBody;
}

ResponseHandler.prototype.handle = function(data) {
  var reader = new FrameReader(data.header, data.body);
  data = null;
  if(reader.opcode === types.opcodes.ready) {
    reader = null;
    this.callback();
  }
  else if (reader.opcode === types.opcodes.error) {
    this.callback(reader.readError());
    reader = null;
  }
  else if(!this.noBody && (reader.opcode === types.opcodes.result ||
          reader.opcode === types.opcodes.prepare)) {
    var result = null;
    var err = null;
    try {
      result = reader.parseResult();
    }
    catch (e) {
      err = e;
    }
    reader = null;
    this.callback(err, result);
    result = null;
  }
  else if (reader.opcode === types.opcodes.authenticate) {
    reader = null;
    this.authCallback(this.callback);
  }
  else {
    throw new Error('Opcode not handled: ' + reader.opcode);
  }
}


function readEvent(data, emitter) {
  var reader = new FrameReader(data);
  var event = reader.readString();
  console.log('emitting ' + event);
  if(event == 'TOPOLOGY_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readInet());
  }
  else if (event == 'STATUS_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readInet());
  }
  else if (event == 'SCHEMA_CHANGE') {
    emitter.emit(event, reader.readString(), reader.readString(), reader.readString());
  }
  else {
    throw new Error('Unknown EVENT type: ' + event);
  }
}

function ResponseError(code, message, isServerUnhealthy) {
  ResponseError.super_.call(this, message, this.constructor);
  this.code = code;
  this.isServerUnhealthy = isServerUnhealthy;
  this.info = 'Represents a error message from the server';
}
util.inherits(ResponseError, types.DriverError);

function ParserError(err, rowIndex, colIndex) {
  ParserError.super_.call(this, err.message, this.constructor);
  this.rowIndex = rowIndex;
  this.colIndex = colIndex;
  this.innerError = err;
  this.info = 'Represents an Error while parsing the result';
}
util.inherits(ParserError, types.DriverError);

exports.ResponseHandler = ResponseHandler;
exports.readEvent = readEvent;
exports.ProtocolParser = ProtocolParser;
exports.FrameParser = FrameParser;
exports.FrameStreamingParser = FrameStreamingParser;