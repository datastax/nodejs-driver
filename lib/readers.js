/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js 
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils.js');
var types = require('./types.js');
var Int64 = require('node-int64');
var events = require('events');
var Transform = require('stream').Transform;

function FrameHeaderReader(buf) {
  if (buf.length < FrameHeaderReader.size) {
    //there is not enough data to read the header
    return;
  }
  this.bufferLength = buf.length;
  this.isResponse = buf[0] & 0x80;
  this.version = buf[0] & 0x7F;
  this.flags = buf.readUInt8(1);
  this.streamId = buf.readInt8(2);
  this.opcode = buf.readUInt8(3);
  this.bodyLength = buf.readUInt32BE(4);
  this.frameLength = this.bodyLength + FrameHeaderReader.size;
}
/**
 * Determines that the frame starts and ends in the buffer
 */
FrameHeaderReader.prototype.isComplete = function() {
  return (this.bufferLength >= this.frameLength) && 
    this.isFrameStart();
}
/**
 * Determines that the frame contains the frame header in the buffer
 */
FrameHeaderReader.prototype.isFrameStart = function() {
  return ((this.isResponse) &&
    (this.version === 1) &&
    (this.flags < 0x04) &&
    (this.opcode <= types.opcodes.maxCode));
}
/**
 * Determines that buffer does not have the necessary amount of data to read the frame header
 */
FrameHeaderReader.prototype.isIncompleteHeader = function() {
  return this.bufferLength > 0 ? false : true;
}
/**
 * The length of the header of the protocol
 */
FrameHeaderReader.size = 8;
/*
 * Identify the beginning of a response message (CQL3 V1 only)
 */
FrameHeaderReader.startMessageByte = 0x81;

/**
 * Buffer forward reader of CQL binary frames
 */
function FrameReader(header, body) {
  this.header = header;
  this.opcode = header.opcode;
  this.offset = 0;
  this.buf = body;
}

FrameReader.prototype.readInt = function() {
  var result = this.buf.readInt32BE(this.offset);
  this.offset += 4;
  return result;
}

FrameReader.prototype.readShort = function() {
  var result = this.buf.readUInt16BE(this.offset);
  this.offset += 2;
  return result;
}

FrameReader.prototype.readByte = function() {
  var result = this.buf.readUInt8(this.offset);
  this.offset += 1;
  return result;
}

FrameReader.prototype.readString = function() {
  var length = this.readShort();
  var result = this.buf.toString('utf8', this.offset, this.offset+length);
  this.offset += length;
  return result;
}

FrameReader.prototype.readStringL = function () {
  var length = this.readInt();
  var result = this.buf.toString('utf8', this.offset, length);
  this.offset += length;
  return result;
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
  var num = this.readInt();
  if(num < 0) {
    return null;
  }
  var bytes = this.buf.slice(this.offset, this.offset+num);

  this.offset += num;

  return bytes;
}

FrameReader.prototype.readShortBytes = function() {
  var num = this.readShort();
  if(num < 0) {
    return null;
  }
  var bytes = this.buf.slice(this.offset, this.offset+num);

  this.offset += num;

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

    spec.column_name = this.readString();
    spec.type = this.readOption();
    meta.columns.push(spec);
    //Store the column index by name, to be able to find the column by name
    meta.columns['_col_' + spec.column_name] = i;
  }

  return meta;
}

FrameReader.prototype.parseResult = function () {
  var kind = this.readInt();

  switch (kind) {
    case 1: //Void
      return null;
    case 2: //Rows
      return this.parseRows();
    case 3: //Set_keyspace
      return this.readString();
    case 4: //Prepared
      return {
        id: utils.copyBuffer(this.readShortBytes()),
        meta: this.readMetadata()
      };
    case 5: //Schema_change
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
    if (length < FrameHeaderReader.size) {
      return;
    }
    var chunksGrouped = Buffer.concat(this.headerChunks, length);
    this.header = new FrameHeaderReader(chunksGrouped);
    if (length >= FrameHeaderReader.size) {
      bodyChunk = chunksGrouped.slice(FrameHeaderReader.size);
    }
  }
  
  this.bodyLength += bodyChunk.length;
  var finishedFrame = this.bodyLength >= this.header.bodyLength;
  var header = this.header;
  
  var nextChunk = null;
  
  //console.log('-- body vs expected', this.bodyLength, this.header.bodyLength, chunk.length);
  if (this.bodyLength > this.header.bodyLength) {
    //We received more than a complete frame
    //calculate where the previous frame ends and the new starts
                  //(expected body length) - (previous bodyLength)
    //var nextStart = this.header.bodyLength - (this.bodyLength - chunk.length);
    var nextStart = this.header.bodyLength;
    
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
 * Gets
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
    console.log('--Received complete frame #' + frameKey, 'emitting...', frame.header.bodyLength, utils.totalLength(frame.chunks), frame.header.bufferLength);
    this.push({header: frame.header, body: body});
  }
  else {
    console.log('--Received partial frame #' + frameKey);
  }
}

/**
 * Represents a handler that parses an empty or error response
 */
function EmptyResponseHandler(callback) {
  this.callback = callback;
}

EmptyResponseHandler.prototype.handle = function(data) {
  var reader = new FrameReader(data.header, data.body);
  data = null;
  if(reader.opcode === types.opcodes.ready){
    reader = null;
    this.callback();
  }
  else if(reader.opcode === types.opcodes.error) {
    this.callback(reader.readError());
    reader = null;
  }
  else {
    throw new Error('Opcode not handled: ' + reader.opcode);
  }
}

/**
 * Represents a handler that parses all types of responses
 */
function ResponseHandler(callback, authCallback) {
  this.callback = callback;
  this.authCallback = authCallback;
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
  else if(reader.opcode === types.opcodes.result ||
          reader.opcode === types.opcodes.prepare) {
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

exports.FrameReader = FrameReader;
exports.ResponseHandler = ResponseHandler;
exports.EmptyResponseHandler = EmptyResponseHandler
exports.readEvent = readEvent;
exports.FrameHeaderReader = FrameHeaderReader;

exports.ProtocolParser = ProtocolParser;
exports.FrameParser = FrameParser;