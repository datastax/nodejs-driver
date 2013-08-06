/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js 
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils.js');
var types = require('./types.js');
var Int64 = require('node-int64');
var events = require('events');

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
 * Buffer forwar reader of CQL binary frames
 */
function FrameReader(buf, header) {
  if (!header) {
    header = new FrameHeaderReader(buf);
  }
  this.header = header;
  this.opcode = header.opcode;
  this.offset = FrameHeaderReader.size;
  this.buf = buf;
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

  throw new Error('Unkown RESPONSE type: ' + kind);
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
        cellValue = utils.typeEncoder.decode(bytes, spec.type);
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
 * Buffers the chunks and emits an event when there is a complete frame
 */
function PartialReader (stream) {
  PartialReader.super_.call(this);
  this.partialStreams = [];
  var self = this;
  stream.on('readable', function (){
    var buf = null;
    var data = [];
    var totalLength = 0;
    while ((buf = stream.read()) !== null) {
      data.push(buf);
      totalLength += buf.length;
    }
    if (totalLength > 0) {
      self.readChunk(Buffer.concat(data, totalLength));
    }
  });
}

util.inherits(PartialReader, events.EventEmitter);

PartialReader.prototype.readChunk = function readChunk (data) {
  if (this.partialStreams.length > 0 && this.partialStreams[0].header === null) {
    //the previous chunk didn't had a header
    var previousChunk = this.partialStreams.shift().parts[0];
    var newChunk = Buffer.concat([previousChunk, data], previousChunk.length + data.length);
    this.readChunk(newChunk);
    return;
  }
  var headerReader = new FrameHeaderReader(data);
  if (headerReader.isIncompleteHeader()) {
    this.emit('log', 'info', 'Incomplete frame header');
    this.partialStreams.push({parts: [data], header: null});
    return;
  }
  if (headerReader.isComplete()) {
    if (data.length === headerReader.frameLength) {
      //Its a complete frame, the same size of the chunk
      this.emit('log', 'info', 'Complete frame #' + headerReader.streamId + ' the size of the chunk');
      this.handleFrame(data, headerReader);
    }
    else {
      this.emit('log', 'info', 'Complete frame #' + headerReader.streamId + ' with extra chunks');
      var restOfChunk = new Buffer(data.length - headerReader.frameLength);
      data.copy(restOfChunk, 0, headerReader.frameLength, data.length);
      this.readChunk(restOfChunk);
      
      //Handle initial part of the chunk, the frame
      var frame = new Buffer(headerReader.frameLength);
      data.copy(frame, 0, 0, headerReader.frameLength);
      this.handleFrame(frame, headerReader);
    }
  }
  else if (headerReader.isFrameStart()) {
    //Start partial
    this.partialStreams.push({parts: [data], header: headerReader});
    this.emit('log', 'info', 'Received - start partial #' + headerReader.streamId + ';bytes expected: ' + (headerReader.bodyLength+8) + '; bytes received in the first: ' + data.length);
  }
  else {
    //partial continue, not a valid frame header
    headerReader = null;
    //First in first out
    var partial = this.partialStreams.shift();
    if (!partial) {
      //something is going wrong
      var error = new Error('Unrecognized stream');
      this.emit('log', 'error', error);
    }
    var totalLength = partial.parts[0].length;
    for (var i=1; i<partial.parts.length; i++) {
      totalLength += partial.parts[i].length;
    }
    //add the new part to the partial
    totalLength += data.length;
    //Check if the length matches
    if (totalLength < partial.header.frameLength) {
      partial.parts.push(data);
      this.partialStreams.unshift(partial);
      this.emit('log', 'info', 'Received partial continue #' + partial.header.streamId+ ';bytes expected: ' + (partial.header.bodyLength+8) + '; bytes received so far: ' + totalLength);
      partial = null;
    }
    else if (totalLength === partial.header.frameLength) {
      partial.parts.push(data);
      this.emit('log', 'info', 'Partial end #' + partial.header.streamId);
      this.handleFrame(Buffer.concat(partial.parts, totalLength), partial.header);
      partial = null;
    }
    else {
      //the size is greater than expected.
      this.emit('log', 'info', 'Receiving end  #' + partial.header.streamId + ' and next message tl:' + totalLength + ' bl:' + partial.header.bodyLength);
      
      //calculate where the previous message ends and the new starts
      var nextStart = partial.header.frameLength-(totalLength-data.length);
      
      var nextChunk = new Buffer(data.length-nextStart);
      data.copy(nextChunk, 0, nextStart, data.length);
      this.readChunk(nextChunk);
      
      //end previous message
      var previousMessageEnd = new Buffer(nextStart);
      data.copy(previousMessageEnd, 0, 0, nextStart);
      partial.parts.push(previousMessageEnd);
      this.emit('log', 'info', 'Partial end #' + partial.header.streamId);
      this.handleFrame(Buffer.concat(partial.parts, partial.header.frameLength), partial.header);
      
      partial = null;
      previousMessageEnd = null;
      nextChunk = null;
      data = null;
    }
  }
}

PartialReader.prototype.handleFrame = function (data, header) {
  //emit the frame
  this.emit('frame', data, header);
}

/**
 * Represents a handler that parses an empty or error response
 */
function EmptyResponseHandler(callback) {
  this.callback = callback;
}

EmptyResponseHandler.prototype.handle = function(data) {
  var reader = new FrameReader(data);
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
  var reader = new FrameReader(data);
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
/**
 * Represents a error message by the server
 * @param {boolean} serverUnhealthy: If true, it tells the client that the server could be in trouble and it should not retry
*/
function ResponseError(code, message, isServerUnhealthy) {
  this.code = code;
  this.message = message;
  this.isServerUnhealthy = isServerUnhealthy;
}
util.inherits(ResponseError, Error);
/**
 * Represents a error message by the server
 * @param {boolean} serverUnhealthy: If true, it tells the client that the server could be in trouble and it should not retry
*/
function ParserError(err, rowIndex, colIndex) {
  ParserError.super_.call(this, err.message);
  this.rowIndex = rowIndex;
  this.colIndex = colIndex;
  this.innerError = err;
  this.name = 'ParserError';
}
util.inherits(ParserError, Error);

exports.FrameReader = FrameReader;
exports.ResponseHandler = ResponseHandler;
exports.EmptyResponseHandler = EmptyResponseHandler
exports.readEvent = readEvent;
exports.FrameHeaderReader = FrameHeaderReader;
exports.PartialReader = PartialReader;