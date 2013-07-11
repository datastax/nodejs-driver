/**
 * Based on https://github.com/isaacbwagner/node-cql3/blob/master/lib/frameParser.js 
 * under the MIT License https://github.com/isaacbwagner/node-cql3/blob/master/LICENSE
 */
var util = require('util');
var utils = require('./utils.js');
var types = require('./types.js');
var Int64 = require('node-int64');

function FrameHeaderReader(buf) {
  this.bufferLength = buf.length;
  this.isResponse = buf[0] & 0x80;
  this.version = buf[0] & 0x7F;
  this.flags = buf.readUInt8(1);
  this.streamId = buf.readInt8(2);
  this.opcode = buf.readUInt8(3);
  this.bodyLength = buf.readUInt32BE(4);
  //TODO: Handle buffers with length < 8
}
/**
 * Determines that the frame starts and ends in the buffer
 */
FrameHeaderReader.prototype.isCompleteFrame = function() {
  return (this.bufferLength === this.bodyLength+8) && 
    this.isFrameResponseStart();
}
/**
 * Determines that the frame contains the frame header in the buffer
 */
FrameHeaderReader.prototype.isFrameResponseStart = function() {
  return ((this.isResponse) &&
    (this.version === 1) &&
    (this.flags < 0x04) &&
    (this.opcode <= types.opcodes.maxCode));
}
/**
 * Identifies the index of the next frameheader inside a buffer
 */
FrameHeaderReader.getNextStart = function(data) {
  var index = -1;
  var header = null;
  for (var i=0;i<data.length;i++) {
    if (data[i] === FrameHeaderReader.startMessageByte) {
      var start = new Buffer(8);
      data.copy(start, 0, i, i+8);
      header = new FrameHeaderReader(start);
      if (header.isFrameResponseStart()) {
        index = i;
        break;
      }
    }
  }
  return {index:index, header: header};
}

/*
 * Identify the beginning of a response message (CQL3 V1 only)
 */
FrameHeaderReader.startMessageByte = 0x81;

function FrameReader(buf, partial) {
    var offset = 0;
    if (!partial) {
      this.request = !(!(buf[0] & 0x80));
      this.version = buf[0] & 0x7F;
      this.flags = buf[1];
      this.streamId = buf.readInt8(2);
      this.opcode = buf[3];
      this.bodyLength = buf.readUInt32BE(4);
      offset = 8;
    }

    this.readInt = function() {
        var result = buf.readInt32BE(offset);
        offset += 4;
        return result;
    };
    
    this.readShort = function() {
        var result = buf.readUInt16BE(offset);
        offset += 2;
        return result;
    };

    this.readByte = function() {
        var result = buf.readUInt8(offset);
        offset += 1;
        return result;
    };

    this.readString = function() {
        var length = this.readShort();
        var result = buf.toString('utf8', offset, offset+length);
        offset += length;
        return result;
    };

    function readStringL(buf) {
        var length = this.readInt();
        var result = buf.toString('utf8', offset, length);
        offset += length;
        return result;
    }

    function readUUID(buf) {
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

    function readStringList(buf) {
        var num = this.readShort();

        var list = [];

        for(var i = 0; i < num; i++) {
            list.push(this.readString());
        }

        return list;
    }

    this.readBytes = function() {
        var num = this.readInt();
        if(num < 0)
            return null;
        //if (offset+num > buf.length) {
        //  console.log('offset', offset, 'num', num, 'buf.length', buf.length, 'this.bodyLength', this.bodyLength, 'this.opcode', this.opcode);
        //}
        var bytes = buf.slice(offset, offset+num);

        offset += num;

        return bytes;
    };

    this.readShortBytes = function() {
        var num = this.readShort();
        if(num < 0)
            return null;

        var bytes = buf.slice(offset, offset+num);

        offset += num;

        return bytes;
    };

    /* returns an array with two elements */
    this.readOption = function () {
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
    };

    /* returns an array of arrays */
    function readOptionList(buf) {
        var num = this.readShort();
        var options = [];
        for(var i = 0; i < num; i++) {
            options.push(this.readOption());
        }
        return options;
    }

    function readInet(buf) {
      //TODO
    }

    function readStringMap(buf) {
        var num = this.readShort();
        var map = {};
        for(var i = 0; i < num; i++) {
            var key = this.readString();
            var value = this.readString();
            map[key] = value;
        }
        return map;
    }

    function readStringMultimap(buf) {
        var num = this.readShort();
        var map = {};
        for(var i = 0; i < num; i++) {
            var key = this.readString();
            var value = this.readStringList();
            map[key] = value;
        }
        return map;
    }    

    this.readMetadata = function() {
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
            meta.columns['_col_' + spec.column_name] = i;
        }

        return meta;
    };

    this.parseResult = function () {
      
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
            id: this.readShortBytes(),
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
    };

    this.parseRows = function () {
      var meta = this.readMetadata();
      var rowCount = this.readInt();
      var rows = [];
      for(var i = 0; i < rowCount; i++) {
        var row = [];
        row.get = (function (row, columns) {
          return function(name) {
            var cellIndex = name;
            if (typeof cellIndex === 'string') {
              cellIndex = columns['_col_' + name];
            }
            return row[cellIndex];
          }
        })(row, meta.columns);
        for(var col = 0; col < meta.columns.length; col++ ) {
          var spec = meta.columns[col];
          var cellValue = null;
          try {
            var bytes = this.readBytes();
            cellValue = this.convert(bytes, spec.type);
          }
          catch (e) {
            throw new ParserError(e, i, col);
          }
          row[col] = cellValue;
        }
        rows.push(row);
      }

      return {
        meta: meta,
        rows: rows
      };
    };
    
    this.convert = function(bytes, type) {
      if (bytes === null) {
        return null;
      }
      switch(type[0]) {
        case 0x0000: //Custom
        case 0x0006: //Decimal
        case 0x0010: //Inet
        case 0x000E: //Varint
        case 0x000F: //Timeuuid
          //return buffer and move on :)
          //https://github.com/datastax/csharp-driver/blob/master/Cassandra/TypeAdapters.cs
          return bytes;
          break;
        case 0x0001: //Ascii
          return bytes.toString('ascii');
        case 0x0002: //Bigint
        case 0x0005: //Counter
        case 0x000B: //Timestamp
          return this.readBigNumber(bytes);
        case 0x0003: //Blob
          return bytes;
        case 0x0004: //Boolean
          return !!bytes.readUInt8(0);
        case 0x0007: //Double
          return bytes.readDoubleBE(0);
        case 0x0008: //Float
          return bytes.readFloatBE(0);
        case 0x0009: //Int
          return bytes.readInt32BE(0);
        case 0x000A: //Text
        case 0x000C: //Uuid
        case 0x000D: //Varchar
          return bytes.toString('utf8');
        case 0x0020:
        case 0x0022:
          var subReader = new FrameReader(bytes, true);
          var list = subReader.readList(type[1][0]);
          return list
        case 0x0021:
          var subReader = new FrameReader(bytes, true);
          var map = subReader.readMap(type[1][0][0], type[1][1][0]);
          return map;
      }

      throw new Error('Unknown data type: ' + type[0]);
    };
    
    this.readBigNumber = function(bytes) {
      var value = new Int64(bytes);
      return value;
    }

    this.readList = function (type) {
      var num = this.readShort();
      var list = [];
      for(var i = 0; i < num; i++) {
        //advance
        var length = this.readShort();
        //slice it
        list.push(this.convert(buf.slice(offset, offset+length), [type]));
        offset += length;
      }
      return list;
    }

    this.readMap = function (type1, type2) {
      var num = this.readShort();
      var map = {};
      for(var i = 0; i < num; i++) {
        var keyLength = this.readShort();
        var key = this.convert(buf.slice(offset, offset+keyLength), [type1]);
        offset += keyLength;
        var valueLength = this.readShort();
        var value = this.convert(buf.slice(offset, offset+valueLength), [type2]);
        map[key] = value;
        offset += valueLength;
      }
      return map;
    }
    
    this.readError = function () {
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
};

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
  else if(reader.opcode === types.opcodes.result) {
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
  }
  else if (reader.opcode === types.opcodes.authenticate) {
    reader = null;
    this.authCallback(this.callback);
  }
  else {
    throw new Error('Opcode not handled: ' + reader.opcode);
  }
}


function eventResponseHandler(data, emitter) {
    var reader = new FrameReader(data);
    var event = reader.readString();
    console.log('emitting ' + event);
    if(event == 'TOPOLOGY_CHANGE') {
      emitter.emit(event, reader.readString(), reader.readInet());
    } else if (event == 'STATUS_CHANGE') {
      emitter.emit(event, reader.readString(), reader.readInet());
    } else if (event == 'SCHEMA_CHANGE') {
      emitter.emit(event, reader.readString(), reader.readString(), reader.readString());
    } else {
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
exports.eventResponseHandler = eventResponseHandler;
exports.FrameHeaderReader = FrameHeaderReader;