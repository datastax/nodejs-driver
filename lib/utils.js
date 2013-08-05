var util = require('util');
var Int64 = require('node-int64');
/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  var targetBuffer = new Buffer(buf.length);
  buf.copy(targetBuffer);
  return targetBuffer;
}

/**
 * Merge the contents of two or more objects together into the first object. Similar to jQuert.extend
 */
function extend(target) {
  var sources = [].slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (!target.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
}

var queryParser = (function() {
  function parse(query, args) {
    if (args == null || args.length === 0) {
      return query;
    }

    var q = 0;
    var a = 0;
    var str = '';

    try {
      while (q >= 0) {
        var oldq = q;
        q = query.indexOf('?', q);
        if (q >= 0) {
          str += query.substr(oldq, q-oldq);
          if (args[a] === undefined) {
            throw new QueryParserError('Query parameter number ' + (a+1) + ' is not defined. Placeholder for not provided argument.');
          }
          str += encodeParam(args[a++]);
          q += 1;
        } else {
          str += query.substr(oldq);
        }
      }

      return str;
    }
    catch (e) {
      throw new QueryParserError(e.message);
    }
  }

  function quote(x) {
    return '\'' + x + '\'';
  }

  function encodeParam(x) {
    if(x === null) {
      return 'null';
    }
    if(x instanceof Date) {
      return x.getTime().toString();
    }
    if(x instanceof Array) {
      return stringifyArray(x);
    }
    if (typeof x === 'string') {
      //quotestring
      return quote(x);
    }
    if (x instanceof Int64) {
      return stringifyInt64(x);
    }
    if (x.hint) {
      if (x.hint === 'set') {
        return stringifyArray(x.value, '{', '}');
      }
      else if (x.hint === 'map') {
        return stringifyMap(x.value);
      }
    }
    if (x instanceof Buffer) {
      return '0x' + x.toString('hex');
    }
    if (x.toString) {
      return x.toString();
    }
    return x;
  }
  
  function stringifyArray(x, openChar, closeChar) {
    if (!openChar) {
      openChar = '[';
      closeChar = ']';
    }
    var stringValues = new Array();
    for (var i=0;i<x.length;i++) {
      stringValues.push(encodeParam(x[i]));
    }
    return openChar + stringValues.join() + closeChar;
  }
  
  function stringifyInt64(x) {
    return 'blobAsBigint(0x' + x.toOctetString() + ')';
  }
  
  function stringifyMap(x) {
    var stringValues = new Array();
    for (var key in x) {
      stringValues.push(encodeParam(key) + ':' + encodeParam(x[key]));
    }
    return '{' + stringValues.join() + '}';
  }
  
  function QueryParserError(message) {
    this.message = message;
    Error.call(this, message);
  }

  util.inherits(QueryParserError, Error);
  
  return {
    parse: parse,
    encodeParam: encodeParam
  }
})();

/**
 * Encodes and decodes from a type to Cassandra bytes
 */
var typeEncoder = (function(){
  /**
   * Decodes Cassandra bytes into Javascript values.
   */
  function decode(bytes, type) {
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
        return copyBuffer(bytes);
        break;
      case 0x0001: //Ascii
        return bytes.toString('ascii');
      case 0x0002: //Bigint
      case 0x0005: //Counter
      case 0x000B: //Timestamp
        return decodeBigNumber(copyBuffer(bytes));
      case 0x0003: //Blob
        return copyBuffer(bytes);
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
        var list = decodeList(bytes, type[1][0]);
        return list;
      case 0x0021:
        var map = decodeMap(bytes, type[1][0][0], type[1][1][0]);
        return map;
    }

    throw new Error('Unknown data type: ' + type[0]);
  }
  
  function decodeBigNumber (bytes) {
    var value = new Int64(bytes);
    return value;
  }

  /*
   * Reads a list from bytes
   */
  function decodeList (bytes, type) {
    var offset = 0;
    //a short containing the total items
    var totalItems = bytes.readUInt16BE(offset);
    offset += 2;
    var list = [];
    for(var i = 0; i < totalItems; i++) {
      //bytes length of the item
      var length = bytes.readUInt16BE(offset);
      offset += 2;
      //slice it
      list.push(decode(bytes.slice(offset, offset+length), [type]));
      offset += length;
    }
    return list;
  }

  /*
   * Reads a map (key / value) from bytes
   */
  function decodeMap (bytes, type1, type2) {
    var offset = 0;
    //a short containing the total items
    var totalItems = bytes.readUInt16BE(offset);
    offset += 2;
    var map = {};
    for(var i = 0; i < totalItems; i++) {
      var keyLength = bytes.readUInt16BE(offset);
      offset += 2;
      var key = decode(bytes.slice(offset, offset+keyLength), [type1]);
      offset += keyLength;
      var valueLength = bytes.readUInt16BE(offset);
      offset += 2;
      var value = decode(bytes.slice(offset, offset+valueLength), [type2]);
      map[key] = value;
      offset += valueLength;
    }
    return map;
  }
  
  return {decode : decode};
})();

exports.copyBuffer = copyBuffer;
exports.extend = extend;
exports.queryParser = queryParser;
exports.typeEncoder = typeEncoder;