var util = require('util');
var uuid = require('node-uuid');

var types = require('./types.js');
var dataTypes = types.dataTypes;
var Long = types.Long;
var utils = require('./utils.js');
/**
 * Encodes and decodes from a type to Cassandra bytes
 */
var encoder = (function(){
  /**
   * Decodes Cassandra bytes into Javascript values.
   */
  function decode(bytes, type) {
    if (bytes === null) {
      return null;
    }
    switch(type[0]) {
      case dataTypes.custom:
      case dataTypes.decimal:
      case dataTypes.inet:
      case dataTypes.varint:
        //return buffer and move on :)
        return utils.copyBuffer(bytes);
      case dataTypes.ascii:
        return bytes.toString('ascii');
      case dataTypes.bigint:
      case dataTypes.counter:
        return decodeBigNumber(utils.copyBuffer(bytes));
      case dataTypes.timestamp:
        return decodeTimestamp(utils.copyBuffer(bytes));
      case dataTypes.blob:
        return utils.copyBuffer(bytes);
      case dataTypes.boolean:
        return !!bytes.readUInt8(0);
      case dataTypes.double:
        return bytes.readDoubleBE(0);
      case dataTypes.float:
        return bytes.readFloatBE(0);
      case dataTypes.int:
        return bytes.readInt32BE(0);
      case dataTypes.uuid:
      case dataTypes.timeuuid:
        return uuid.unparse(bytes);
      case dataTypes.text:
      case dataTypes.varchar:
        return bytes.toString('utf8');
      case dataTypes.list:
      case dataTypes.set:
        var list = decodeList(bytes, type[1][0]);
        return list;
      case dataTypes.map:
        var map = decodeMap(bytes, type[1][0][0], type[1][1][0]);
        return map;
    }

    throw new Error('Unknown data type: ' + type[0]);
  }

  function decodeBigNumber (bytes) {
    return Long.fromBuffer(bytes);
  }

  function decodeTimestamp (bytes) {
    return new Date(decodeBigNumber(bytes).toNumber());
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
      map[key] = decode(bytes.slice(offset, offset+valueLength), [type2]);
      offset += valueLength;
    }
    return map;
  }

  /**
   * @param value
   * @param {({type: number, [subtypes]: Array}|String|Number)} [typeInfo]
   * @returns {*}
   */
  function encode (value, typeInfo) {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeInfo) {
      if (typeof typeInfo === 'number') {
        typeInfo = {type: typeInfo};
      }
      if (typeof typeInfo === 'string') {
        typeInfo = dataTypes.getByName(typeInfo);
      }
    }
    else {
      //Lets guess
      typeInfo = guessDataType(value);
      if (!typeInfo) {
        throw new TypeError('Target data type could not be guessed, you must specify a hint.', value);
      }
    }
    switch (typeInfo.type) {
      case dataTypes.int:
        return encodeInt(value);
      case dataTypes.float:
        return encodeFloat(value);
      case dataTypes.double:
        return encodeDouble(value);
      case dataTypes.boolean:
        return encodeBoolean(value);
      case dataTypes.text:
      case dataTypes.varchar:
        return encodeString(value);
      case dataTypes.ascii:
        return encodeString(value, 'ascii');
      case dataTypes.uuid:
      case dataTypes.timeuuid:
        return encodeUuid(value);
      case dataTypes.custom:
      case dataTypes.decimal:
      case dataTypes.inet:
      case dataTypes.varint:
      case dataTypes.blob:
        return encodeBlob(value, typeInfo.type);
      case dataTypes.bigint:
      case dataTypes.counter:
        return encodeBigNumber(value, typeInfo.type);
      case dataTypes.timestamp:
        return encodeTimestamp(value, typeInfo.type);
      case dataTypes.list:
      case dataTypes.set:
        return encodeList(value, typeInfo);
      case dataTypes.map:
        return encodeMap(value, typeInfo);
      default:
        throw new TypeError('Type not supported ' + typeInfo.type, value);
    }
  }

  /**
   * Try to guess the Cassandra type to be stored, based on the javascript value type
   * @param value
   * @returns {{type: number}}
   */
  function guessDataType (value) {
    var type = null;
    if (typeof value === 'number') {
      type = dataTypes.double;
    }
    else if(value instanceof Date) {
      type = dataTypes.timestamp;
    }
    else if(value instanceof Long) {
      type = dataTypes.bigint;
    }
    else if (typeof value === 'string') {
      type = dataTypes.text;
      if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)){
        type = dataTypes.uuid;
      }
    }
    else if (value instanceof Buffer) {
      type = dataTypes.blob;
    }
    else if (util.isArray(value)) {
      type = dataTypes.list;
    }
    else if (value === true || value === false) {
      type = dataTypes.boolean;
    }

    if (type === null) {
      return null;
    }
    return {type: type};
  }

  function encodeInt (value) {
    if (typeof value !== 'number') {
      throw new TypeError(null, value, 'number');
    }
    var buf = new Buffer(4);
    buf.writeInt32BE(value, 0);
    return buf;
  }

  function encodeFloat (value) {
    if (typeof value !== 'number') {
      throw new TypeError(null, value, 'number');
    }
    var buf = new Buffer(4);
    buf.writeFloatBE(value, 0);
    return buf;
  }

  function encodeDouble (value) {
    if (typeof value !== 'number') {
      throw new TypeError(null, value, 'number');
    }
    var buf = new Buffer(8);
    buf.writeDoubleBE(value, 0);
    return buf;
  }

  function encodeTimestamp (value, type) {
    if (value instanceof Date) {
      value = value.getTime();
    }
    return encodeBigNumber (value, type);
  }

  function encodeUuid (value) {
    if (typeof value === 'string') {
      value = uuid.parse(value, new Buffer(16));
    }
    if (!(value instanceof Buffer)) {
      throw new TypeError('Only Buffer and string objects allowed for UUID values', value, Buffer);
    }
    return value;
  }

  function encodeBigNumber (value, type) {
    var buf = getBigNumberBuffer(value);
    if (buf === null) {
      throw new TypeError(null, value, Buffer, null, type);
    }
    return buf;
  }

  function getBigNumberBuffer (value) {
    var buf = null;
    if (value instanceof Buffer) {
      buf = value;
    } else if (value instanceof Long) {
      buf = Long.toBuffer(value);
    } else if (typeof value === 'number') {
      buf = Long.toBuffer(Long.fromNumber(value));
    }
    return buf;
  }

  function encodeString (value, encoding) {
    if (typeof value !== 'string') {
      throw new TypeError(null, value, 'string');
    }
    return new Buffer(value, encoding);
  }

  function encodeBlob (value, type) {
    if (!(value instanceof Buffer)) {
      throw new TypeError(null, value, Buffer, null, type);
    }
    return value;
  }

  function encodeBoolean(value) {
    return new Buffer([(value ? 1 : 0)]);
  }

  function encodeList(value, typeInfo) {
    if (!util.isArray(value)) {
      throw new TypeError(null, value, Array, null, typeInfo);
    }
    if (value.length === 0) {
      return null;
    }
    var parts = [];
    parts.push(getLengthBuffer(value));
    var subtype = typeInfo.subtypes ? typeInfo.subtypes[0] : null;
    for (var i=0;i<value.length;i++) {
      var bytes = encode(value[i], subtype);
      //include item byte length
      parts.push(getLengthBuffer(bytes));
      //include item
      parts.push(bytes);
    }
    return Buffer.concat(parts);
  }

  /**
   * Serializes a map into a Buffer
   * @param value
   * @param {{type: number, [subtypes]: Array}} [typeInfo]
   * @returns {Buffer}
   */
  function encodeMap(value, typeInfo) {
    var parts = [];
    var propCounter = 0;
    var keySubtype = null;
    var valueSubtype = null;
    if (typeInfo.subtypes) {
      keySubtype = typeInfo.subtypes[0];
      valueSubtype = typeInfo.subtypes[1];
    }
    for (var key in value) {
      if (!value.hasOwnProperty(key)) continue;
      //add the key and the value
      var keyBuffer = encode(key, keySubtype);
      //include item byte length
      parts.push(getLengthBuffer(keyBuffer));
      //include item
      parts.push(keyBuffer);
      //value
      var valueBuffer = encode(value[key], valueSubtype);
      //include item byte length
      parts.push(getLengthBuffer(valueBuffer));
      //include item
      if (valueBuffer !== null) {
        parts.push(valueBuffer);
      }
      propCounter++;
    }

    parts.unshift(getLengthBuffer(propCounter));

    return Buffer.concat(parts);
  }

  /**
   * Gets a buffer containing with 2 bytes (BE) representing the array length or the value
   * @param {Buffer|Number} value
   * @returns {Buffer}
   */
  function getLengthBuffer(value) {
    var lengthBuffer = new Buffer(2);
    if (!value) {
      lengthBuffer.writeUInt16BE(0, 0);
    }
    else if (value.length) {
      lengthBuffer.writeUInt16BE(value.length, 0);
    }
    else {
      lengthBuffer.writeUInt16BE(value, 0);
    }
    return lengthBuffer;
  }

  return {
    decode: decode,
    encode: encode,
    guessDataType: guessDataType};
})();

module.exports = encoder;
