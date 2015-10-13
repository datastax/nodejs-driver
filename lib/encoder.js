"use strict";
var util = require('util');

var types = require('./types');
var dataTypes = types.dataTypes;
var Long = types.Long;
var Integer = types.Integer;
var BigDecimal = types.BigDecimal;
var utils = require('./utils');
/**
 * @const
 * @type {RegExp}
 */
var uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** @const */
var int16Zero = new Buffer([0, 0]);
/** @const */
var int32Zero = new Buffer([0, 0, 0, 0]);
/** @const */
var complexTypeNames = Object.freeze({
  list      : 'org.apache.cassandra.db.marshal.ListType',
  set       : 'org.apache.cassandra.db.marshal.SetType',
  map       : 'org.apache.cassandra.db.marshal.MapType',
  udt       : 'org.apache.cassandra.db.marshal.UserType',
  tuple     : 'org.apache.cassandra.db.marshal.TupleType',
  frozen    : 'org.apache.cassandra.db.marshal.FrozenType',
  reversed  : 'org.apache.cassandra.db.marshal.ReversedType',
  composite : 'org.apache.cassandra.db.marshal.CompositeType',
  collection: 'org.apache.cassandra.db.marshal.ColumnToCollectionType'
});
/** @const */
var singleTypeNames = Object.freeze({
  'org.apache.cassandra.db.marshal.UTF8Type':           dataTypes.varchar,
  'org.apache.cassandra.db.marshal.AsciiType':          dataTypes.ascii,
  'org.apache.cassandra.db.marshal.UUIDType':           dataTypes.uuid,
  'org.apache.cassandra.db.marshal.TimeUUIDType':       dataTypes.timeuuid,
  'org.apache.cassandra.db.marshal.Int32Type':          dataTypes.int,
  'org.apache.cassandra.db.marshal.BytesType':          dataTypes.blob,
  'org.apache.cassandra.db.marshal.FloatType':          dataTypes.float,
  'org.apache.cassandra.db.marshal.DoubleType':         dataTypes.double,
  'org.apache.cassandra.db.marshal.BooleanType':        dataTypes.boolean,
  'org.apache.cassandra.db.marshal.InetAddressType':    dataTypes.inet,
  'org.apache.cassandra.db.marshal.SimpleDateType':     dataTypes.date,
  'org.apache.cassandra.db.marshal.TimeType':           dataTypes.time,
  'org.apache.cassandra.db.marshal.ShortType':          dataTypes.smallint,
  'org.apache.cassandra.db.marshal.ByteType':           dataTypes.tinyint,
  'org.apache.cassandra.db.marshal.DateType':           dataTypes.timestamp,
  'org.apache.cassandra.db.marshal.TimestampType':      dataTypes.timestamp,
  'org.apache.cassandra.db.marshal.LongType':           dataTypes.bigint,
  'org.apache.cassandra.db.marshal.DecimalType':        dataTypes.decimal,
  'org.apache.cassandra.db.marshal.IntegerType':        dataTypes.varint,
  'org.apache.cassandra.db.marshal.CounterColumnType':  dataTypes.counter
});
var singleTypeNamesLength = Object.keys(singleTypeNames).reduce(function (previous, current) {
  return current.length > previous ? current.length : previous;
}, 0);
var nullValueBuffer = new Buffer([255, 255, 255, 255]);

/**
 * Encodes and decodes from a type to Cassandra bytes
 * @param {Number} protocolVersion
 * @param {ClientOptions} options
 * @constructor
 */
function Encoder(protocolVersion, options) {
  this.encodingOptions = options.encoding || utils.emptyObject;
  this.setProtocolVersion(protocolVersion);
  this.setEncoders();
  if (this.encodingOptions.copyBuffer) {
    this.handleBuffer = this.handleBufferCopy;
  }
  else {
    this.handleBuffer = this.handleBufferRef;
  }
}

/**
 * Sets the encoder and decoder methods for this instance
 */
Encoder.prototype.setEncoders = function () {
  //decoders
  var d = {};
  d[dataTypes.custom] = this.decodeBlob;
  d[dataTypes.ascii] = this.decodeAsciiString;
  d[dataTypes.bigint] = this.decodeLong;
  d[dataTypes.blob] = this.decodeBlob;
  d[dataTypes.boolean] = this.decodeBoolean;
  d[dataTypes.counter] = this.decodeLong;
  d[dataTypes.decimal] = this.decodeDecimal;
  d[dataTypes.double] = this.decodeDouble;
  d[dataTypes.float] = this.decodeFloat;
  d[dataTypes.int] = this.decodeInt;
  d[dataTypes.text] = this.decodeUtf8String;
  d[dataTypes.timestamp] = this.decodeTimestamp;
  d[dataTypes.uuid] = this.decodeUuid;
  d[dataTypes.varchar] = this.decodeUtf8String;
  d[dataTypes.varint] = this.decodeVarint;
  d[dataTypes.timeuuid] = this.decodeTimeUuid;
  d[dataTypes.inet] = this.decodeInet;
  d[dataTypes.date] = this.decodeDate;
  d[dataTypes.time] = this.decodeTime;
  d[dataTypes.smallint] = this.decodeSmallint;
  d[dataTypes.tinyint] = this.decodeTinyint;
  d[dataTypes.list] = this.decodeList;
  d[dataTypes.map] = this.decodeMap;
  d[dataTypes.set] = this.decodeSet;
  d[dataTypes.udt] = this.decodeUdt;
  d[dataTypes.tuple] = this.decodeTuple;

  //encoders
  var e = {};
  e[dataTypes.custom] = this.encodeBlob;
  e[dataTypes.ascii] = this.encodeAsciiString;
  e[dataTypes.bigint] = this.encodeLong;
  e[dataTypes.blob] = this.encodeBlob;
  e[dataTypes.boolean] = this.encodeBoolean;
  e[dataTypes.counter] = this.encodeLong;
  e[dataTypes.decimal] = this.encodeDecimal;
  e[dataTypes.double] = this.encodeDouble;
  e[dataTypes.float] = this.encodeFloat;
  e[dataTypes.int] = this.encodeInt;
  e[dataTypes.text] = this.encodeUtf8String;
  e[dataTypes.timestamp] = this.encodeTimestamp;
  e[dataTypes.uuid] = this.encodeUuid;
  e[dataTypes.varchar] = this.encodeUtf8String;
  e[dataTypes.varint] = this.encodeVarint;
  e[dataTypes.timeuuid] = this.encodeUuid;
  e[dataTypes.inet] = this.encodeInet;
  e[dataTypes.date] = this.encodeDate;
  e[dataTypes.time] = this.encodeTime;
  e[dataTypes.smallint] = this.encodeSmallint;
  e[dataTypes.tinyint] = this.encodeTinyint;
  e[dataTypes.list] = this.encodeList;
  e[dataTypes.map] = this.encodeMap;
  e[dataTypes.set] = this.encodeSet;
  e[dataTypes.udt] = this.encodeUdt;
  e[dataTypes.tuple] = this.encodeTuple;

  this.decoders = d;
  this.encoders = e;
};

/**
 * Decodes Cassandra bytes into Javascript values.
 * @param {Buffer} buffer
 * @param {{code: Number, info: *|Object}} type
 */
Encoder.prototype.decode = function (buffer, type) {
  if (buffer === null) {
    return null;
  }
  var decoder = this.decoders[type.code];
  if (!decoder) {
    throw new Error('Unknown data type: ' + type.code);
  }
  return decoder.call(this, buffer, type.info);
};

/**
 * @param value
 * @param {{code: number, info: *|Object}|String|Number} [typeInfo]
 * @returns {Buffer}
 * @throws {TypeError} When there is an encoding error
 */
Encoder.prototype.encode = function (value, typeInfo) {
  if (value === undefined) {
    //defaults to null
    value = null;
    if (this.encodingOptions.useUndefinedAsUnset) {
      //use undefined as unset
      value = types.unset;
    }
  }
  if (value === null) {
    return value;
  }
  if (value === types.unset) {
    if (this.protocolVersion < 4) {
      throw new TypeError('Unset value can not be used for this version of Cassandra, protocol version: ' + this.protocolVersion);
    }
    return value;
  }
  /** @type {{code: Number, info: object}} */
  var type = {
    code: null,
    info: null
  };
  if (typeInfo) {
    if (typeof typeInfo === 'number') {
      type.code = typeInfo;
    }
    else if (typeof typeInfo === 'string') {
      type = dataTypes.getByName(typeInfo);
    }
    if (typeof typeInfo.code === 'number') {
      type.code = typeInfo.code;
      type.info = typeInfo.info;
    }
    if (typeof type.code !== 'number') {
      throw new TypeError('Type information not valid, only String and Number values are valid hints');
    }
  }
  else {
    //Lets guess
    type = this.guessDataType(value);
    if (!type) {
      throw new TypeError('Target data type could not be guessed, you should use prepared statements for accurate type mapping. Value: ' + util.inspect(value));
    }
  }
  var encoder = this.encoders[type.code];
  if (!encoder) {
    throw new Error('Type not supported ' + type.code);
  }
  return encoder.call(this, value, type.info);
};

/**
 * Try to guess the Cassandra type to be stored, based on the javascript value type
 * @param value
 * @returns {{code: number, info: object}}
 */
Encoder.prototype.guessDataType = function (value) {
  var code = null;
  if (typeof value === 'number') {
    code = dataTypes.double;
  }
  else if (typeof value === 'string') {
    code = dataTypes.text;
    if (value.length === 36 && uuidRegex.test(value)){
      code = dataTypes.uuid;
    }
  }
  else if (value instanceof Buffer) {
    code = dataTypes.blob;
  }
  else if (value instanceof Date) {
    code = dataTypes.timestamp;
  }
  else if (value instanceof Long) {
    code = dataTypes.bigint;
  }
  else if (value instanceof Integer) {
    code = dataTypes.varint;
  }
  else if (value instanceof BigDecimal) {
    code = dataTypes.decimal;
  }
  else if (value instanceof types.Uuid) {
    code = dataTypes.uuid;
  }
  else if (value instanceof types.InetAddress) {
    code = dataTypes.inet;
  }
  else if (value instanceof types.Tuple) {
    code = dataTypes.tuple;
  }
  else if (value instanceof types.LocalDate) {
    code = dataTypes.date;
  }
  else if (value instanceof types.LocalTime) {
    code = dataTypes.time;
  }
  else if (util.isArray(value)) {
    code = dataTypes.list;
  }
  else if (value === true || value === false) {
    code = dataTypes.boolean;
  }

  if (code === null) {
    return null;
  }
  return {code: code, info: null};
};

/**
 * If not provided, it uses the array of buffers or the parameters and hints to build the routingKey
 * @param {Array} params
 * @param {QueryOptions} options
 * @param [keys] parameter keys and positions
 * @throws TypeError
 */
Encoder.prototype.setRoutingKey = function (params, options, keys) {
  var totalLength;
  if (util.isArray(options.routingKey)) {
    if (options.routingKey.length === 1) {
      options.routingKey = options.routingKey[0];
      return;
    }
    //Is a Composite key
    totalLength = 0;
    for (var i = 0; i < options.routingKey.length; i++) {
      var item = options.routingKey[i];
      if (!item) {
        //An routing key part may be null/undefined if provided by user
        //Or when there is a hardcoded parameter in the query
        //Clear the routing key
        options.routingKey = null;
        return;
      }
      totalLength += item.length + 3;
    }
    //Set the buffer containing the contents of the previous Array of buffers as routing key
    options.routingKey = this._concatRoutingKey(options.routingKey, totalLength);
    return;
  }
  if (options.routingKey instanceof Buffer || !params || params.length === 0) {
    //There is already a routing key
    // or no parameter indexes for routing were provided
    // or there are no parameters to build the routing key
    return;
  }
  var parts = [];
  totalLength = 0;
  if (options.routingIndexes) {
    totalLength = this._encodeRoutingKeyParts(parts, options.routingIndexes, params, options.hints);
  }
  if (options.routingNames && keys) {
    totalLength = this._encodeRoutingKeyParts(parts, options.routingNames, params, options.hints, keys);
  }
  if (totalLength === 0) {
    options.routingKey = null;
    return;
  }
  if (parts.length === 1) {
    options.routingKey = parts[0];
    return;
  }
  //its a composite partition key
  options.routingKey = this._concatRoutingKey(parts, totalLength);
};

/**
 * @param {Array} parts
 * @param {Array} routingIndexes
 * @param {Array} params
 * @param {Array} hints
 * @param {Object} [keys]
 * @returns {Number} The total length
 * @private
 */
Encoder.prototype._encodeRoutingKeyParts = function (parts, routingIndexes, params, hints, keys) {
  hints = hints || utils.emptyArray;
  var totalLength = 0;
  for (var i = 0; i < routingIndexes.length; i++) {
    var paramIndex = routingIndexes[i];
    if (typeof paramIndex === 'undefined') {
      //probably undefined (parameter not found) or bad input from the user
      return 0;
    }
    if (keys) {
      //is composed of parameter names
      paramIndex = keys[paramIndex];
    }
    var item = this.encode(params[paramIndex], hints[paramIndex]);
    if (!item) {
      //bad input from the user
      return 0;
    }
    totalLength += item.length + 3;
    parts.push(item);
  }
  return totalLength;
};

/**
 *
 * @param {Array.<Buffer>} parts
 * @param {Number} totalLength
 * @returns {Buffer}
 * @private
 */
Encoder.prototype._concatRoutingKey = function (parts, totalLength) {
  var routingKey = new Buffer(totalLength);
  var offset = 0;
  parts.forEach(function (item) {
    routingKey.writeInt16BE(item.length, offset);
    offset += 2;
    item.copy(routingKey, offset);
    offset += item.length;
    routingKey[offset] = 0;
    offset++;
  });
  return routingKey;
};

/**
 * Sets the protocol version and the encoding/decoding methods depending on the protocol version
 * @param {Number} value
 */
Encoder.prototype.setProtocolVersion = function (value) {
  this.protocolVersion = value;
  //Set the collection serialization based on the protocol version
  this.decodeCollectionLength = this.decodeCollectionLengthV3;
  this.getLengthBuffer = this.getLengthBufferV3;
  this.collectionLengthSize = 4;
  if (this.protocolVersion < 3) {
    this.decodeCollectionLength = this.decodeCollectionLengthV2;
    this.getLengthBuffer = this.getLengthBufferV2;
    this.collectionLengthSize = 2;
  }
};

Encoder.prototype.decodeBlob = function (bytes) {
  return this.handleBuffer(bytes);
};

Encoder.prototype.decodeUtf8String = function (bytes) {
  return bytes.toString('utf8');
};

Encoder.prototype.decodeAsciiString = function (bytes) {
  return bytes.toString('ascii');
};

Encoder.prototype.decodeBoolean = function (bytes) {
  return !!bytes.readUInt8(0);
};

Encoder.prototype.decodeDouble = function (bytes) {
  return bytes.readDoubleBE(0);
};

Encoder.prototype.decodeFloat = function (bytes) {
  return bytes.readFloatBE(0);
};

Encoder.prototype.decodeInt = function (bytes) {
  return bytes.readInt32BE(0);
};

Encoder.prototype.decodeSmallint = function (bytes) {
  return bytes.readInt16BE(0);
};

Encoder.prototype.decodeTinyint = function (bytes) {
  return bytes.readInt8(0);
};

Encoder.prototype.decodeLong = function (bytes) {
  return Long.fromBuffer(bytes);
};

Encoder.prototype.decodeVarint = function (bytes) {
  return Integer.fromBuffer(bytes);
};

Encoder.prototype.decodeDecimal = function(bytes) {
  return BigDecimal.fromBuffer(bytes);
};

Encoder.prototype.decodeTimestamp = function(bytes) {
  return new Date(this.decodeLong(bytes).toNumber());
};

Encoder.prototype.decodeDate = function (bytes) {
  return types.LocalDate.fromBuffer(bytes);
};

Encoder.prototype.decodeTime = function (bytes) {
  return types.LocalTime.fromBuffer(bytes);
};

/*
 * Reads a list from bytes
 */
Encoder.prototype.decodeList = function (bytes, subtype) {
  var totalItems = this.decodeCollectionLength(bytes, 0);
  var offset = this.collectionLengthSize;
  var list = new Array(totalItems);
  for(var i = 0; i < totalItems; i++) {
    //bytes length of the item
    var length = this.decodeCollectionLength(bytes, offset);
    offset += this.collectionLengthSize;
    //slice it
    list[i] = this.decode(bytes.slice(offset, offset+length), subtype);
    offset += length;
  }
  return list;
};

/*
 * Reads a Set from bytes
 */
Encoder.prototype.decodeSet = function (bytes, subtype) {
  var arr = this.decodeList(bytes, subtype);
  if (this.encodingOptions.set) {
    var setConstructor = this.encodingOptions.set;
    return new setConstructor(arr);
  }
  return arr;
};

/*
 * Reads a map (key / value) from bytes
 */
Encoder.prototype.decodeMap = function (bytes, subtypes) {
  var map;
  var totalItems = this.decodeCollectionLength(bytes, 0);
  var offset = this.collectionLengthSize;
  var self = this;
  function readValues(callback, thisArg) {
    for(var i = 0; i < totalItems; i++) {
      var keyLength = self.decodeCollectionLength(bytes, offset);
      offset += self.collectionLengthSize;
      var key = self.decode(bytes.slice(offset, offset + keyLength), subtypes[0]);
      offset += keyLength;
      var valueLength = self.decodeCollectionLength(bytes, offset);
      offset += self.collectionLengthSize;
      var value = self.decode(bytes.slice(offset, offset + valueLength), subtypes[1]);
      offset += valueLength;
      callback.call(thisArg, key, value);
    }
  }
  if (this.encodingOptions.map) {
    var mapConstructor = this.encodingOptions.map;
    map = new mapConstructor();
    readValues(map.set, map);
  }
  else {
    map = {};
    readValues(function (key, value) {
      map[key] = value;
    });
  }
  return map;
};

/**
 * Decodes collection length for protocol v3 and above
 * @param bytes
 * @param offset
 * @returns {Number}
 */
Encoder.prototype.decodeCollectionLengthV3 = function (bytes, offset) {
  return bytes.readUInt32BE(offset);
};

/**
 * Decodes collection length for protocol v2 and below
 * @param bytes
 * @param offset
 * @returns {Number}
 */
Encoder.prototype.decodeCollectionLengthV2 = function (bytes, offset) {
  return bytes.readUInt16BE(offset)
};

Encoder.prototype.decodeUuid = function (bytes) {
  return new types.Uuid(this.handleBuffer(bytes));
};

Encoder.prototype.decodeTimeUuid = function (bytes) {
  return new types.TimeUuid(this.handleBuffer(bytes));
};

Encoder.prototype.decodeInet = function (bytes) {
  return new types.InetAddress(this.handleBuffer(bytes));
};

/**
 * Decodes a user defined type into an object
 * @param {Buffer} bytes
 * @param {{fields: Array}} udtInfo
 */
Encoder.prototype.decodeUdt = function (bytes, udtInfo) {
  var result = {};
  var offset = 0;
  for (var i = 0; i < udtInfo.fields.length && offset < bytes.length; i++) {
    //bytes length of the field value
    var length = bytes.readInt32BE(offset);
    offset += 4;
    //slice it
    var field = udtInfo.fields[i];
    if (length < 0) {
      result[field.name] = null;
      continue;
    }
    result[field.name] = this.decode(bytes.slice(offset, offset+length), field.type);
    offset += length;
  }
  return result;
};

Encoder.prototype.decodeTuple = function (bytes, tupleInfo) {
  var elements = new Array(tupleInfo.length);
  var offset = 0;
  for (var i = 0; i < tupleInfo.length; i++) {
    var length = bytes.readInt32BE(offset);
    offset += 4;
    if (length < 0) {
      elements[i] = null;
      continue;
    }
    elements[i] = this.decode(bytes.slice(offset, offset+length), tupleInfo[i]);
    offset += length;
  }
  return new types.Tuple(elements);
};

Encoder.prototype.encodeFloat = function (value) {
  if (typeof value !== 'number') {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(4);
  buf.writeFloatBE(value, 0);
  return buf;
};

Encoder.prototype.encodeDouble = function (value) {
  if (typeof value !== 'number') {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(8);
  buf.writeDoubleBE(value, 0);
  return buf;
};

/**
 * @param {Date|String|Long|Number} value
 */
Encoder.prototype.encodeTimestamp = function (value) {
  var originalValue = value;
  if (typeof value === 'string') {
    value = new Date(value);
  }
  if (value instanceof Date) {
    //milliseconds since epoch
    value = value.getTime();
    if (isNaN(value)) {
      throw new TypeError('Invalid date: ' + originalValue);
    }
  }
  //noinspection JSCheckFunctionSignatures
  return this.encodeLong(value);
};

/**
 * @param {Date|String|LocalDate} value
 * @returns {Buffer}
 * @throws {TypeError}
 */
Encoder.prototype.encodeDate = function (value) {
  var originalValue = value;
  try {
    if (typeof value === 'string') {
      value = types.LocalDate.fromString(value);
    }
    if (value instanceof Date) {
      value = types.LocalDate.fromDate(value);
    }
  }
  catch (err) {
    //Wrap into a TypeError
    throw new TypeError('LocalDate could not be parsed ' + err);
  }
  if (!(value instanceof types.LocalDate)) {
    throw new TypeError('Expected Date/String/LocalDate, obtained ' + util.inspect(originalValue));
  }
  return value.toBuffer();
};

/**
 * @param {String|LocalDate} value
 * @returns {Buffer}
 * @throws {TypeError}
 */
Encoder.prototype.encodeTime = function (value) {
  var originalValue = value;
  try {
    if (typeof value === 'string') {
      value = types.LocalTime.fromString(value);
    }
  }
  catch (err) {
    //Wrap into a TypeError
    throw new TypeError('LocalTime could not be parsed ' + err);
  }
  if (!(value instanceof types.LocalTime)) {
    throw new TypeError('Expected String/LocalTime, obtained ' + util.inspect(originalValue));
  }
  return value.toBuffer();
};

/**
 * @param {Uuid|String|Buffer} value
 */
Encoder.prototype.encodeUuid = function (value) {
  if (typeof value === 'string') {
    try {
      value = types.Uuid.fromString(value);
    }
    catch (err) {
      throw new TypeError(err.message);
    }
  }
  if (value instanceof types.Uuid) {
    value = value.getBuffer();
  }
  if (!(value instanceof Buffer)) {
    throw new TypeError('Not a valid Uuid, expected Uuid/String/Buffer, obtained ' + util.inspect(value));
  }
  return value;
};

/**
 * @param {String|InetAddress|Buffer} value
 * @returns {Buffer}
 */
Encoder.prototype.encodeInet = function (value) {
  if (typeof value === 'string') {
    value = types.InetAddress.fromString(value);
  }
  if (value instanceof types.InetAddress) {
    value = value.getBuffer();
  }
  if (!(value instanceof Buffer)) {
    throw new TypeError('Not a valid Inet, expected InetAddress/Buffer, obtained ' + util.inspect(value));
  }
  return value;
};

/**
 * @param {Long|Buffer|String|Number} value
 */
Encoder.prototype.encodeLong = function (value) {
  if (typeof value === 'number') {
    value = Long.fromNumber(value);
  }
  if (typeof value === 'string') {
    value = Long.fromString(value);
  }
  var buf = null;
  if (value instanceof Buffer) {
    buf = value;
  }
  if (value instanceof Long) {
    //noinspection JSCheckFunctionSignatures
    buf = Long.toBuffer(value);
  }
  if (buf === null) {
    throw new TypeError('Not a valid bigint, expected Long/Number/String/Buffer, obtained ' + util.inspect(value));
  }
  return buf;
};

/**
 * @param {Integer|Buffer|String|Number} value
 * @returns {Buffer}
 */
Encoder.prototype.encodeVarint = function (value) {
  if (typeof value === 'number') {
    value = Integer.fromNumber(value);
  }
  if (typeof value === 'string') {
    value = Integer.fromString(value);
  }
  var buf = null;
  if (value instanceof Buffer) {
    buf = value;
  }
  if (value instanceof Integer) {
    buf = Integer.toBuffer(value);
  }
  if (buf === null) {
    throw new TypeError('Not a valid varint, expected Integer/Number/String/Buffer, obtained ' + util.inspect(value));
  }
  return buf;
};

/**
 * @param {BigDecimal|Buffer|String|Number} value
 * @returns {Buffer}
 */
Encoder.prototype.encodeDecimal = function (value) {
  if (typeof value === 'number') {
    value = BigDecimal.fromNumber(value);
  }
  if (typeof value === 'string') {
    value = BigDecimal.fromString(value);
  }
  var buf = null;
  if (value instanceof Buffer) {
    buf = value;
  }
  if (value instanceof BigDecimal) {
    buf = BigDecimal.toBuffer(value);
  }
  if (buf === null) {
    throw new TypeError('Not a valid varint, expected BigDecimal/Number/String/Buffer, obtained ' + util.inspect(value));
  }
  return buf;
};

Encoder.prototype.encodeString = function (value, encoding) {
  if (typeof value !== 'string') {
    throw new TypeError('Not a valid text value, expected String obtained ' + util.inspect(value));
  }
  return new Buffer(value, encoding);
};

Encoder.prototype.encodeUtf8String = function (value) {
  return this.encodeString(value, 'utf8');
};

Encoder.prototype.encodeAsciiString = function (value) {
  return this.encodeString(value, 'ascii');
};

Encoder.prototype.encodeBlob = function (value) {
  if (!(value instanceof Buffer)) {
    throw new TypeError('Not a valid blob, expected Buffer obtained ' + util.inspect(value));
  }
  return value;
};

/**
 * @param {Boolean} value
 * @returns {Buffer}
 */
Encoder.prototype.encodeBoolean = function (value) {
  return new Buffer([(value ? 1 : 0)]);
};

/** @param {Number|String} value */
Encoder.prototype.encodeInt = function (value) {
  if (isNaN(value)) {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(4);
  buf.writeInt32BE(value, 0);
  return buf;
};

/** @param {Number|String} value */
Encoder.prototype.encodeSmallint = function (value) {
  if (isNaN(value)) {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(2);
  buf.writeInt16BE(value, 0);
  return buf;
};

/** @param {Number|String} value */
Encoder.prototype.encodeTinyint = function (value) {
  if (isNaN(value)) {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(1);
  buf.writeInt8(value, 0);
  return buf;
};

Encoder.prototype.encodeList = function (value, subtype) {
  if (!util.isArray(value)) {
    throw new TypeError('Not a valid list value, expected Array obtained ' + util.inspect(value));
  }
  if (value.length === 0) {
    return null;
  }
  var parts = [];
  parts.push(this.getLengthBuffer(value));
  for (var i=0;i < value.length;i++) {
    var bytes = this.encode(value[i], subtype);
    //include item byte length
    parts.push(this.getLengthBuffer(bytes));
    //include item
    parts.push(bytes);
  }
  return Buffer.concat(parts);
};

Encoder.prototype.encodeSet = function (value, subtype) {
  if (this.encodingOptions.set && value instanceof this.encodingOptions.set) {
    var arr = [];
    value.forEach(function (x) {
      arr.push(x);
    });
    return this.encodeList(arr, subtype);
  }
  return this.encodeList(value, subtype);
};

/**
 * Serializes a map into a Buffer
 * @param value
 * @param {Array} [subtypes]
 * @returns {Buffer}
 */
Encoder.prototype.encodeMap = function (value, subtypes) {
  var parts = [];
  var propCounter = 0;
  var keySubtype = null;
  var valueSubtype = null;
  var self = this;
  if (subtypes) {
    keySubtype = subtypes[0];
    valueSubtype = subtypes[1];
  }
  function addItem(val, key) {
    var keyBuffer = self.encode(key, keySubtype);
    //include item byte length
    parts.push(self.getLengthBuffer(keyBuffer));
    //include item
    parts.push(keyBuffer);
    //value
    var valueBuffer = self.encode(val, valueSubtype);
    //include item byte length
    parts.push(self.getLengthBuffer(valueBuffer));
    //include item
    if (valueBuffer !== null) {
      parts.push(valueBuffer);
    }
    propCounter++;
  }
  if (this.encodingOptions.map && value instanceof this.encodingOptions.map) {
    //Use Map#forEach() method to iterate
    value.forEach(addItem);
  }
  else {
    //Use object
    for (var key in value) {
      if (!value.hasOwnProperty(key)) continue;
      var val = value[key];
      addItem(val, key);
    }
  }

  parts.unshift(this.getLengthBuffer(propCounter));
  return Buffer.concat(parts);
};

Encoder.prototype.encodeUdt = function (value, udtInfo) {
  var parts = [];
  var totalLength = 0;
  for (var i = 0; i < udtInfo.fields.length; i++) {
    var field = udtInfo.fields[i];
    var item = this.encode(value[field.name], field.type);
    if (!item) {
      parts.push(nullValueBuffer);
      totalLength += 4;
      continue;
    }
    var lengthBuffer = new Buffer(4);
    lengthBuffer.writeInt32BE(item.length, 0);
    parts.push(lengthBuffer);
    parts.push(item);
    totalLength += item.length + 4;
  }
  return Buffer.concat(parts, totalLength);
};

Encoder.prototype.encodeTuple = function (value, tupleInfo) {
  var parts = [];
  var totalLength = 0;
  for (var i = 0; i < tupleInfo.length; i++) {
    var type = tupleInfo[i];
    var item = this.encode(value.get(i), type);
    if (!item) {
      parts.push(nullValueBuffer);
      totalLength += 4;
      continue;
    }
    var lengthBuffer = new Buffer(4);
    lengthBuffer.writeInt32BE(item.length, 0);
    parts.push(lengthBuffer);
    parts.push(item);
    totalLength += item.length + 4;
  }
  return Buffer.concat(parts, totalLength);
};

/**
 * Gets a buffer containing with the bytes (BE) representing the collection length for protocol v2 and below
 * @param {Buffer|Number} value
 * @returns {Buffer}
 */
Encoder.prototype.getLengthBufferV2 = function (value) {
  if (!value) {
    return int16Zero;
  }
  var lengthBuffer = new Buffer(2);
  if (typeof value === 'number') {
    lengthBuffer.writeUInt16BE(value, 0);
  }
  else {
    lengthBuffer.writeUInt16BE(value.length, 0);
  }
  return lengthBuffer;
};

/**
 * Gets a buffer containing with the bytes (BE) representing the collection length for protocol v3 and above
 * @param {Buffer|Number} value
 * @returns {Buffer}
 */
Encoder.prototype.getLengthBufferV3 = function (value) {
  if (!value) {
    return int32Zero;
  }
  var lengthBuffer = new Buffer(4);
  if (typeof value === 'number') {
    lengthBuffer.writeUInt32BE(value, 0);
  }
  else {
    lengthBuffer.writeUInt32BE(value.length, 0);
  }
  return lengthBuffer;
};

Encoder.prototype.handleBufferCopy = function (buffer) {
  if (buffer === null) {
    return null;
  }
  return utils.copyBuffer(buffer);
};

Encoder.prototype.handleBufferRef = function (buffer) {
  return buffer;
};

/**
 * Parses a given Cassandra type name to get the data type information
 * @param {String} typeName
 * @param {Number} [startIndex]
 * @param {Number} [length]
 * @throws TypeError
 * @returns {{code: number, info: Object|Array|null, options: {frozen: Boolean, reversed: Boolean}}}
 */
Encoder.prototype.parseTypeName = function (typeName, startIndex, length) {
  var dataType = {
    code: 0,
    info: null,
    options: {
      reversed: false,
      frozen: false
    }
  };
  startIndex = startIndex || 0;
  var innerTypes;
  if (!length) {
    length = typeName.length;
  }
  if (length > complexTypeNames.reversed.length && typeName.substr(startIndex, complexTypeNames.reversed.length) === complexTypeNames.reversed) {
    //Remove the reversed token
    startIndex += complexTypeNames.reversed.length + 1;
    length -= complexTypeNames.reversed.length + 2;
    dataType.options.reversed = true;
  }
  if (length > complexTypeNames.frozen.length && typeName.substr(startIndex, complexTypeNames.frozen.length) == complexTypeNames.frozen) {
    //Remove the frozen token
    startIndex += complexTypeNames.frozen.length + 1;
    length -= complexTypeNames.frozen.length + 2;
    dataType.options.frozen = true;
  }
  //Quick check if its a single type
  if (length <= singleTypeNamesLength) {
    if (startIndex > 0) {
      typeName = typeName.substr(startIndex, length);
    }
    var typeCode = singleTypeNames[typeName];
    if (typeof typeCode === 'number') {
      dataType.code = typeCode;
      return dataType;
    }
    throw new TypeError('Not a valid type ' + typeName);
  }
  if (typeName.substr(startIndex, complexTypeNames.list.length) === complexTypeNames.list) {
    //Its a list
    //org.apache.cassandra.db.marshal.ListType(innerType)
    //move cursor across the name and bypass the parenthesis
    startIndex += complexTypeNames.list.length + 1;
    length -= complexTypeNames.list.length + 2;
    innerTypes = parseParams(typeName, startIndex, length);
    if (innerTypes.length != 1) {
      throw new TypeError('Not a valid type ' + typeName);
    }
    dataType.code = dataTypes.list;
    dataType.info = this.parseTypeName(innerTypes[0]);
    return dataType;
  }
  if (typeName.substr(startIndex, complexTypeNames.set.length) === complexTypeNames.set) {
    //Its a set
    //org.apache.cassandra.db.marshal.SetType(innerType)
    //move cursor across the name and bypass the parenthesis
    startIndex += complexTypeNames.set.length + 1;
    length -= complexTypeNames.set.length + 2;
    innerTypes = parseParams(typeName, startIndex, length);
    if (innerTypes.length != 1)
    {
      throw new TypeError('Not a valid type ' + typeName);
    }
    dataType.code = dataTypes.set;
    dataType.info = this.parseTypeName(innerTypes[0]);
    return dataType;
  }
  if (typeName.substr(startIndex, complexTypeNames.map.length) === complexTypeNames.map) {
    //org.apache.cassandra.db.marshal.MapType(keyType,valueType)
    //move cursor across the name and bypass the parenthesis
    startIndex += complexTypeNames.map.length + 1;
    length -= complexTypeNames.map.length + 2;
    innerTypes = parseParams(typeName, startIndex, length);
    //It should contain the key and value types
    if (innerTypes.length != 2) {
      throw new TypeError('Not a valid type ' + typeName);
    }
    dataType.code = dataTypes.map;
    dataType.info = [this.parseTypeName(innerTypes[0]), this.parseTypeName(innerTypes[1])];
    return dataType;
  }
  if (typeName.substr(startIndex, complexTypeNames.udt.length) === complexTypeNames.udt) {
    //move cursor across the name and bypass the parenthesis
    startIndex += complexTypeNames.udt.length + 1;
    length -= complexTypeNames.udt.length + 2;
    return this._parseUdtName(typeName, startIndex, length);
  }
  if (typeName.substr(startIndex, complexTypeNames.tuple.length) === complexTypeNames.tuple) {
    //move cursor across the name and bypass the parenthesis
    startIndex += complexTypeNames.tuple.length + 1;
    length -= complexTypeNames.tuple.length + 2;
    innerTypes = parseParams(typeName, startIndex, length);
    if (innerTypes.length < 1) {
      throw new TypeError('Not a valid type ' + typeName);
    }
    dataType.code = dataTypes.tuple;
    dataType.info = innerTypes.map(function (x) {
      return this.parseTypeName(x);
    }, this);
    return dataType;
  }
  throw new TypeError('Not a valid type ' + typeName);
};

/**
 * Parses type names with composites
 * @param {String} typesString
 * @returns {{types: Array, isComposite: Boolean, hasCollections: Boolean}}
 */
Encoder.prototype.parseKeyTypes = function (typesString) {
  var i = 0;
  var length = typesString.length;
  var isComposite = typesString.indexOf(complexTypeNames.composite) === 0;
  if (isComposite) {
    i = complexTypeNames.composite.length + 1;
    length--;
  }
  var types = [];
  var startIndex = i;
  var nested = 0;
  var inCollectionType = false;
  var hasCollections = false;
  //as collection types are not allowed, it is safe to split by ,
  while (++i < length) {
    switch (typesString[i]) {
      case ',':
        if (nested > 0) break;
        if (inCollectionType) {
          //remove type id
          startIndex = typesString.indexOf(':', startIndex) + 1;
        }
        types.push(typesString.substring(startIndex, i));
        startIndex = i + 1;
        break;
      case '(':
        if (nested === 0 && typesString.indexOf(complexTypeNames.collection, startIndex) === startIndex) {
          inCollectionType = true;
          hasCollections = true;
          //skip collection type
          i++;
          startIndex = i;
          break;
        }
        nested++;
        break;
      case ')':
        if (inCollectionType && nested === 0){
          types.push(typesString.substring(typesString.indexOf(':', startIndex) + 1, i));
          startIndex = i + 1;
          break;
        }
        nested--;
        break;
    }
  }
  if (startIndex < length) {
    types.push(typesString.substring(startIndex, length));
  }
  return {
    types: types.map(function (name) {
      return this.parseTypeName(name);
    }, this),
    hasCollections: hasCollections,
    isComposite: isComposite
  };
}


Encoder.prototype._parseUdtName = function (typeName, startIndex, length) {
  var udtParams = parseParams(typeName, startIndex, length);
  if (udtParams.length < 2) {
    //It should contain at least the keyspace, name of the udt and a type
    throw new TypeError('Not a valid type ' + typeName);
  }
  var dataType = {
    code: dataTypes.udt,
    info: null
  };
  var udtInfo = {
    keyspace: udtParams[0],
    name: new Buffer(udtParams[1], 'hex').toString(),
    fields: []
  };
  for (var i = 2; i < udtParams.length; i++) {
    var p = udtParams[i];
    var separatorIndex = p.indexOf(':');
    var fieldType = this.parseTypeName(p, separatorIndex + 1, p.length - (separatorIndex + 1));
    udtInfo.fields.push({
      name: new Buffer(p.substr(0, separatorIndex), 'hex').toString(),
      type: fieldType
    });
  }
  dataType.info = udtInfo;
  return dataType;
};

/**
 * @param {String} value
 * @param {Number} startIndex
 * @param {Number} length
 * @returns {Array}
 */
function parseParams(value, startIndex, length) {
  var types = [];
  var paramStart = startIndex;
  var level = 0;
  for (var i = startIndex; i < startIndex + length; i++) {
    var c = value[i];
    if (c == '(') {
      level++;
    }
    if (c == ')') {
      level--;
    }
    if (level == 0 && c == ',') {
      types.push(value.substr(paramStart, i - paramStart));
      paramStart = i + 1;
    }
  }
  //Add the last one
  types.push(value.substr(paramStart, length - (paramStart - startIndex)));
  return types;
}

module.exports = Encoder;
