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

/**
 * Encodes and decodes from a type to Cassandra bytes
 * @param {Number} protocolVersion
 * @param {ClientOptions} options
 * @constructor
 */
function Encoder(protocolVersion, options) {
  this.protocolVersion = protocolVersion;
  this.encodingOptions = options.encoding || utils.emptyObject;
}

/**
 * Decodes Cassandra bytes into Javascript values.
 * @param {Buffer} bytes
 * @param {Array} type
 */
Encoder.prototype.decode = function (bytes, type) {
  if (bytes === null) {
    return null;
  }
  switch(type[0]) {
    case dataTypes.custom:
      return utils.copyBuffer(bytes);
    case dataTypes.inet:
      return this.decodeInet(bytes);
    case dataTypes.varint:
      return this.decodeVarint(bytes);
    case dataTypes.decimal:
      return this.decodeDecimal(bytes);
    case dataTypes.ascii:
      return bytes.toString('ascii');
    case dataTypes.bigint:
    case dataTypes.counter:
      return this.decodeBigNumber(bytes);
    case dataTypes.timestamp:
      return this.decodeTimestamp(bytes);
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
      return this.decodeUuid(bytes);
    case dataTypes.timeuuid:
      return this.decodeTimeUuid(bytes);
    case dataTypes.text:
    case dataTypes.varchar:
      return bytes.toString('utf8');
    case dataTypes.list:
      return this.decodeList(bytes, type[1][0]);
    case dataTypes.set:
      return this.decodeSet(bytes, type[1][0]);
    case dataTypes.map:
      return this.decodeMap(bytes, type[1][0][0], type[1][1][0]);
  }

  throw new Error('Unknown data type: ' + type[0]);
};

/**
 * @param value
 * @param {{type: number, subtypes: Array}|String|Number} [typeInfo]
 * @returns {Buffer}
 * @throws {TypeError} When there is an encoding error
 */
Encoder.prototype.encode = function (value, typeInfo) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeInfo) {
    if (typeof typeInfo === 'number') {
      //noinspection JSValidateTypes
      typeInfo = {type: typeInfo};
    }
    else if (typeof typeInfo === 'string') {
      typeInfo = dataTypes.getByName(typeInfo);
    }
    if (typeof typeInfo.type !== 'number') {
      throw new TypeError('Type information not valid, only String and Number values are valid hints');
    }
  }
  else {
    //Lets guess
    //noinspection JSValidateTypes
    typeInfo = this.guessDataType(value);
    if (!typeInfo) {
      throw new TypeError('Target data type could not be guessed, you should use prepared statements for accurate type mapping. Value: ' + util.inspect(value));
    }
  }
  switch (typeInfo.type) {
    case dataTypes.int:
      return this.encodeInt(value);
    case dataTypes.float:
      return this.encodeFloat(value);
    case dataTypes.double:
      return this.encodeDouble(value);
    case dataTypes.boolean:
      return this.encodeBoolean(value);
    case dataTypes.text:
    case dataTypes.varchar:
      return this.encodeString(value);
    case dataTypes.ascii:
      return this.encodeString(value, 'ascii');
    case dataTypes.uuid:
    case dataTypes.timeuuid:
      return this.encodeUuid(value);
    case dataTypes.custom:
    case dataTypes.blob:
      return this.encodeBlob(value, typeInfo.type);
    case dataTypes.inet:
      return this.encodeInet(value);
    case dataTypes.bigint:
    case dataTypes.counter:
      return this.encodeBigNumber(value);
    case dataTypes.timestamp:
      return this.encodeTimestamp(value);
    case dataTypes.varint:
      return this.encodeVarint(value);
    case dataTypes.decimal:
      return this.encodeDecimal(value);
    case dataTypes.list:
      return this.encodeList(value, typeInfo);
    case dataTypes.set:
      return this.encodeSet(value, typeInfo);
    case dataTypes.map:
      return this.encodeMap(value, typeInfo);
    default:
      throw new TypeError('Type not supported ' + typeInfo.type);
  }
};

/**
 * Try to guess the Cassandra type to be stored, based on the javascript value type
 * @param value
 * @returns {{type: number}}
 */
Encoder.prototype.guessDataType = function (value) {
  var type = null;
  if (typeof value === 'number') {
    type = dataTypes.double;
  }
  else if (typeof value === 'string') {
    type = dataTypes.text;
    if (value.length === 36 && uuidRegex.test(value)){
      type = dataTypes.uuid;
    }
  }
  else if (value instanceof Buffer) {
    type = dataTypes.blob;
  }
  else if (value instanceof Date) {
    type = dataTypes.timestamp;
  }
  else if (value instanceof Long) {
    type = dataTypes.bigint;
  }
  else if (value instanceof Integer) {
    type = dataTypes.varint;
  }
  else if (value instanceof BigDecimal) {
    type = dataTypes.decimal;
  }
  else if (value instanceof types.Uuid) {
    type = dataTypes.uuid;
  }
  else if (value instanceof types.InetAddress) {
    type = dataTypes.inet;
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
};

/**
 * If not provided, it uses the array of buffers or the parameters and hints to build the routingKey
 * @param {Array} params
 * @param {QueryOptions} options
 * @param [keys] parameter keys and positions
 * @throws TypeError
 */
Encoder.prototype.setRoutingKey = function (params, options, keys) {
  if (util.isArray(options.routingKey)) {
    if (options.routingKey.length === 1) {
      options.routingKey = options.routingKey[0];
      return;
    }
    //Is a Composite key
    var totalLength = 0;
    options.routingKey.forEach(function (item) {
      totalLength += 2 + item.length + 1;
    });

    var routingKey = new Buffer(totalLength);
    var index = 0;
    options.routingKey.forEach(function (item) {
      routingKey.writeInt16BE(item.length, index);
      index += 2;
      item.copy(routingKey, index);
      index += item.length;
      routingKey[index] = 0;
      index++;
    });
    //Set the buffer containing the contents of the previous Array of buffers as routing key
    options.routingKey = routingKey;
    return;
  }
  if (options.routingKey instanceof Buffer ||
      (!util.isArray(options.routingIndexes) && !util.isArray(options.routingNames)) ||
      !params ||
      params.length === 0) {
    //There is already a routing key
    // or no parameter indexes for routing were provided
    // or there are no parameters to build the routing key
    return;
  }
  var routingKeys = [];
  var hints = options.hints;
  if (!hints) {
    hints = [];
  }
  if (options.routingNames && keys) {
    options.routingNames.forEach(function (name) {
      var paramIndex = keys[name];
      routingKeys.push(this.encode(params[paramIndex], hints[paramIndex]));
    }, this);
  }
  else if (options.routingIndexes) {
    options.routingIndexes.forEach(function (paramIndex) {
      routingKeys.push(this.encode(params[paramIndex], hints[paramIndex]));
    }, this);
  }
  if (routingKeys.length === 0) {
    return;
  }
  if (routingKeys.length === 1) {
    options.routingKey = routingKeys[0];
    return;
  }
  //Its a composite routing key
  options.routingKey = routingKeys;
  this.setRoutingKey(params, options);
};

/**
 * Sets the protocol version for this instance
 * @param {Number} value
 */
Encoder.prototype.setProtocolVersion = function (value) {
  this.protocolVersion = value;
};

Encoder.prototype.decodeBigNumber = function (bytes) {
  return Long.fromBuffer(bytes);
};

Encoder.prototype.decodeVarint = function (bytes) {
  return Integer.fromBuffer(bytes);
};

Encoder.prototype.decodeDecimal = function(bytes) {
  return BigDecimal.fromBuffer(bytes);
};

Encoder.prototype.decodeTimestamp = function(bytes) {
  return new Date(this.decodeBigNumber(bytes).toNumber());
};

/*
 * Reads a list from bytes
 */
Encoder.prototype.decodeList = function (bytes, type) {
  var offset = 0;
  //a short containing the total items
  var totalItems = bytes.readUInt16BE(offset);
  offset += 2;
  var list = new Array(totalItems);
  for(var i = 0; i < totalItems; i++) {
    //bytes length of the item
    var length = bytes.readUInt16BE(offset);
    offset += 2;
    //slice it
    list[i] = this.decode(bytes.slice(offset, offset+length), [type]);
    offset += length;
  }
  return list;
};

/*
 * Reads a Set from bytes
 */
Encoder.prototype.decodeSet = function (bytes, type) {
  var arr = this.decodeList(bytes, type);
  if (this.encodingOptions.set) {
    var setConstructor = this.encodingOptions.set;
    return new setConstructor(arr);
  }
  return arr;
};

/*
 * Reads a map (key / value) from bytes
 */
Encoder.prototype.decodeMap = function (bytes, type1, type2) {
  var map;
  var offset = 0;
  //a short containing the total items
  var totalItems = bytes.readUInt16BE(offset);
  offset += 2;
  var self = this;
  function readValues(callback, thisArg) {
    for(var i = 0; i < totalItems; i++) {
      var keyLength = bytes.readUInt16BE(offset);
      offset += 2;
      var key = self.decode(bytes.slice(offset, offset + keyLength), [type1]);
      offset += keyLength;
      var valueLength = bytes.readUInt16BE(offset);
      offset += 2;
      var value = self.decode(bytes.slice(offset, offset + valueLength), [type2]);
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

Encoder.prototype.decodeUuid = function (bytes) {
  return new types.Uuid(utils.copyBuffer(bytes));
};

Encoder.prototype.decodeTimeUuid = function (bytes) {
  return new types.TimeUuid(utils.copyBuffer(bytes));
};

Encoder.prototype.decodeInet = function (bytes) {
  return new types.InetAddress(utils.copyBuffer(bytes));
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
  return this.encodeBigNumber(value);
};

/**
 * @param {Uuid|String|Buffer} value
 */
Encoder.prototype.encodeUuid = function (value) {
  if (typeof value === 'string') {
    value = types.Uuid.fromString(value);
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
Encoder.prototype.encodeBigNumber = function (value) {
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

/**
 * @param {Number|String} value
 */
Encoder.prototype.encodeInt = function (value) {
  if (isNaN(value)) {
    throw new TypeError('Expected Number, obtained ' + util.inspect(value));
  }
  var buf = new Buffer(4);
  buf.writeInt32BE(value, 0);
  return buf;
};

Encoder.prototype.encodeList = function (value, typeInfo) {
  if (!util.isArray(value)) {
    throw new TypeError('Not a valid list value, expected Array obtained ' + util.inspect(value));
  }
  if (value.length === 0) {
    return null;
  }
  var parts = [];
  parts.push(this.getLengthBuffer(value));
  var subtype = typeInfo.subtypes ? typeInfo.subtypes[0] : null;
  for (var i=0;i < value.length;i++) {
    var bytes = this.encode(value[i], subtype);
    //include item byte length
    parts.push(this.getLengthBuffer(bytes));
    //include item
    parts.push(bytes);
  }
  return Buffer.concat(parts);
};

Encoder.prototype.encodeSet = function (value, typeInfo) {
  if (this.encodingOptions.set && value instanceof this.encodingOptions.set) {
    var arr = [];
    value.forEach(function (x) {
      arr.push(x);
    });
    return this.encodeList(arr, typeInfo);
  }
  return this.encodeList(value, typeInfo);
};

/**
 * Serializes a map into a Buffer
 * @param value
 * @param {{type: number, subtypes: Array}} [typeInfo]
 * @returns {Buffer}
 */
Encoder.prototype.encodeMap = function (value, typeInfo) {
  var parts = [];
  var propCounter = 0;
  var keySubtype = null;
  var valueSubtype = null;
  var self = this;
  if (typeInfo.subtypes) {
    keySubtype = typeInfo.subtypes[0];
    valueSubtype = typeInfo.subtypes[1];
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

/**
 * Gets a buffer containing with 2 bytes (BE) representing the array length or the value
 * @param {Buffer|Number} value
 * @returns {Buffer}
 */
Encoder.prototype.getLengthBuffer = function (value) {
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
};

module.exports = Encoder;
