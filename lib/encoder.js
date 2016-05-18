"use strict";
var util = require('util');

var types = require('./types');
var dataTypes = types.dataTypes;
var Long = types.Long;
var Integer = types.Integer;
var BigDecimal = types.BigDecimal;
var utils = require('./utils');

var uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
var int16Zero = new Buffer([0, 0]);
var int32Zero = new Buffer([0, 0, 0, 0]);
var complexTypeNames = Object.freeze({
  list      : 'org.apache.cassandra.db.marshal.ListType',
  set       : 'org.apache.cassandra.db.marshal.SetType',
  map       : 'org.apache.cassandra.db.marshal.MapType',
  udt       : 'org.apache.cassandra.db.marshal.UserType',
  tuple     : 'org.apache.cassandra.db.marshal.TupleType',
  frozen    : 'org.apache.cassandra.db.marshal.FrozenType',
  reversed  : 'org.apache.cassandra.db.marshal.ReversedType',
  composite : 'org.apache.cassandra.db.marshal.CompositeType',
  empty     : 'org.apache.cassandra.db.marshal.EmptyType',
  collection: 'org.apache.cassandra.db.marshal.ColumnToCollectionType'
});
var cqlNames = Object.freeze({
  frozen: 'frozen',
  list: 'list',
  'set': 'set',
  map: 'map',
  tuple: 'tuple',
  empty: 'empty'
});
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
var singleFqTypeNamesLength = Object.keys(singleTypeNames).reduce(function (previous, current) {
  return current.length > previous ? current.length : previous;
}, 0);
var nullValueBuffer = new Buffer([255, 255, 255, 255]);
var unsetValueBuffer = new Buffer([255, 255, 255, 254]);

/**
 * Serializes and deserializes to and from a CQL type and a Javascript Type.
 * @param {Number} protocolVersion
 * @param {ClientOptions} options
 * @constructor
 */
function Encoder(protocolVersion, options) {
  this.encodingOptions = options.encoding || utils.emptyObject;
  defineInstanceMembers.call(this);
  this.setProtocolVersion(protocolVersion);
  setEncoders.call(this);
  if (this.encodingOptions.copyBuffer) {
    this.handleBuffer = handleBufferCopy;
  }
  else {
    this.handleBuffer = handleBufferRef;
  }
}

/**
 * Declares the privileged instance members.
 * @private
 */
function defineInstanceMembers() {
  /**
   * Sets the protocol version and the encoding/decoding methods depending on the protocol version
   * @param {Number} value
   * @ignore
   * @internal
   */
  this.setProtocolVersion = function (value) {
    this.protocolVersion = value;
    //Set the collection serialization based on the protocol version
    this.decodeCollectionLength = decodeCollectionLengthV3;
    this.getLengthBuffer = getLengthBufferV3;
    this.collectionLengthSize = 4;
    if (this.protocolVersion < 3) {
      this.decodeCollectionLength = decodeCollectionLengthV2;
      this.getLengthBuffer = getLengthBufferV2;
      this.collectionLengthSize = 2;
    }
  };
  //Decoding methods
  this.decodeBlob = function (bytes) {
    return this.handleBuffer(bytes);
  };
  this.decodeUtf8String = function (bytes) {
    return bytes.toString('utf8');
  };
  this.decodeAsciiString = function (bytes) {
    return bytes.toString('ascii');
  };
  this.decodeBoolean = function (bytes) {
    return !!bytes.readUInt8(0);
  };
  this.decodeDouble = function (bytes) {
    return bytes.readDoubleBE(0);
  };
  this.decodeFloat = function (bytes) {
    return bytes.readFloatBE(0);
  };
  this.decodeInt = function (bytes) {
    return bytes.readInt32BE(0);
  };
  this.decodeSmallint = function (bytes) {
    return bytes.readInt16BE(0);
  };
  this.decodeTinyint = function (bytes) {
    return bytes.readInt8(0);
  };
  this.decodeLong = function (bytes) {
    return Long.fromBuffer(bytes);
  };
  this.decodeVarint = function (bytes) {
    return Integer.fromBuffer(bytes);
  };
  this.decodeDecimal = function(bytes) {
    return BigDecimal.fromBuffer(bytes);
  };
  this.decodeTimestamp = function(bytes) {
    return new Date(this.decodeLong(bytes).toNumber());
  };
  this.decodeDate = function (bytes) {
    return types.LocalDate.fromBuffer(bytes);
  };
  this.decodeTime = function (bytes) {
    return types.LocalTime.fromBuffer(bytes);
  };
  /*
   * Reads a list from bytes
   */
  this.decodeList = function (bytes, subtype) {
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
  this.decodeSet = function (bytes, subtype) {
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
  this.decodeMap = function (bytes, subtypes) {
    var map;
    var totalItems = this.decodeCollectionLength(bytes, 0);
    var offset = this.collectionLengthSize;
    var self = this;
    function readValues(callback, thisArg) {
      for (var i = 0; i < totalItems; i++) {
        var keyLength = self.decodeCollectionLength(bytes, offset);
        offset += self.collectionLengthSize;
        var key = self.decode(bytes.slice(offset, offset + keyLength), subtypes[0]);
        offset += keyLength;
        var valueLength = self.decodeCollectionLength(bytes, offset);
        offset += self.collectionLengthSize;
        if (valueLength <= 0) {
          callback.call(thisArg, key, null);
          continue;
        }
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
  this.decodeUuid = function (bytes) {
    return new types.Uuid(this.handleBuffer(bytes));
  };
  this.decodeTimeUuid = function (bytes) {
    return new types.TimeUuid(this.handleBuffer(bytes));
  };
  this.decodeInet = function (bytes) {
    return new types.InetAddress(this.handleBuffer(bytes));
  };
  /**
   * Decodes a user defined type into an object
   * @param {Buffer} bytes
   * @param {{fields: Array}} udtInfo
   * @private
   */
  this.decodeUdt = function (bytes, udtInfo) {
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
  this.decodeTuple = function (bytes, tupleInfo) {
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
  //Encoding methods
  this.encodeFloat = function (value) {
    if (typeof value !== 'number') {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    var buf = new Buffer(4);
    buf.writeFloatBE(value, 0);
    return buf;
  };
  this.encodeDouble = function (value) {
    if (typeof value !== 'number') {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    var buf = new Buffer(8);
    buf.writeDoubleBE(value, 0);
    return buf;
  };
  /**
   * @param {Date|String|Long|Number} value
   * @private
   */
  this.encodeTimestamp = function (value) {
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
   * @private
   */
  this.encodeDate = function (value) {
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
   * @private
   */
  this.encodeTime = function (value) {
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
   * @private
   */
  this.encodeUuid = function (value) {
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
   * @private
   */
  this.encodeInet = function (value) {
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
   * @private
   */
  this.encodeLong = function (value) {
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
   * @private
   */
  this.encodeVarint = function (value) {
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
   * @private
   */
  this.encodeDecimal = function (value) {
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
  this.encodeString = function (value, encoding) {
    if (typeof value !== 'string') {
      throw new TypeError('Not a valid text value, expected String obtained ' + util.inspect(value));
    }
    return new Buffer(value, encoding);
  };
  this.encodeUtf8String = function (value) {
    return this.encodeString(value, 'utf8');
  };
  this.encodeAsciiString = function (value) {
    return this.encodeString(value, 'ascii');
  };
  this.encodeBlob = function (value) {
    if (!(value instanceof Buffer)) {
      throw new TypeError('Not a valid blob, expected Buffer obtained ' + util.inspect(value));
    }
    return value;
  };
  /**
   * @param {Boolean} value
   * @returns {Buffer}
   * @private
   */
  this.encodeBoolean = function (value) {
    return new Buffer([(value ? 1 : 0)]);
  };
  /**
   * @param {Number|String} value
   * @private
   */
  this.encodeInt = function (value) {
    if (isNaN(value)) {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    var buf = new Buffer(4);
    buf.writeInt32BE(value, 0);
    return buf;
  };
  /**
   * @param {Number|String} value
   * @private
   */
  this.encodeSmallint = function (value) {
    if (isNaN(value)) {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    var buf = new Buffer(2);
    buf.writeInt16BE(value, 0);
    return buf;
  };
  /**
   * @param {Number|String} value
   * @private
   */
  this.encodeTinyint = function (value) {
    if (isNaN(value)) {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    var buf = new Buffer(1);
    buf.writeInt8(value, 0);
    return buf;
  };
  this.encodeList = function (value, subtype) {
    if (!util.isArray(value)) {
      throw new TypeError('Not a valid list value, expected Array obtained ' + util.inspect(value));
    }
    if (value.length === 0) {
      return null;
    }
    var parts = [];
    parts.push(this.getLengthBuffer(value));
    for (var i=0;i < value.length;i++) {
      var val = value[i];
      if (val === null || typeof val === 'undefined' || val === types.unset) {
        throw new TypeError('A collection can\'t contain null or unset values');
      }
      var bytes = this.encode(val, subtype);
      //include item byte length
      parts.push(this.getLengthBuffer(bytes));
      //include item
      parts.push(bytes);
    }
    return Buffer.concat(parts);
  };
  this.encodeSet = function (value, subtype) {
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
   * @private
   */
  this.encodeMap = function (value, subtypes) {
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
      if (key === null || typeof key === 'undefined' || key === types.unset) {
        throw new TypeError('A map can\'t contain null or unset keys');
      }
      if (val === null || typeof val === 'undefined' || val === types.unset) {
        throw new TypeError('A map can\'t contain null or unset values');
      }
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
  this.encodeUdt = function (value, udtInfo) {
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
      if (item === types.unset) {
        parts.push(unsetValueBuffer);
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
  this.encodeTuple = function (value, tupleInfo) {
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
      if (item === types.unset) {
        parts.push(unsetValueBuffer);
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
   * If not provided, it uses the array of buffers or the parameters and hints to build the routingKey
   * @param {Array} params
   * @param {QueryOptions} options
   * @param [keys] parameter keys and positions
   * @throws TypeError
   * @internal
   * @ignore
   */
  this.setRoutingKey = function (params, options, keys) {
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
      options.routingKey = concatRoutingKey(options.routingKey, totalLength);
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
    options.routingKey = concatRoutingKey(parts, totalLength);
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
  this._encodeRoutingKeyParts = function (parts, routingIndexes, params, hints, keys) {
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
   * Parses a CQL name string into data type information
   * @param {String} keyspace
   * @param {String} typeName
   * @param {Number} startIndex
   * @param {Number|null} length
   * @param {Function} udtResolver
   * @param {Function} callback Callback invoked with err and  {{code: number, info: Object|Array|null, options: {frozen: Boolean}}}
   * @internal
   * @ignore
   */
  this.parseTypeName = function (keyspace, typeName, startIndex, length, udtResolver, callback) {
    startIndex = startIndex || 0;
    if (!length) {
      length = typeName.length;
    }
    var dataType = {
      code: 0,
      info: null,
      options: {
        frozen: false
      }
    };
    var innerTypes;
    if (typeName.indexOf("'", startIndex) === startIndex) {
      //If quoted, this is a custom type.
      dataType.info = typeName.substr(startIndex+1, length-2);
      return callback(null, dataType);
    }
    if (!length) {
      length = typeName.length;
    }
    if (typeName.indexOf(cqlNames.frozen, startIndex) === startIndex) {
      //Remove the frozen token
      startIndex += cqlNames.frozen.length + 1;
      length -= cqlNames.frozen.length + 2;
      dataType.options.frozen = true;
    }
    if (typeName.indexOf(cqlNames.list, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.list.length + 1;
      length -= cqlNames.list.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');
      if (innerTypes.length != 1) {
        return callback(new TypeError('Not a valid type ' + typeName));
      }
      dataType.code = dataTypes.list;
      return this.parseTypeName(keyspace, innerTypes[0], 0, null, udtResolver, function (err, childType) {
        if (err) {
          return callback(err);
        }
        dataType.info = childType;
        callback(null, dataType);
      });
    }
    if (typeName.indexOf(cqlNames.set, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.set.length + 1;
      length -= cqlNames.set.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');
      if (innerTypes.length != 1) {
        return callback(new TypeError('Not a valid type ' + typeName));
      }
      dataType.code = dataTypes.set;
      return this.parseTypeName(keyspace, innerTypes[0], 0, null, udtResolver, function (err, childType) {
        if (err) {
          return callback(err);
        }
        dataType.info = childType;
        callback(null, dataType);
      });
    }
    if (typeName.indexOf(cqlNames.map, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.map.length + 1;
      length -= cqlNames.map.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');
      //It should contain the key and value types
      if (innerTypes.length != 2) {
        return callback(new TypeError('Not a valid type ' + typeName));
      }
      dataType.code = dataTypes.map;
      return this._parseChildTypes(keyspace, dataType, innerTypes, udtResolver, callback);
    }
    if (typeName.indexOf(cqlNames.tuple, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.tuple.length + 1;
      length -= cqlNames.tuple.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');
      if (innerTypes.length < 1) {
        throw new TypeError('Not a valid type ' + typeName);
      }
      dataType.code = dataTypes.tuple;
      return this._parseChildTypes(keyspace, dataType, innerTypes, udtResolver, callback);
    }
    var quoted = typeName.indexOf('"', startIndex) === startIndex;
    if (quoted) {
      //Remove quotes
      startIndex++;
      length -= 2;
    }
    //Quick check if its a single type
    if (startIndex > 0) {
      typeName = typeName.substr(startIndex, length);
    }
    // Un-escape double quotes if quoted.
    if (quoted) {
      typeName = typeName.replace('""', '"');
    }
    var typeCode = dataTypes[typeName];
    if (typeof typeCode === 'number') {
      dataType.code = typeCode;
      return callback(null, dataType);
    }
    if (typeName === cqlNames.empty) {
      //set as custom
      dataType.info = 'empty';
      return callback(null, dataType);
    }
    udtResolver(keyspace, typeName, function (err, udtInfo) {
      if (err) {
        return callback(err);
      }
      if (udtInfo) {
        dataType.code = dataTypes.udt;
        dataType.info = udtInfo;
        return callback(null, dataType);
      }
      callback(new TypeError('Not a valid type "' + typeName + '"'));
    });
  };
  /**
   * @param {String} keyspace
   * @param dataType
   * @param {Array} typeNames
   * @param {Function} udtResolver
   * @param {Function} callback
   * @private
   */
  this._parseChildTypes = function (keyspace, dataType, typeNames, udtResolver, callback) {
    var self = this;
    utils.mapSeries(typeNames, function (name, next) {
      self.parseTypeName(keyspace, name.trim(), 0, null, udtResolver, next);
    }, function (err, childTypes) {
      if (err) {
        return callback(err);
      }
      dataType.info = childTypes;
      callback(null, dataType);
    });
  };

  /**
   * Parses a Cassandra fully-qualified class name string into data type information
   * @param {String} typeName
   * @param {Number} [startIndex]
   * @param {Number} [length]
   * @throws TypeError
   * @returns {{code: number, info: Object|Array|null, options: {frozen: Boolean, reversed: Boolean}}}
   * @internal
   * @ignore
   */
  this.parseFqTypeName = function (typeName, startIndex, length) {
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
    if (length > complexTypeNames.reversed.length && typeName.indexOf(complexTypeNames.reversed) === startIndex) {
      //Remove the reversed token
      startIndex += complexTypeNames.reversed.length + 1;
      length -= complexTypeNames.reversed.length + 2;
      dataType.options.reversed = true;
    }
    if (length > complexTypeNames.frozen.length && typeName.indexOf(complexTypeNames.frozen, startIndex) == startIndex) {
      //Remove the frozen token
      startIndex += complexTypeNames.frozen.length + 1;
      length -= complexTypeNames.frozen.length + 2;
      dataType.options.frozen = true;
    }
    if (typeName === complexTypeNames.empty) {
      //set as custom
      dataType.info = 'empty';
      return dataType;
    }
    //Quick check if its a single type
    if (length <= singleFqTypeNamesLength) {
      if (startIndex > 0) {
        typeName = typeName.substr(startIndex, length);
      }
      var typeCode = singleTypeNames[typeName];
      if (typeof typeCode === 'number') {
        dataType.code = typeCode;
        return dataType;
      }
      throw new TypeError('Not a valid type "' + typeName + '"');
    }
    if (typeName.indexOf(complexTypeNames.list, startIndex) === startIndex) {
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
      dataType.info = this.parseFqTypeName(innerTypes[0]);
      return dataType;
    }
    if (typeName.indexOf(complexTypeNames.set, startIndex) === startIndex) {
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
      dataType.info = this.parseFqTypeName(innerTypes[0]);
      return dataType;
    }
    if (typeName.indexOf(complexTypeNames.map, startIndex) === startIndex) {
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
      dataType.info = [this.parseFqTypeName(innerTypes[0]), this.parseFqTypeName(innerTypes[1])];
      return dataType;
    }
    if (typeName.indexOf(complexTypeNames.udt, startIndex) === startIndex) {
      //move cursor across the name and bypass the parenthesis
      startIndex += complexTypeNames.udt.length + 1;
      length -= complexTypeNames.udt.length + 2;
      return this._parseUdtName(typeName, startIndex, length);
    }
    if (typeName.indexOf(complexTypeNames.tuple, startIndex) === startIndex) {
      //move cursor across the name and bypass the parenthesis
      startIndex += complexTypeNames.tuple.length + 1;
      length -= complexTypeNames.tuple.length + 2;
      innerTypes = parseParams(typeName, startIndex, length);
      if (innerTypes.length < 1) {
        throw new TypeError('Not a valid type ' + typeName);
      }
      dataType.code = dataTypes.tuple;
      dataType.info = innerTypes.map(function (x) {
        return this.parseFqTypeName(x);
      }, this);
      return dataType;
    }

    // Assume custom type if cannot be parsed up to this point.
    dataType.info = typeName.substr(startIndex, length);
    return dataType;
  };
  /**
   * Parses type names with composites
   * @param {String} typesString
   * @returns {{types: Array, isComposite: Boolean, hasCollections: Boolean}}
   * @internal
   * @ignore
   */
  this.parseKeyTypes = function (typesString) {
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
        return this.parseFqTypeName(name);
      }, this),
      hasCollections: hasCollections,
      isComposite: isComposite
    };
  };
  this._parseUdtName = function (typeName, startIndex, length) {
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
      var fieldType = this.parseFqTypeName(p, separatorIndex + 1, p.length - (separatorIndex + 1));
      udtInfo.fields.push({
        name: new Buffer(p.substr(0, separatorIndex), 'hex').toString(),
        type: fieldType
      });
    }
    dataType.info = udtInfo;
    return dataType;
  };
}

/**
 * Sets the encoder and decoder methods for this instance
 * @private
 */
function setEncoders() {
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
}

/**
 * Decodes Cassandra bytes into Javascript values.
 * <p>
 * This is part of an <b>experimental</b> API, this can be changed future releases.
 * </p>
 * @param {Buffer} buffer Raw buffer to be decoded.
 * @param {Object} type An object containing the data type <code>code</code> and <code>info</code>.
 * @param {Number} type.code Type code.
 * @param {Object} [type.info] Additional information on the type for complex / nested types.
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
 * Encodes Javascript types into Buffer according to the Cassandra protocol.
 * <p>
 * This is part of an <b>experimental</b> API, this can be changed future releases.
 * </p>
 * @param {*} value The value to be converted.
 * @param {{code: number, info: *|Object}|String|Number} [typeInfo] The type information.
 * <p>It can be either a:</p>
 * <ul>
 *   <li>A <code>String</code> representing the data type.</li>
 *   <li>A <code>Number</code> with one of the values of {@link module:types~dataTypes dataTypes}.</li>
 *   <li>An <code>Object</code> containing the <code>type.code</code> as one of the values of
 *   {@link module:types~dataTypes dataTypes} and <code>type.info</code>.
 *   </li>
 * </ul>
 * @returns {Buffer}
 * @throws {TypeError} When there is an encoding error
 */
Encoder.prototype.encode = function (value, typeInfo) {
  if (value === undefined) {
    //defaults to null
    value = null;
    if (this.encodingOptions.useUndefinedAsUnset && this.protocolVersion >= 4) {
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
    type = Encoder.guessDataType(value);
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
 * @ignore
 * @internal
 */
Encoder.guessDataType = function (value) {
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
 * Gets a buffer containing with the bytes (BE) representing the collection length for protocol v2 and below
 * @param {Buffer|Number} value
 * @returns {Buffer}
 * @private
 */
function getLengthBufferV2(value) {
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
}

/**
 * Gets a buffer containing with the bytes (BE) representing the collection length for protocol v3 and above
 * @param {Buffer|Number} value
 * @returns {Buffer}
 * @private
 */
function getLengthBufferV3(value) {
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
}

/**
 * @param {Buffer} buffer
 * @private
 */
function handleBufferCopy(buffer) {
  if (buffer === null) {
    return null;
  }
  return utils.copyBuffer(buffer);
}

/**
 * @param {Buffer} buffer
 * @private
 */
function handleBufferRef(buffer) {
  return buffer;
}
/**
 * Decodes collection length for protocol v3 and above
 * @param bytes
 * @param offset
 * @returns {Number}
 * @private
 */
function decodeCollectionLengthV3(bytes, offset) {
  return bytes.readUInt32BE(offset);
}
/**
 * Decodes collection length for protocol v2 and below
 * @param bytes
 * @param offset
 * @returns {Number}
 * @private
 */
function decodeCollectionLengthV2(bytes, offset) {
  return bytes.readUInt16BE(offset)
}

/**
 * @param {String} value
 * @param {Number} startIndex
 * @param {Number} length
 * @param {String} [open]
 * @param {String} [close]
 * @returns {Array}
 * @private
 */
function parseParams(value, startIndex, length, open, close) {
  open = open || '(';
  close = close || ')';
  var types = [];
  var paramStart = startIndex;
  var level = 0;
  for (var i = startIndex; i < startIndex + length; i++) {
    var c = value[i];
    if (c == open) {
      level++;
    }
    if (c == close) {
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

/**
 * @param {Array.<Buffer>} parts
 * @param {Number} totalLength
 * @returns {Buffer}
 * @private
 */
function concatRoutingKey(parts, totalLength) {
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
}

module.exports = Encoder;
