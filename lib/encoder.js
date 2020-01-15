/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const util = require('util');

const types = require('./types');
const dataTypes = types.dataTypes;
const Long = types.Long;
const Integer = types.Integer;
const BigDecimal = types.BigDecimal;
const MutableLong = require('./types/mutable-long');
const utils = require('./utils');
const token = require('./token');
const { DateRange } = require('./datastax/search');
const geo = require('./geometry');
const Geometry = geo.Geometry;
const LineString = geo.LineString;
const Point = geo.Point;
const Polygon = geo.Polygon;

const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const buffers = {
  int16Zero: utils.allocBufferFromArray([0, 0]),
  int32Zero: utils.allocBufferFromArray([0, 0, 0, 0]),
  int8Zero: utils.allocBufferFromArray([0]),
  int8One: utils.allocBufferFromArray([1]),
  int8MaxValue: utils.allocBufferFromArray([0xff])
};

// BigInt: Avoid using literals (e.g., 32n) as we must be able to compile with older engines
const isBigIntSupported = typeof BigInt !== 'undefined';
const bigInt32 = isBigIntSupported ? BigInt(32) : null;
const bigInt8 = isBigIntSupported ? BigInt(8) : null;
const bigInt0 = isBigIntSupported ? BigInt(0) : null;
const bigIntMinus1 = isBigIntSupported ? BigInt(-1) : null;
const bigInt32BitsOn = isBigIntSupported ? BigInt(0xffffffff) : null;
const bigInt8BitsOn = isBigIntSupported ? BigInt(0xff) : null;

const complexTypeNames = Object.freeze({
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
const cqlNames = Object.freeze({
  frozen: 'frozen',
  list: 'list',
  'set': 'set',
  map: 'map',
  tuple: 'tuple',
  empty: 'empty',
  duration: 'duration'
});
const singleTypeNames = Object.freeze({
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
const singleFqTypeNamesLength = Object.keys(singleTypeNames).reduce(function (previous, current) {
  return current.length > previous ? current.length : previous;
}, 0);

const customTypeNames = Object.freeze({
  duration: 'org.apache.cassandra.db.marshal.DurationType',
  lineString: 'org.apache.cassandra.db.marshal.LineStringType',
  point: 'org.apache.cassandra.db.marshal.PointType',
  polygon: 'org.apache.cassandra.db.marshal.PolygonType',
  dateRange: 'org.apache.cassandra.db.marshal.DateRangeType'
});

const nullValueBuffer = utils.allocBufferFromArray([255, 255, 255, 255]);
const unsetValueBuffer = utils.allocBufferFromArray([255, 255, 255, 254]);

/**
 * For backwards compatibility, empty buffers as text/blob/custom values are supported.
 * In the case of other types, they are going to be decoded as a <code>null</code> value.
 * @private
 * @type {Set}
 */
const zeroLengthTypesSupported = new Set([
  dataTypes.text,
  dataTypes.ascii,
  dataTypes.varchar,
  dataTypes.custom,
  dataTypes.blob
]);

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
    if (!types.protocolVersion.uses4BytesCollectionLength(this.protocolVersion)) {
      this.decodeCollectionLength = decodeCollectionLengthV2;
      this.getLengthBuffer = getLengthBufferV2;
      this.collectionLengthSize = 2;
    }
  };

  const customDecoders = {
    [customTypeNames.duration]: decodeDuration,
    [customTypeNames.lineString]: decodeLineString,
    [customTypeNames.point]: decodePoint,
    [customTypeNames.polygon]: decodePolygon,
    [customTypeNames.dateRange]: decodeDateRange
  };

  const customEncoders = {
    [customTypeNames.duration]: encodeDuration,
    [customTypeNames.lineString]: encodeLineString,
    [customTypeNames.point]: encodePoint,
    [customTypeNames.polygon]: encodePolygon,
    [customTypeNames.dateRange]: encodeDateRange
  };

  // Decoding methods
  this.decodeBlob = function (bytes) {
    return this.handleBuffer(bytes);
  };
  this.decodeCustom = function (bytes, typeName) {
    const handler = customDecoders[typeName];
    if (handler) {
      return handler.call(this, bytes);
    }
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

  this._decodeCqlLongAsLong = function (bytes) {
    return Long.fromBuffer(bytes);
  };

  this._decodeCqlLongAsBigInt = function (bytes) {
    return BigInt.asIntN(64, (BigInt(bytes.readUInt32BE(0)) << bigInt32) | BigInt(bytes.readUInt32BE(4)));
  };

  this.decodeLong = this.encodingOptions.useBigIntAsLong
    ? this._decodeCqlLongAsBigInt
    : this._decodeCqlLongAsLong;

  this._decodeVarintAsInteger = function (bytes) {
    return Integer.fromBuffer(bytes);
  };

  this._decodeVarintAsBigInt = function decodeVarintAsBigInt(bytes) {
    let result = bigInt0;
    if (bytes[0] <= 0x7f) {
      for (let i = 0; i < bytes.length; i++) {
        const b = BigInt(bytes[bytes.length - 1 - i]);
        result = result | (b << BigInt(i * 8));
      }
    } else {
      for (let i = 0; i < bytes.length; i++) {
        const b = BigInt(bytes[bytes.length - 1 - i]);
        result = result | ((~b & bigInt8BitsOn) << BigInt(i * 8));
      }
      result = ~result;
    }

    return result;
  };

  this.decodeVarint = this.encodingOptions.useBigIntAsVarint
    ? this._decodeVarintAsBigInt
    : this._decodeVarintAsInteger;

  this.decodeDecimal = function(bytes) {
    return BigDecimal.fromBuffer(bytes);
  };
  this.decodeTimestamp = function(bytes) {
    return new Date(this._decodeCqlLongAsLong(bytes).toNumber());
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
    const totalItems = this.decodeCollectionLength(bytes, 0);
    let offset = this.collectionLengthSize;
    const list = new Array(totalItems);
    for (let i = 0; i < totalItems; i++) {
      //bytes length of the item
      const length = this.decodeCollectionLength(bytes, offset);
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
    const arr = this.decodeList(bytes, subtype);
    if (this.encodingOptions.set) {
      const setConstructor = this.encodingOptions.set;
      return new setConstructor(arr);
    }
    return arr;
  };
  /*
   * Reads a map (key / value) from bytes
   */
  this.decodeMap = function (bytes, subtypes) {
    let map;
    const totalItems = this.decodeCollectionLength(bytes, 0);
    let offset = this.collectionLengthSize;
    const self = this;
    function readValues(callback, thisArg) {
      for (let i = 0; i < totalItems; i++) {
        const keyLength = self.decodeCollectionLength(bytes, offset);
        offset += self.collectionLengthSize;
        const key = self.decode(bytes.slice(offset, offset + keyLength), subtypes[0]);
        offset += keyLength;
        const valueLength = self.decodeCollectionLength(bytes, offset);
        offset += self.collectionLengthSize;
        if (valueLength < 0) {
          callback.call(thisArg, key, null);
          continue;
        }
        const value = self.decode(bytes.slice(offset, offset + valueLength), subtypes[1]);
        offset += valueLength;
        callback.call(thisArg, key, value);
      }
    }
    if (this.encodingOptions.map) {
      const mapConstructor = this.encodingOptions.map;
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
    const result = {};
    let offset = 0;
    for (let i = 0; i < udtInfo.fields.length && offset < bytes.length; i++) {
      //bytes length of the field value
      const length = bytes.readInt32BE(offset);
      offset += 4;
      //slice it
      const field = udtInfo.fields[i];
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
    const elements = new Array(tupleInfo.length);
    let offset = 0;

    for (let i = 0; i < tupleInfo.length && offset < bytes.length; i++) {
      const length = bytes.readInt32BE(offset);
      offset += 4;

      if (length < 0) {
        elements[i] = null;
        continue;
      }

      elements[i] = this.decode(bytes.slice(offset, offset+length), tupleInfo[i]);
      offset += length;
    }

    return types.Tuple.fromArray(elements);
  };

  //Encoding methods
  this.encodeFloat = function (value) {
    if (typeof value === 'string') {
      // All numeric types are supported as strings for historical reasons
      value = parseFloat(value);

      if (Number.isNaN(value)) {
        throw new TypeError(`Expected string representation of a number, obtained ${util.inspect(value)}`);
      }
    }

    if (typeof value !== 'number') {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }

    const buf = utils.allocBufferUnsafe(4);
    buf.writeFloatBE(value, 0);
    return buf;
  };

  this.encodeDouble = function (value) {
    if (typeof value === 'string') {
      // All numeric types are supported as strings for historical reasons
      value = parseFloat(value);

      if (Number.isNaN(value)) {
        throw new TypeError(`Expected string representation of a number, obtained ${util.inspect(value)}`);
      }
    }

    if (typeof value !== 'number') {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }

    const buf = utils.allocBufferUnsafe(8);
    buf.writeDoubleBE(value, 0);
    return buf;
  };

  /**
   * @param {Date|String|Long|Number} value
   * @private
   */
  this.encodeTimestamp = function (value) {
    const originalValue = value;
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
    if (this.encodingOptions.useBigIntAsLong) {
      value = BigInt(value);
    }
    return this.encodeLong(value);
  };
  /**
   * @param {Date|String|LocalDate} value
   * @returns {Buffer}
   * @throws {TypeError}
   * @private
   */
  this.encodeDate = function (value) {
    const originalValue = value;
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
    const originalValue = value;
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
        value = types.Uuid.fromString(value).getBuffer();
      }
      catch (err) {
        throw new TypeError(err.message);
      }
    } else if (value instanceof types.Uuid) {
      value = value.getBuffer();
    } else {
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
  this._encodeBigIntFromLong = function (value) {
    if (typeof value === 'number') {
      value = Long.fromNumber(value);
    } else if (typeof value === 'string') {
      value = Long.fromString(value);
    }

    let buf = null;

    if (value instanceof Long) {
      buf = Long.toBuffer(value);
    } else if (value instanceof MutableLong) {
      buf = Long.toBuffer(value.toImmutable());
    }

    if (buf === null) {
      throw new TypeError('Not a valid bigint, expected Long/Number/String/Buffer, obtained ' + util.inspect(value));
    }

    return buf;
  };

  this._encodeBigIntFromBigInt = function (value) {
    if (typeof value === 'string') {
      // All numeric types are supported as strings for historical reasons
      value = BigInt(value);
    }

    // eslint-disable-next-line valid-typeof
    if (typeof value !== 'bigint') {
      // Only BigInt values are supported
      throw new TypeError('Not a valid BigInt value, obtained ' + util.inspect(value));
    }

    const buffer = utils.allocBufferUnsafe(8);
    buffer.writeUInt32BE(Number(value >> bigInt32) >>> 0, 0);
    buffer.writeUInt32BE(Number(value & bigInt32BitsOn), 4);
    return buffer;
  };

  this.encodeLong = this.encodingOptions.useBigIntAsLong
    ? this._encodeBigIntFromBigInt
    : this._encodeBigIntFromLong;

  /**
   * @param {Integer|Buffer|String|Number} value
   * @returns {Buffer}
   * @private
   */
  this._encodeVarintFromInteger = function (value) {
    if (typeof value === 'number') {
      value = Integer.fromNumber(value);
    }
    if (typeof value === 'string') {
      value = Integer.fromString(value);
    }
    let buf = null;
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

  this._encodeVarintFromBigInt = function (value) {
    if (typeof value === 'string') {
      // All numeric types are supported as strings for historical reasons
      value = BigInt(value);
    }

    // eslint-disable-next-line valid-typeof
    if (typeof value !== 'bigint') {
      throw new TypeError('Not a valid varint, expected BigInt, obtained ' + util.inspect(value));
    }

    if (value === bigInt0) {
      return buffers.int8Zero;

    }
    else if (value === bigIntMinus1) {
      return buffers.int8MaxValue;
    }

    const parts = [];

    if (value > bigInt0){
      while (value !== bigInt0) {
        parts.unshift(Number(value & bigInt8BitsOn));
        value = value >> bigInt8;
      }

      if (parts[0] > 0x7f) {
        // Positive value needs a padding
        parts.unshift(0);
      }
    } else {
      while (value !== bigIntMinus1) {
        parts.unshift(Number(value & bigInt8BitsOn));
        value = value >> bigInt8;
      }

      if (parts[0] <= 0x7f) {
        // Negative value needs a padding
        parts.unshift(0xff);
      }
    }

    return utils.allocBufferFromArray(parts);
  };

  this.encodeVarint = this.encodingOptions.useBigIntAsVarint
    ? this._encodeVarintFromBigInt
    : this._encodeVarintFromInteger;

  /**
   * @param {BigDecimal|Buffer|String|Number} value
   * @returns {Buffer}
   * @private
   */
  this.encodeDecimal = function (value) {
    if (typeof value === 'number') {
      value = BigDecimal.fromNumber(value);
    } else if (typeof value === 'string') {
      value = BigDecimal.fromString(value);
    }

    let buf = null;

    if (value instanceof BigDecimal) {
      buf = BigDecimal.toBuffer(value);
    } else {
      throw new TypeError('Not a valid varint, expected BigDecimal/Number/String/Buffer, obtained ' + util.inspect(value));
    }

    return buf;
  };
  this.encodeString = function (value, encoding) {
    if (typeof value !== 'string') {
      throw new TypeError('Not a valid text value, expected String obtained ' + util.inspect(value));
    }
    return utils.allocBufferFromString(value, encoding);
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
  this.encodeCustom = function (value, name) {
    const handler = customEncoders[name];
    if (handler) {
      return handler.call(this, value);
    }
    throw new TypeError('No encoding handler found for type ' + name);
  };
  /**
   * @param {Boolean} value
   * @returns {Buffer}
   * @private
   */
  this.encodeBoolean = function (value) {
    return value ? buffers.int8One : buffers.int8Zero;
  };
  /**
   * @param {Number|String} value
   * @private
   */
  this.encodeInt = function (value) {
    if (isNaN(value)) {
      throw new TypeError('Expected Number, obtained ' + util.inspect(value));
    }
    const buf = utils.allocBufferUnsafe(4);
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
    const buf = utils.allocBufferUnsafe(2);
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
    const buf = utils.allocBufferUnsafe(1);
    buf.writeInt8(value, 0);
    return buf;
  };
  this.encodeList = function (value, subtype) {
    if (!Array.isArray(value)) {
      throw new TypeError('Not a valid list value, expected Array obtained ' + util.inspect(value));
    }
    if (value.length === 0) {
      return null;
    }
    const parts = [];
    parts.push(this.getLengthBuffer(value));
    for (let i = 0;i < value.length;i++) {
      const val = value[i];
      if (val === null || typeof val === 'undefined' || val === types.unset) {
        throw new TypeError('A collection can\'t contain null or unset values');
      }
      const bytes = this.encode(val, subtype);
      //include item byte length
      parts.push(this.getLengthBuffer(bytes));
      //include item
      parts.push(bytes);
    }
    return Buffer.concat(parts);
  };
  this.encodeSet = function (value, subtype) {
    if (this.encodingOptions.set && value instanceof this.encodingOptions.set) {
      const arr = [];
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
    const parts = [];
    let propCounter = 0;
    let keySubtype = null;
    let valueSubtype = null;
    const self = this;
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
      const keyBuffer = self.encode(key, keySubtype);
      //include item byte length
      parts.push(self.getLengthBuffer(keyBuffer));
      //include item
      parts.push(keyBuffer);
      //value
      const valueBuffer = self.encode(val, valueSubtype);
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
      for (const key in value) {
        if (!value.hasOwnProperty(key)) {
          continue;
        }
        const val = value[key];
        addItem(val, key);
      }
    }

    parts.unshift(this.getLengthBuffer(propCounter));
    return Buffer.concat(parts);
  };
  this.encodeUdt = function (value, udtInfo) {
    const parts = [];
    let totalLength = 0;
    for (let i = 0; i < udtInfo.fields.length; i++) {
      const field = udtInfo.fields[i];
      const item = this.encode(value[field.name], field.type);
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
      const lengthBuffer = utils.allocBufferUnsafe(4);
      lengthBuffer.writeInt32BE(item.length, 0);
      parts.push(lengthBuffer);
      parts.push(item);
      totalLength += item.length + 4;
    }
    return Buffer.concat(parts, totalLength);
  };
  this.encodeTuple = function (value, tupleInfo) {
    const parts = [];
    let totalLength = 0;
    const length = Math.min(tupleInfo.length, value.length);

    for (let i = 0; i < length; i++) {
      const type = tupleInfo[i];
      const item = this.encode(value.get(i), type);

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

      const lengthBuffer = utils.allocBufferUnsafe(4);
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
   * @param {ExecutionOptions} execOptions
   * @param [keys] parameter keys and positions in the params array
   * @throws TypeError
   * @internal
   * @ignore
   */
  this.setRoutingKeyFromUser = function (params, execOptions, keys) {
    let totalLength = 0;
    const userRoutingKey = execOptions.getRoutingKey();
    if (Array.isArray(userRoutingKey)) {
      if (userRoutingKey.length === 1) {
        execOptions.setRoutingKey(userRoutingKey[0]);
        return;
      }

      // Its a composite routing key
      totalLength = 0;
      for (let i = 0; i < userRoutingKey.length; i++) {
        const item = userRoutingKey[i];
        if (!item) {
          // Invalid routing key part provided by the user, clear the value
          execOptions.setRoutingKey(null);
          return;
        }
        totalLength += item.length + 3;
      }

      execOptions.setRoutingKey(concatRoutingKey(userRoutingKey, totalLength));
      return;
    }
    // If routingKey is present, ensure it is a Buffer, Token, or TokenRange.  Otherwise throw an error.
    if (userRoutingKey) {
      if (userRoutingKey instanceof Buffer || userRoutingKey instanceof token.Token
        || userRoutingKey instanceof token.TokenRange) {
        return;
      }

      throw new TypeError(`Unexpected routingKey '${util.inspect(userRoutingKey)}' provided. ` +
        `Expected Buffer, Array<Buffer>, Token, or TokenRange.`);
    }

    // If no params are present, return as routing key cannot be determined.
    if (!params || params.length === 0) {
      return;
    }

    let routingIndexes = execOptions.getRoutingIndexes();
    if (execOptions.getRoutingNames()) {
      routingIndexes = execOptions.getRoutingNames().map(k => keys[k]);
    }
    if (!routingIndexes) {
      return;
    }

    const parts = [];
    const hints = execOptions.getHints() || utils.emptyArray;

    const encodeParam = !keys ?
      (i => this.encode(params[i], hints[i])) :
      (i => this.encode(params[i].value, hints[i]));

    try {
      totalLength = this._encodeRoutingKeyParts(parts, routingIndexes, encodeParam);
    } catch (e) {
      // There was an error encoding a parameter that is part of the routing key,
      // ignore now to fail afterwards
    }

    if (totalLength === 0) {
      return;
    }

    execOptions.setRoutingKey(concatRoutingKey(parts, totalLength));
  };

  /**
   * Sets the routing key in the options based on the prepared statement metadata.
   * @param {Object} meta Prepared metadata
   * @param {Array} params Array of parameters
   * @param {ExecutionOptions} execOptions
   * @throws TypeError
   * @internal
   * @ignore
   */
  this.setRoutingKeyFromMeta = function (meta, params, execOptions) {
    const routingIndexes = execOptions.getRoutingIndexes();
    if (!routingIndexes) {
      return;
    }
    const parts = new Array(routingIndexes.length);
    const encodeParam = i => {
      const columnInfo = meta.columns[i];
      return this.encode(params[i], columnInfo ? columnInfo.type : null);
    };

    let totalLength = 0;

    try {
      totalLength = this._encodeRoutingKeyParts(parts, routingIndexes, encodeParam);
    } catch (e) {
      // There was an error encoding a parameter that is part of the routing key,
      // ignore now to fail afterwards
    }

    if (totalLength === 0) {
      return;
    }

    execOptions.setRoutingKey(concatRoutingKey(parts, totalLength));
  };

  /**
   * @param {Array} parts
   * @param {Array} routingIndexes
   * @param {Function} encodeParam
   * @returns {Number} The total length
   * @private
   */
  this._encodeRoutingKeyParts = function (parts, routingIndexes, encodeParam) {
    let totalLength = 0;
    for (let i = 0; i < routingIndexes.length; i++) {
      const paramIndex = routingIndexes[i];
      if (paramIndex === undefined) {
        // Bad input from the user, ignore
        return 0;
      }

      const item = encodeParam(paramIndex);
      if (item === null || item === undefined || item === types.unset) {
        // The encoded partition key should an instance of Buffer
        // Let it fail later in the pipeline for null/undefined parameter values
        return 0;
      }

      // Per each part of the routing key, 3 extra bytes are needed
      totalLength += item.length + 3;
      parts[i] = item;
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
   * @returns {Promise<{err, info, options}>} callback Callback invoked with err and  {{code: number, info: Object|Array|null, options: {frozen: Boolean}}}
   * @internal
   * @ignore
   */
  this.parseTypeName = async function (keyspace, typeName, startIndex, length, udtResolver) {
    startIndex = startIndex || 0;
    if (!length) {
      length = typeName.length;
    }

    const dataType = {
      code: 0,
      info: null,
      options: {
        frozen: false
      }
    };

    let innerTypes;

    if (typeName.indexOf("'", startIndex) === startIndex) {
      //If quoted, this is a custom type.
      dataType.info = typeName.substr(startIndex+1, length-2);
      return dataType;
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

      if (innerTypes.length !== 1) {
        throw new TypeError('Not a valid type ' + typeName);
      }

      dataType.code = dataTypes.list;
      dataType.info = await this.parseTypeName(keyspace, innerTypes[0], 0, null, udtResolver);
      return dataType;
    }

    if (typeName.indexOf(cqlNames.set, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.set.length + 1;
      length -= cqlNames.set.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');

      if (innerTypes.length !== 1) {
        throw new TypeError('Not a valid type ' + typeName);
      }

      dataType.code = dataTypes.set;
      dataType.info = await this.parseTypeName(keyspace, innerTypes[0], 0, null, udtResolver);
      return dataType;
    }

    if (typeName.indexOf(cqlNames.map, startIndex) === startIndex) {
      //move cursor across the name and bypass the angle brackets
      startIndex += cqlNames.map.length + 1;
      length -= cqlNames.map.length + 2;
      innerTypes = parseParams(typeName, startIndex, length, '<', '>');

      //It should contain the key and value types
      if (innerTypes.length !== 2) {
        throw new TypeError('Not a valid type ' + typeName);
      }

      dataType.code = dataTypes.map;
      dataType.info = await this._parseChildTypes(keyspace, innerTypes, udtResolver);
      return dataType;
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
      dataType.info = await this._parseChildTypes(keyspace, innerTypes, udtResolver);
      return dataType;
    }

    const quoted = typeName.indexOf('"', startIndex) === startIndex;
    if (quoted) {
      // Remove quotes
      startIndex++;
      length -= 2;
    }

    // Quick check if its a single type
    if (startIndex > 0) {
      typeName = typeName.substr(startIndex, length);
    }

    // Un-escape double quotes if quoted.
    if (quoted) {
      typeName = typeName.replace('""', '"');
    }

    const typeCode = dataTypes[typeName];
    if (typeof typeCode === 'number') {
      dataType.code = typeCode;
      return dataType;
    }

    if (typeName === cqlNames.duration) {
      dataType.info = customTypeNames.duration;
      return dataType;
    }

    if (typeName === cqlNames.empty) {
      // Set as custom
      dataType.info = 'empty';
      return dataType;
    }

    const udtInfo = await udtResolver(keyspace, typeName);
    if (udtInfo) {
      dataType.code = dataTypes.udt;
      dataType.info = udtInfo;
      return dataType;
    }

    throw new TypeError('Not a valid type "' + typeName + '"');
  };

  /**
   * @param {String} keyspace
   * @param {Array} typeNames
   * @param {Function} udtResolver
   * @returns {Promise}
   * @private
   */
  this._parseChildTypes = function (keyspace, typeNames, udtResolver) {
    return Promise.all(typeNames.map(name => this.parseTypeName(keyspace, name.trim(), 0, null, udtResolver)));
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
    const dataType = {
      code: 0,
      info: null,
      options: {
        reversed: false,
        frozen: false
      }
    };
    startIndex = startIndex || 0;
    let innerTypes;
    if (!length) {
      length = typeName.length;
    }
    if (length > complexTypeNames.reversed.length && typeName.indexOf(complexTypeNames.reversed) === startIndex) {
      //Remove the reversed token
      startIndex += complexTypeNames.reversed.length + 1;
      length -= complexTypeNames.reversed.length + 2;
      dataType.options.reversed = true;
    }
    if (length > complexTypeNames.frozen.length &&
        typeName.indexOf(complexTypeNames.frozen, startIndex) === startIndex) {
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
      const typeCode = singleTypeNames[typeName];
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
      if (innerTypes.length !== 1) {
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
      if (innerTypes.length !== 1)
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
      if (innerTypes.length !== 2) {
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
      dataType.info = innerTypes.map(x => this.parseFqTypeName(x));
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
    let i = 0;
    let length = typesString.length;
    const isComposite = typesString.indexOf(complexTypeNames.composite) === 0;
    if (isComposite) {
      i = complexTypeNames.composite.length + 1;
      length--;
    }
    const types = [];
    let startIndex = i;
    let nested = 0;
    let inCollectionType = false;
    let hasCollections = false;
    //as collection types are not allowed, it is safe to split by ,
    while (++i < length) {
      switch (typesString[i]) {
        case ',':
          if (nested > 0) {
            break;
          }
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
      types: types.map(name => this.parseFqTypeName(name)),
      hasCollections: hasCollections,
      isComposite: isComposite
    };
  };
  this._parseUdtName = function (typeName, startIndex, length) {
    const udtParams = parseParams(typeName, startIndex, length);
    if (udtParams.length < 2) {
      //It should contain at least the keyspace, name of the udt and a type
      throw new TypeError('Not a valid type ' + typeName);
    }
    const dataType = {
      code: dataTypes.udt,
      info: null
    };
    const udtInfo = {
      keyspace: udtParams[0],
      name: utils.allocBufferFromString(udtParams[1], 'hex').toString(),
      fields: []
    };
    for (let i = 2; i < udtParams.length; i++) {
      const p = udtParams[i];
      const separatorIndex = p.indexOf(':');
      const fieldType = this.parseFqTypeName(p, separatorIndex + 1, p.length - (separatorIndex + 1));
      udtInfo.fields.push({
        name: utils.allocBufferFromString(p.substr(0, separatorIndex), 'hex').toString(),
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
  this.decoders = {
    [dataTypes.custom]: this.decodeCustom,
    [dataTypes.ascii]: this.decodeAsciiString,
    [dataTypes.bigint]: this.decodeLong,
    [dataTypes.blob]: this.decodeBlob,
    [dataTypes.boolean]: this.decodeBoolean,
    [dataTypes.counter]: this.decodeLong,
    [dataTypes.decimal]: this.decodeDecimal,
    [dataTypes.double]: this.decodeDouble,
    [dataTypes.float]: this.decodeFloat,
    [dataTypes.int]: this.decodeInt,
    [dataTypes.text]: this.decodeUtf8String,
    [dataTypes.timestamp]: this.decodeTimestamp,
    [dataTypes.uuid]: this.decodeUuid,
    [dataTypes.varchar]: this.decodeUtf8String,
    [dataTypes.varint]: this.decodeVarint,
    [dataTypes.timeuuid]: this.decodeTimeUuid,
    [dataTypes.inet]: this.decodeInet,
    [dataTypes.date]: this.decodeDate,
    [dataTypes.time]: this.decodeTime,
    [dataTypes.smallint]: this.decodeSmallint,
    [dataTypes.tinyint]: this.decodeTinyint,
    [dataTypes.duration]: decodeDuration,
    [dataTypes.list]: this.decodeList,
    [dataTypes.map]: this.decodeMap,
    [dataTypes.set]: this.decodeSet,
    [dataTypes.udt]: this.decodeUdt,
    [dataTypes.tuple]: this.decodeTuple
  };

  this.encoders = {
    [dataTypes.custom]: this.encodeCustom,
    [dataTypes.ascii]: this.encodeAsciiString,
    [dataTypes.bigint]: this.encodeLong,
    [dataTypes.blob]: this.encodeBlob,
    [dataTypes.boolean]: this.encodeBoolean,
    [dataTypes.counter]: this.encodeLong,
    [dataTypes.decimal]: this.encodeDecimal,
    [dataTypes.double]: this.encodeDouble,
    [dataTypes.float]: this.encodeFloat,
    [dataTypes.int]: this.encodeInt,
    [dataTypes.text]: this.encodeUtf8String,
    [dataTypes.timestamp]: this.encodeTimestamp,
    [dataTypes.uuid]: this.encodeUuid,
    [dataTypes.varchar]: this.encodeUtf8String,
    [dataTypes.varint]: this.encodeVarint,
    [dataTypes.timeuuid]: this.encodeUuid,
    [dataTypes.inet]: this.encodeInet,
    [dataTypes.date]: this.encodeDate,
    [dataTypes.time]: this.encodeTime,
    [dataTypes.smallint]: this.encodeSmallint,
    [dataTypes.tinyint]: this.encodeTinyint,
    [dataTypes.duration]: encodeDuration,
    [dataTypes.list]: this.encodeList,
    [dataTypes.map]: this.encodeMap,
    [dataTypes.set]: this.encodeSet,
    [dataTypes.udt]: this.encodeUdt,
    [dataTypes.tuple]: this.encodeTuple
  };
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
  if (buffer === null || (buffer.length === 0 && !zeroLengthTypesSupported.has(type.code))) {
    return null;
  }

  const decoder = this.decoders[type.code];

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
    value = this.encodingOptions.useUndefinedAsUnset && this.protocolVersion >= 4 ? types.unset : null;
  }

  if (value === types.unset) {
    if (!types.protocolVersion.supportsUnset(this.protocolVersion)) {
      throw new TypeError('Unset value can not be used for this version of Cassandra, protocol version: ' +
        this.protocolVersion);
    }

    return value;
  }

  if (value === null || value instanceof Buffer) {
    return value;
  }

  /** @type {{code: Number, info: object}} */
  let type = {
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

  const encoder = this.encoders[type.code];

  if (!encoder) {
    throw new Error('Type not supported ' + type.code);
  }

  return encoder.call(this, value, type.info);
};

/**
 * Try to guess the Cassandra type to be stored, based on the javascript value type
 * @param value
 * @returns {{code: number, info: object}|null}
 * @ignore
 * @internal
 */
Encoder.guessDataType = function (value) {
  let code = null;
  let info = null;
  const esTypeName = (typeof value);
  if (esTypeName === 'number') {
    code = dataTypes.double;
  }
  else if (esTypeName === 'string') {
    code = dataTypes.text;
    if (value.length === 36 && uuidRegex.test(value)){
      code = dataTypes.uuid;
    }
  }
  else if (esTypeName === 'boolean') {
    code = dataTypes.boolean;
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
  else if (value instanceof types.Duration) {
    code = dataTypes.custom;
    info = customTypeNames.duration;
  }
  else if (Array.isArray(value)) {
    code = dataTypes.list;
  }
  else if (value instanceof Geometry) {
    code = dataTypes.custom;
    if (value instanceof LineString) {
      info = customTypeNames.lineString;
    } else if (value instanceof Point) {
      info = customTypeNames.point;
    } else if (value instanceof Polygon) {
      info = customTypeNames.polygon;
    }
  }
  else if (value instanceof DateRange) {
    code = dataTypes.custom;
    info = customTypeNames.dateRange;
  }

  if (code === null) {
    return null;
  }
  return { code: code, info: info };
};

/**
 * Gets a buffer containing with the bytes (BE) representing the collection length for protocol v2 and below
 * @param {Buffer|Number} value
 * @returns {Buffer}
 * @private
 */
function getLengthBufferV2(value) {
  if (!value) {
    return buffers.int16Zero;
  }
  const lengthBuffer = utils.allocBufferUnsafe(2);
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
    return buffers.int32Zero;
  }
  const lengthBuffer = utils.allocBufferUnsafe(4);
  if (typeof value === 'number') {
    lengthBuffer.writeInt32BE(value, 0);
  }
  else {
    lengthBuffer.writeInt32BE(value.length, 0);
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
  return bytes.readInt32BE(offset);
}
/**
 * Decodes collection length for protocol v2 and below
 * @param bytes
 * @param offset
 * @returns {Number}
 * @private
 */
function decodeCollectionLengthV2(bytes, offset) {
  return bytes.readUInt16BE(offset);
}

function decodeDuration(bytes) {
  return types.Duration.fromBuffer(bytes);
}

function encodeDuration(value) {
  if (!(value instanceof types.Duration)) {
    throw new TypeError('Not a valid duration, expected Duration/Buffer obtained ' + util.inspect(value));
  }
  return value.toBuffer();
}

/**
 * @private
 * @param {Buffer} buffer
 */
function decodeLineString(buffer) {
  return LineString.fromBuffer(buffer);
}

/**
 * @private
 * @param {LineString} value
 */
function encodeLineString(value) {
  return value.toBuffer();
}

/**
 * @private
 * @param {Buffer} buffer
 */
function decodePoint(buffer) {
  return Point.fromBuffer(buffer);
}

/**
 * @private
 * @param {LineString} value
 */
function encodePoint(value) {
  return value.toBuffer();
}

/**
 * @private
 * @param {Buffer} buffer
 */
function decodePolygon(buffer) {
  return Polygon.fromBuffer(buffer);
}

/**
 * @private
 * @param {Polygon} value
 */
function encodePolygon(value) {
  return value.toBuffer();
}

function decodeDateRange(buffer) {
  return DateRange.fromBuffer(buffer);
}

/**
 * @private
 * @param {DateRange} value
 */
function encodeDateRange(value) {
  return value.toBuffer();
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
  const types = [];
  let paramStart = startIndex;
  let level = 0;
  for (let i = startIndex; i < startIndex + length; i++) {
    const c = value[i];
    if (c === open) {
      level++;
    }
    if (c === close) {
      level--;
    }
    if (level === 0 && c === ',') {
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
  if (totalLength === 0) {
    return null;
  }
  if (parts.length === 1) {
    return parts[0];
  }
  const routingKey = utils.allocBufferUnsafe(totalLength);
  let offset = 0;
  for (let i = 0; i < parts.length; i++) {
    const item = parts[i];
    routingKey.writeUInt16BE(item.length, offset);
    offset += 2;
    item.copy(routingKey, offset);
    offset += item.length;
    routingKey[offset] = 0;
    offset++;
  }
  return routingKey;
}

module.exports = Encoder;
