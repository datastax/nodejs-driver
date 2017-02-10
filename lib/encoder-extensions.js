/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var util = require('util');
var types = require('./types');
var Geometry = require('./geometry/geometry');
var Point = require('./geometry/point');
var Polygon = require('./geometry/polygon');
var LineString = require('./geometry/line-string');
var graphModule = require('./graph/');
var DateRange = require('./search').DateRange;
var Edge = graphModule.Edge;
var Path = graphModule.Path;
var Property = graphModule.Property;
var Vertex = graphModule.Vertex;
var VertexProperty = graphModule.VertexProperty;

var dseDecoders = {
  'org.apache.cassandra.db.marshal.LineStringType': decodeLineString,
  'org.apache.cassandra.db.marshal.PointType': decodePoint,
  'org.apache.cassandra.db.marshal.PolygonType': decodePolygon,
  'org.apache.cassandra.db.marshal.DateRangeType': decodeDateRange
};

var graphSONTypeKey = '@type';
var graphSONValueKey = '@value';

/**
 * @param {Encoder} EncoderConstructor
 */
function register(EncoderConstructor) {
  if (EncoderConstructor['_hasDseExtensions']) {
    return;
  }
  EncoderConstructor.prototype.baseDecode = EncoderConstructor.prototype.decode;
  EncoderConstructor.prototype.decode = decodeDse;
  EncoderConstructor.prototype.baseEncode = EncoderConstructor.prototype.encode;
  EncoderConstructor.prototype.encode = encodeDse;
  EncoderConstructor['_hasDseExtensions'] = true;
}

function decodeDse(buffer, type) {
  if (buffer != null && type.code === types.dataTypes.custom) {
    var func = dseDecoders[type.info];
    if (func) {
      return func.call(this, buffer);
    }
  }
  return this.baseDecode(buffer, type);
}

function encodeDse(value, typeInfo) {
  if (!value) {
    return this.baseEncode(value, typeInfo);
  }
  if (value instanceof Geometry) {
    if (value instanceof LineString) {
      return encodeLineString.call(this, value);
    }
    if (value instanceof Point) {
      return encodePoint.call(this, value);
    }
    if (value instanceof Polygon) {
      return encodePolygon.call(this, value);
    }
  }
  if (value instanceof DateRange) {
    return encodeDateRange.call(this, value);
  }
  return this.baseEncode(value, typeInfo);
}

/** @param {Buffer} buffer */
function decodeLineString(buffer) {
  return LineString.fromBuffer(buffer);
}

/** @param {LineString} value */
function encodeLineString(value) {
  return value.toBuffer();
}

/** @param {Buffer} buffer */
function decodePoint(buffer) {
  return Point.fromBuffer(buffer);
}

/** @param {LineString} value */
function encodePoint(value) {
  return value.toBuffer();
}

/** @param {Buffer} buffer */
function decodePolygon(buffer) {
  return Polygon.fromBuffer(buffer);
}

/** @param {Polygon} value */
function encodePolygon(value) {
  return value.toBuffer();
}

function decodeDateRange(buffer) {
  return DateRange.fromBuffer(buffer);
}

/** @param {DateRange} value */
function encodeDateRange(value) {
  return value.toBuffer();
}

/**
 * GraphSON2 Reader
 * @constructor
 */
function GraphSONReader() {
  var deserializerConstructors = [
    VertexDeserializer,
    EdgeDeserializer,
    VertexPropertyDeserializer,
    PropertyDeserializer,
    PathDeserializer,
    UuidDeserializer,
    InstantDeserializer,
    LongDeserializer,
    BigDecimalDeserializer,
    BigIntegerDeserializer,
    InetAddressDeserializer,
    LocalDateDeserializer,
    LocalTimeDeserializer,
    BlobDeserializer,
    PointDeserializer,
    LineStringDeserializer,
    PolygonDeserializer
  ];
  this._deserializers = {};
  deserializerConstructors.forEach(function (C) {
    var s = new C();
    s.reader = this;
    this._deserializers[s.key] = s;
  }, this);
}

GraphSONReader.prototype.read = function (obj) {
  if (obj === undefined) {
    return undefined;
  }
  if (Array.isArray(obj)) {
    return obj.map(function mapEach(item) {
      return this.read(item);
    }, this);
  }
  var type = obj[graphSONTypeKey];
  if (type) {
    var d = this._deserializers[type];
    if (d) {
      // Use type serializer
      return d.deserialize(obj);
    }
    return obj[graphSONValueKey];
  }
  if (obj && typeof obj === 'object' && obj.constructor === Object) {
    return this._deserializeObject(obj);
  }
  // Default (for boolean, number and other scalars)
  return obj;
};

GraphSONReader.prototype._deserializeObject = function (obj) {
  var keys = Object.keys(obj);
  var result = {};
  for (var i = 0; i < keys.length; i++) {
    result[keys[i]] = this.read(obj[keys[i]]);
  }
  return result;
};

function VertexDeserializer() {
  this.key = 'g:Vertex';
}

VertexDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
};

function VertexPropertyDeserializer() {
  this.key = 'g:VertexProperty';
}

VertexPropertyDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new VertexProperty(
    this.reader.read(value['id']),
    value['label'],
    this.reader.read(value['value']),
    this.reader.read(value['properties']));
};

function PropertyDeserializer() {
  this.key = 'g:Property';
}

PropertyDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Property(
    value['key'],
    this.reader.read(value['value']));
};

function EdgeDeserializer() {
  this.key = 'g:Edge';
}

EdgeDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Edge(
    this.reader.read(value['id']),
    this.reader.read(value['outV']),
    value['outVLabel'],
    value['label'],
    this.reader.read(value['inV']),
    value['inVLabel'],
    this.reader.read(value['properties'])
  );
};

function PathDeserializer() {
  this.key = 'g:Path';
}

PathDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  var objects = value['objects'].map(function objectMapItem(o) {
    return this.reader.read(o);
  }, this);
  return new Path(this.reader.read(value['labels']), objects);
};

/**
 * Uses toString() instance method and fromString() static method to serialize and deserialize the value.
 * @param {String} key
 * @param {Function} targetType
 * @constructor
 * @abstract
 */
function StringBasedDeserializer(key, targetType) {
  if (!key) {
    throw new Error('Deserializer must provide a type key');
  }
  if (!targetType) {
    throw new Error('Deserializer must provide a target type');
  }
  this.key = key;
  this.targetType = targetType;
}

StringBasedDeserializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  if (typeof value !== 'string') {
    value = value.toString();
  }
  return this.targetType.fromString(value);
};

function UuidDeserializer() {
  StringBasedDeserializer.call(this, 'g:UUID', types.Uuid);
}

util.inherits(UuidDeserializer, StringBasedDeserializer);

function LongDeserializer() {
  StringBasedDeserializer.call(this, 'g:Int64', types.Long);
}

util.inherits(LongDeserializer, StringBasedDeserializer);

function BigDecimalDeserializer() {
  StringBasedDeserializer.call(this, 'gx:BigDecimal', types.BigDecimal);
}

util.inherits(BigDecimalDeserializer, StringBasedDeserializer);

function BigIntegerDeserializer() {
  StringBasedDeserializer.call(this, 'gx:BigInteger', types.Integer);
}

util.inherits(BigIntegerDeserializer, StringBasedDeserializer);

function InetAddressDeserializer() {
  StringBasedDeserializer.call(this, 'gx:InetAddress', types.InetAddress);
}

util.inherits(InetAddressDeserializer, StringBasedDeserializer);

function LocalDateDeserializer() {
  StringBasedDeserializer.call(this, 'gx:LocalDate', types.LocalDate);
}

util.inherits(LocalDateDeserializer, StringBasedDeserializer);

function LocalTimeDeserializer() {
  StringBasedDeserializer.call(this, 'gx:LocalTime', types.LocalTime);
}

util.inherits(LocalTimeDeserializer, StringBasedDeserializer);

function InstantDeserializer() {
  StringBasedDeserializer.call(this, 'gx:Instant', Date);
}

util.inherits(InstantDeserializer, StringBasedDeserializer);

InstantDeserializer.prototype.deserialize = function (obj) {
  return new Date(obj[graphSONValueKey]);
};

function BlobDeserializer() {
  StringBasedDeserializer.call(this, 'dse:Blob', Buffer);
}

util.inherits(BlobDeserializer, StringBasedDeserializer);

BlobDeserializer.prototype.deserialize = function (obj) {
  return new Buffer(obj[graphSONValueKey], 'base64');
};

function PointDeserializer() {
  StringBasedDeserializer.call(this, 'dse:Point', Point);
}

util.inherits(PointDeserializer, StringBasedDeserializer);

function LineStringDeserializer() {
  StringBasedDeserializer.call(this, 'dse:LineString', LineString);
}

util.inherits(LineStringDeserializer, StringBasedDeserializer);

function PolygonDeserializer() {
  StringBasedDeserializer.call(this, 'dse:Polygon', Polygon);
}

util.inherits(PolygonDeserializer, StringBasedDeserializer);

exports.GraphSONReader = GraphSONReader;
exports.register = register;