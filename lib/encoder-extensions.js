/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var cassandra = require('cassandra-driver');
var util = require('util');
var Geometry = require('./geometry/geometry');
var Point = require('./geometry/point');
var Polygon = require('./geometry/polygon');
var LineString = require('./geometry/line-string');
var graphModule = require('./graph/');
var Edge = graphModule.Edge;
var Path = graphModule.Path;
var Property = graphModule.Property;
var Vertex = graphModule.Vertex;
var VertexProperty = graphModule.VertexProperty;

var dseDecoders = {
  'org.apache.cassandra.db.marshal.LineStringType': decodeLineString,
  'org.apache.cassandra.db.marshal.PointType': decodePoint,
  'org.apache.cassandra.db.marshal.PolygonType': decodePolygon
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
  if (buffer != null && type.code === cassandra.types.dataTypes.custom) {
    var func = dseDecoders[type.info];
    if (func) {
      return func.call(this, buffer);
    }
  }
  return this.baseDecode(buffer, type);
}

function encodeDse(value, typeInfo) {
  if (value && value instanceof Geometry) {
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

/**
 * GraphSON2 Reader
 * @constructor
 */
function GraphSONReader() {
  var deserializerConstructors = [
    VertexSerializer,
    EdgeSerializer,
    VertexPropertySerializer,
    PropertySerializer,
    PathSerializer,
    UuidSerializer,
    InstantSerializer,
    LongSerializer,
    BigDecimalSerializer,
    BigIntegerSerializer,
    InetAddressSerializer,
    BlobSerializer,
    PointSerializer,
    LineStringSerializer,
    PolygonSerializer
  ];
  this._deserializers = {};
  deserializerConstructors.forEach(function (C) {
    var s = new C();
    s.reader = this;
    this._deserializers[s.key] = s;
  }, this);
}

GraphSONReader.prototype.read = function (obj) {
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

function VertexSerializer() {
  this.key = 'g:Vertex';
}

VertexSerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
};

function VertexPropertySerializer() {
  this.key = 'g:VertexProperty';
}

VertexPropertySerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new VertexProperty(this.reader.read(value['id']), value['label'], this.reader.read(value['value']));
};

function PropertySerializer() {
  this.key = 'g:Property';
}

PropertySerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Property(
    value['key'],
    this.reader.read(value['value']));
};

function EdgeSerializer() {
  this.key = 'g:Edge';
}

EdgeSerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  return new Edge(
    this.reader.read(value['id']),
    this.reader.read(value['outV']),
    value['outVLabel'],
    value['label'],
    this.reader.read(value['inV']),
    value['inVLabel']
  );
};

function PathSerializer() {
  this.key = 'g:Path'
}

PathSerializer.prototype.deserialize = function (obj) {
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
function StringBasedSerializer(key, targetType) {
  if (!key) {
    throw new Error('Serializer must provide a type key');
  }
  if (!targetType) {
    throw new Error('Serializer must provide a target type');
  }
  this.key = key;
  this.targetType = targetType;
}

StringBasedSerializer.prototype.deserialize = function (obj) {
  var value = obj[graphSONValueKey];
  if (typeof value !== 'string') {
    value = value.toString();
  }
  return this.targetType.fromString(value);
};

function UuidSerializer() {
  StringBasedSerializer.call(this, 'g:UUID', cassandra.types.Uuid);
}

util.inherits(UuidSerializer, StringBasedSerializer);

function LongSerializer() {
  StringBasedSerializer.call(this, 'g:Int64', cassandra.types.Long);
}

util.inherits(LongSerializer, StringBasedSerializer);

function BigDecimalSerializer() {
  StringBasedSerializer.call(this, 'gx:BigDecimal', cassandra.types.BigDecimal);
}

util.inherits(BigDecimalSerializer, StringBasedSerializer);

function BigIntegerSerializer() {
  StringBasedSerializer.call(this, 'gx:BigInteger', cassandra.types.Integer);
}

util.inherits(BigIntegerSerializer, StringBasedSerializer);

function InetAddressSerializer() {
  StringBasedSerializer.call(this, 'gx:InetAddress', cassandra.types.InetAddress);
}

util.inherits(InetAddressSerializer, StringBasedSerializer);

function InstantSerializer() {
  StringBasedSerializer.call(this, 'gx:Instant', Date);
}

util.inherits(InstantSerializer, StringBasedSerializer);

InstantSerializer.prototype.deserialize = function (obj) {
  return new Date(obj[graphSONValueKey]);
};

function BlobSerializer() {
  StringBasedSerializer.call(this, 'dse:Blob', Buffer);
}

util.inherits(BlobSerializer, StringBasedSerializer);

BlobSerializer.prototype.deserialize = function (obj) {
  return new Buffer(obj[graphSONValueKey], 'base64');
};

function PointSerializer() {
  StringBasedSerializer.call(this, 'dse:Point', Point);
}

util.inherits(PointSerializer, StringBasedSerializer);

function LineStringSerializer() {
  StringBasedSerializer.call(this, 'dse:LineString', LineString);
}

util.inherits(LineStringSerializer, StringBasedSerializer);

function PolygonSerializer() {
  StringBasedSerializer.call(this, 'dse:Polygon', Polygon);
}

util.inherits(PolygonSerializer, StringBasedSerializer);

exports.GraphSONReader = GraphSONReader;
exports.register = register;