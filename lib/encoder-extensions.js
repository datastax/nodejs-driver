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
  return new VertexProperty(this.reader.read(value['id']), value['label'], this.reader.read(value['value']));
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
    value['inVLabel']
  );
};

function PathDeserializer() {
  this.key = 'g:Path'
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
  StringBasedDeserializer.call(this, 'g:UUID', cassandra.types.Uuid);
}

util.inherits(UuidDeserializer, StringBasedDeserializer);

function LongDeserializer() {
  StringBasedDeserializer.call(this, 'g:Int64', cassandra.types.Long);
}

util.inherits(LongDeserializer, StringBasedDeserializer);

function BigDecimalDeserializer() {
  StringBasedDeserializer.call(this, 'gx:BigDecimal', cassandra.types.BigDecimal);
}

util.inherits(BigDecimalDeserializer, StringBasedDeserializer);

function BigIntegerDeserializer() {
  StringBasedDeserializer.call(this, 'gx:BigInteger', cassandra.types.Integer);
}

util.inherits(BigIntegerDeserializer, StringBasedDeserializer);

function InetAddressDeserializer() {
  StringBasedDeserializer.call(this, 'gx:InetAddress', cassandra.types.InetAddress);
}

util.inherits(InetAddressDeserializer, StringBasedDeserializer);

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