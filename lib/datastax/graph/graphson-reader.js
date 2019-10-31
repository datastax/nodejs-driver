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

//TODO: Move to graph serializers

const util = require('util');
const types = require('../../types');
const utils = require('../../utils');
const Point = require('../../geometry/point');
const Polygon = require('../../geometry/polygon');
const LineString = require('../../geometry/line-string');
const graphModule = require('./');
const Edge = graphModule.Edge;
const Path = graphModule.Path;
const Property = graphModule.Property;
const Vertex = graphModule.Vertex;
const VertexProperty = graphModule.VertexProperty;

const graphSONTypeKey = '@type';
const graphSONValueKey = '@value';

/**
 * GraphSON2 Reader
 * @ignore
 * @internal
 * @constructor
 */
function GraphSONReader() {
  const deserializerConstructors = [
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
    const s = new C();
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
  const type = obj[graphSONTypeKey];
  if (type) {
    const d = this._deserializers[type];
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
  const keys = Object.keys(obj);
  const result = {};
  for (let i = 0; i < keys.length; i++) {
    result[keys[i]] = this.read(obj[keys[i]]);
  }
  return result;
};

function VertexDeserializer() {
  this.key = 'g:Vertex';
}

VertexDeserializer.prototype.deserialize = function (obj) {
  const value = obj[graphSONValueKey];
  return new Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
};

function VertexPropertyDeserializer() {
  this.key = 'g:VertexProperty';
}

VertexPropertyDeserializer.prototype.deserialize = function (obj) {
  const value = obj[graphSONValueKey];
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
  const value = obj[graphSONValueKey];
  return new Property(
    value['key'],
    this.reader.read(value['value']));
};

function EdgeDeserializer() {
  this.key = 'g:Edge';
}

EdgeDeserializer.prototype.deserialize = function (obj) {
  const value = obj[graphSONValueKey];
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
  const value = obj[graphSONValueKey];
  const objects = value['objects'].map(function objectMapItem(o) {
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
 * @private
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
  let value = obj[graphSONValueKey];
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
  return utils.allocBufferFromString(obj[graphSONValueKey], 'base64');
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

module.exports = GraphSONReader;