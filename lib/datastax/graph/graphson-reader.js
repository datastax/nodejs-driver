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
class GraphSONReader {
  constructor() {
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

  read(obj) {
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
  }

  _deserializeObject(obj) {
    const keys = Object.keys(obj);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      result[keys[i]] = this.read(obj[keys[i]]);
    }
    return result;
  }
}

class VertexDeserializer {
  constructor() {
    this.key = 'g:Vertex';
  }
  deserialize(obj) {
    const value = obj[graphSONValueKey];
    return new Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
  }
}

class VertexPropertyDeserializer {
  constructor() {
    this.key = 'g:VertexProperty';
  }
  deserialize(obj) {
    const value = obj[graphSONValueKey];
    return new VertexProperty(this.reader.read(value['id']), value['label'], this.reader.read(value['value']), this.reader.read(value['properties']));
  }
}

class PropertyDeserializer {
  constructor() {
    this.key = 'g:Property';
  }
  deserialize(obj) {
    const value = obj[graphSONValueKey];
    return new Property(value['key'], this.reader.read(value['value']));
  }
}

class EdgeDeserializer {
  constructor() {
    this.key = 'g:Edge';
  }
  deserialize(obj) {
    const value = obj[graphSONValueKey];
    return new Edge(this.reader.read(value['id']), this.reader.read(value['outV']), value['outVLabel'], value['label'], this.reader.read(value['inV']), value['inVLabel'], this.reader.read(value['properties']));
  }
}

class PathDeserializer {
  constructor() {
    this.key = 'g:Path';
  }
  deserialize(obj) {
    const value = obj[graphSONValueKey];
    const objects = value['objects'].map(function objectMapItem(o) {
      return this.reader.read(o);
    }, this);
    return new Path(this.reader.read(value['labels']), objects);
  }
}

/**
 * Uses toString() instance method and fromString() static method to serialize and deserialize the value.
 * @abstract
 * @private
 */
class StringBasedDeserializer {

  /**
   * Creates a new instance of the deserializer.
   * @param {String} key
   * @param {Function} targetType
   */
  constructor(key, targetType) {
    if (!key) {
      throw new Error('Deserializer must provide a type key');
    }
    if (!targetType) {
      throw new Error('Deserializer must provide a target type');
    }
    this.key = key;
    this.targetType = targetType;
  }

  deserialize(obj) {
    let value = obj[graphSONValueKey];
    if (typeof value !== 'string') {
      value = value.toString();
    }
    return this.targetType.fromString(value);
  }
}

class UuidDeserializer extends StringBasedDeserializer {
  constructor() {
    super('g:UUID', types.Uuid);
  }
}

class LongDeserializer extends StringBasedDeserializer {
  constructor() {
    super('g:Int64', types.Long);
  }
}

class BigDecimalDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:BigDecimal', types.BigDecimal);
  }
}

class BigIntegerDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:BigInteger', types.Integer);
  }
}

class InetAddressDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:InetAddress', types.InetAddress);
  }
}

class LocalDateDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:LocalDate', types.LocalDate);
  }
}

class LocalTimeDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:LocalTime', types.LocalTime);
  }
}

class InstantDeserializer extends StringBasedDeserializer {
  constructor() {
    super('gx:Instant', Date);
  }

  deserialize(obj) {
    return new Date(obj[graphSONValueKey]);
  }
}

class BlobDeserializer extends StringBasedDeserializer {
  constructor() {
    super('dse:Blob', Buffer);
  }

  deserialize(obj) {
    return utils.allocBufferFromString(obj[graphSONValueKey], 'base64');
  }
}

class PointDeserializer extends StringBasedDeserializer {
  constructor() {
    super('dse:Point', Point);
  }
}

class LineStringDeserializer extends StringBasedDeserializer {
  constructor() {
    super('dse:LineString', LineString);
  }
}

class PolygonDeserializer extends StringBasedDeserializer {
  constructor() {
    super('dse:Polygon', Polygon);
  }
}

module.exports = GraphSONReader;