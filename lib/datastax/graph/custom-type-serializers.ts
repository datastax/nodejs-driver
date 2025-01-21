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
const { getTypeDefinitionByValue, getUdtTypeDefinitionByValue } = require('./complex-type-helper');
const { Point, Polygon, LineString } = require('../../geometry');
const { Edge } = require('./structure');
const { GraphTypeWrapper, UdtGraphWrapper } = require('./wrappers');
const { Tuple, dataTypes } = types;

const typeKey = '@type';
const valueKey = '@value';

class EdgeDeserializer {
  constructor() {
    this.key = 'g:Edge';
  }

  deserialize(obj) {
    const value = obj[valueKey];
    return new Edge(this.reader.read(value['id']), this.reader.read(value['outV']), value['outVLabel'], value['label'], this.reader.read(value['inV']), value['inVLabel'], this.reader.read(value['properties']));
  }
}

/**
 * Uses toString() instance method and fromString() static method to serialize and deserialize the value.
 * @abstract
 * @private
 */
class StringBasedTypeSerializer {

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
    let value = obj[valueKey];
    if (typeof value !== 'string') {
      value = value.toString();
    }
    return this.targetType.fromString(value);
  }

  serialize(value) {
    return {
      [typeKey]: this.key,
      [valueKey]: value.toString()
    };
  }

  canBeUsedFor(value) {
    return value instanceof this.targetType;
  }
}

class UuidSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('g:UUID', types.Uuid);
  }
}

class LongSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('g:Int64', types.Long);
  }
}

class BigDecimalSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:BigDecimal', types.BigDecimal);
  }
}

class BigIntegerSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:BigInteger', types.Integer);
  }
}

class InetAddressSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:InetAddress', types.InetAddress);
  }
}

class LocalDateSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:LocalDate', types.LocalDate);
  }
}

class LocalTimeSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:LocalTime', types.LocalTime);
  }
}

class InstantSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('gx:Instant', Date);
  }

  serialize(item) {
    return {
      [typeKey]: this.key,
      [valueKey]: item.toISOString()
    };
  }

  deserialize(obj) {
    return new Date(obj[valueKey]);
  }
}

class BlobSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('dse:Blob', Buffer);
  }

  deserialize(obj) {
    return utils.allocBufferFromString(obj[valueKey], 'base64');
  }

  serialize(item) {
    return {
      [typeKey]: this.key,
      [valueKey]: item.toString('base64')
    };
  }
}

class PointSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('dse:Point', Point);
  }
}

class LineStringSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('dse:LineString', LineString);
  }
}

class PolygonSerializer extends StringBasedTypeSerializer {
  constructor() {
    super('dse:Polygon', Polygon);
  }
}

class TupleSerializer {
  constructor() {
    this.key = 'dse:Tuple';
  }

  deserialize(obj) {
    // Skip definitions and go to the value
    const value = obj[valueKey]['value'];

    if (!Array.isArray(value)) {
      throw new Error('Expected Array, obtained: ' + value);
    }

    const result = [];

    for (const element of value) {
      result.push(this.reader.read(element));
    }

    return Tuple.fromArray(result);
  }

  /** @param {Tuple} tuple */
  serialize(tuple) {
    const result = {
      'cqlType': 'tuple',
      'definition': tuple.elements.map(getTypeDefinitionByValue),
      'value': tuple.elements.map(e => this.writer.adaptObject(e))
    };

    return {
      [typeKey]: this.key,
      [valueKey]: result
    };
  }

  canBeUsedFor(value) {
    return value instanceof Tuple;
  }
}

class DurationSerializer {
  constructor() {
    this.key = 'dse:Duration';
  }

  deserialize(obj) {
    // Skip definitions and go to the value
    const value = obj[valueKey];

    return new types.Duration(
      this.reader.read(value['months']), this.reader.read(value['days']), this.reader.read(value['nanos']));
  }

  /** @param {Duration} value */
  serialize(value) {
    return {
      [typeKey]: this.key,
      [valueKey]: {
        'months': value['months'],
        'days': value['days'],
        'nanos': value['nanoseconds'],
      }
    };
  }

  canBeUsedFor(value) {
    return value instanceof types.Duration;
  }
}

class UdtSerializer {
  constructor() {
    this.key = 'dse:UDT';
  }

  deserialize(obj) {
    // Skip definitions and go to the value
    const valueRoot = obj[valueKey];
    const result = {};
    const value = valueRoot['value'];

    valueRoot['definition'].forEach((definition, index) => {
      result[definition.fieldName] = this.reader.read(value[index]);
    });

    return result;
  }

  serialize(udtWrapper) {
    const serializedValue = getUdtTypeDefinitionByValue(udtWrapper);
    // New properties can be added to the existing object without need to clone
    // as getTypeDefinition() returns a new object each time
    serializedValue['value'] = Object.entries(udtWrapper.value).map(([_, v]) => this.writer.adaptObject(v));

    return {
      [typeKey]: this.key,
      [valueKey]: serializedValue
    };
  }

  canBeUsedFor(value) {
    return value instanceof UdtGraphWrapper;
  }
}

class InternalSerializer {
  constructor(name, transformFn) {
    this._name = name;
    this._transformFn = transformFn || (x => x);
  }

  serialize(item) {
    return {
      [typeKey]: this._name,
      [valueKey]: this._transformFn(item)
    };
  }
}

// Associative array of graph type name by CQL type code, used by the type wrapper
const graphSONSerializerByCqlType = {
  [dataTypes.int]: new InternalSerializer('g:Int32'),
  [dataTypes.bigint]: new InternalSerializer('g:Int64'),
  [dataTypes.double]: new InternalSerializer('g:Double'),
  [dataTypes.float]: new InternalSerializer('g:Float'),
  [dataTypes.timestamp]: new InternalSerializer('g:Timestamp', x => x.getTime())
};

class GraphTypeWrapperSerializer {
  constructor() {
    // Use a fixed name that doesn't conflict with TinkerPop and DS Graph
    this.key = 'client:wrapper';
  }

  serialize(wrappedValue) {
    const s = graphSONSerializerByCqlType[wrappedValue.typeInfo.code];

    if (!s) {
      throw new Error(`No serializer found for wrapped value ${wrappedValue}`);
    }

    return s.serialize(wrappedValue.value);
  }

  canBeUsedFor(value) {
    return value instanceof GraphTypeWrapper;
  }
}

const serializersArray = [
  EdgeDeserializer,
  UuidSerializer,
  LongSerializer,
  BigDecimalSerializer,
  BigIntegerSerializer,
  InetAddressSerializer,
  LocalDateSerializer,
  LocalTimeSerializer,
  InstantSerializer,
  BlobSerializer,
  PointSerializer,
  LineStringSerializer,
  PolygonSerializer,
  TupleSerializer,
  UdtSerializer,
  GraphTypeWrapperSerializer,
  DurationSerializer
];

function getCustomSerializers() {
  const customSerializers = {};

  serializersArray.forEach(sConstructor => {
    const instance = new sConstructor();
    if (!instance.key) {
      throw new TypeError(`Key for ${sConstructor} instance not set`);
    }

    customSerializers[instance.key] = instance;
  });

  return customSerializers;
}

module.exports = getCustomSerializers;