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

/**
 * Graph module.
 * @module datastax/graph
 */

const GraphResultSet = require('./result-set');
const getCustomTypeSerializers = require('./custom-type-serializers');
const { asInt, asDouble, asFloat, asTimestamp, asUdt, UdtGraphWrapper, GraphTypeWrapper} = require('./wrappers');
const { Edge, Element, Path, Property, Vertex, VertexProperty } = require('./structure');

class EnumValue {
  constructor(typeName, elementName) {
    this.typeName = typeName;
    this.elementName = elementName;
  }

  toString() {
    return this.elementName;
  }
}

/**
 * Represents a collection of tokens for more concise Traversal definitions.
 */
const t = {
  id: new EnumValue('T', 'id'),
  key: new EnumValue('T', 'key'),
  label: new EnumValue('T', 'label'),
  value: new EnumValue('T', 'value'),
};

/**
 * Represents the edge direction.
 */
const direction = {
  'both': new EnumValue('Direction', 'BOTH'),
  'in': new EnumValue('Direction', 'IN'),
  'out': new EnumValue('Direction', 'OUT')
};

// `in` is a reserved keyword depending on the context
// TinkerPop JavaScript GLV only exposes `in` but it can lead to issues for TypeScript users and others.
// Expose an extra property to represent `Direction.IN`.
direction.in_ = direction.in;

module.exports = {
  Edge,
  Element,
  Path,
  Property,
  Vertex,
  VertexProperty,

  asInt,
  asDouble,
  asFloat,
  asTimestamp,
  asUdt,
  direction,
  getCustomTypeSerializers,
  GraphResultSet,
  GraphTypeWrapper,
  t,
  UdtGraphWrapper
};