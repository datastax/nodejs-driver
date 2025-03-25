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
import GraphResultSet from "./result-set";
import getCustomTypeSerializers from "./custom-type-serializers";
import { asInt, asDouble, asFloat, asTimestamp, asUdt, UdtGraphWrapper, GraphTypeWrapper} from "./wrappers";
import { Edge, Element, Path, Property, Vertex, VertexProperty } from "./structure";
import type { QueryOptions } from "../../client";
import * as types from "../../types";



class EnumValue {
  typeName: any;
  elementName: any;
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

const directionIn = new EnumValue('Direction', 'IN');
/**
 * Represents the edge direction.
 */
const direction = {
  'both': new EnumValue('Direction', 'BOTH'),
  'in': directionIn,
  'out': new EnumValue('Direction', 'OUT'),
  // `in` is a reserved keyword depending on the context
  // TinkerPop JavaScript GLV only exposes `in` but it can lead to issues for TypeScript users and others.
  // Expose an extra property to represent `Direction.IN`.
  'in_': directionIn
};

export interface GraphQueryOptions extends QueryOptions {
  graphLanguage?: string;
  graphName?: string;
  graphReadConsistency?: typeof types.consistencies;
  graphSource?: string;
  graphWriteConsistency?: typeof types.consistencies;
  graphResults?: string;
}

export type GraphOptions = {
  language?: string;
  name?: string;
  readConsistency?: typeof types.consistencies;
  readTimeout?: number;
  source?: string;
  writeConsistency?: typeof types.consistencies;
};

export default {
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

export {
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