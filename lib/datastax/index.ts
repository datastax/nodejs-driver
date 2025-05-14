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



/**
 * DataStax module.
 * <p>
 *   Contains modules and classes to represent functionality that is specific to DataStax products.
 * </p>
 * @module datastax
 */

import graph, {
  asDouble, asFloat, asInt, asTimestamp, asUdt, direction, Edge, Element, getCustomTypeSerializers, GraphResultSet, GraphTypeWrapper, Path, Property,
  t, UdtGraphWrapper, Vertex, VertexProperty
} from "./graph/index";
import search, { DateRange, DateRangeBound, dateRangePrecision } from "./search/index";

export default {
  // Had to do this for api-extractor to remove the internal exports
  graph: {
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
    /** @internal */
    getCustomTypeSerializers,
    GraphResultSet,
    /** @internal */
    GraphTypeWrapper,
    t,
    /** @internal */
    UdtGraphWrapper
  },
  search: {
    DateRange,
    DateRangeBound,
    dateRangePrecision
  }
};

export {
  search, graph
};

export * from "./graph";
export * from "./search";
export * from "./cloud";