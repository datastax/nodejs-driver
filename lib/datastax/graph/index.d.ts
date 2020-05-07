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

import { types } from '../../types';

export namespace graph {
  interface Edge extends Element {
    outV?: Vertex;
    outVLabel?: string;
    inV?: Vertex;
    inVLabel?: string;
    properties?: object;
  }

  interface Element {
    id: any;
    label: string;
  }

  class GraphResultSet implements Iterator<any> {
    constructor(rs: types.ResultSet);

    first(): any;

    toArray(): any[];

    values(): Iterator<any>;

    next(value?: any): IteratorResult<any>;
  }

  interface Path {
    labels: any[];
    objects: any[];
  }

  interface Property {
    value: any
    key: any
  }

  interface Vertex extends Element {
    properties?: { [key: string]: any[] }
  }

  interface VertexProperty extends Element {
    value: any
    key: string
    properties?: any
  }

  function asDouble(value: number): object;

  function asFloat(value: number): object;

  function asInt(value: number): object;

  function asTimestamp(value: Date): object;

  function asUdt(value: object): object;

  interface EnumValue {
    toString(): string
  }

  namespace t {
    const id: EnumValue;
    const key: EnumValue;
    const label: EnumValue;
    const value: EnumValue;
  }

  namespace direction {
    // `in` is a reserved word
    const in_: EnumValue;
    const out: EnumValue;
    const both: EnumValue;
  }
}