/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

import { types } from '../types';

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
}