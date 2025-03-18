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
 * Represents a graph Element.
 * @abstract
 * @memberOf module:datastax/graph
 */
class Element {
  id: any;
  label: string;

  /**
   * @param id
   * @param label
   */
  constructor(id: any, label: string) {
    /**
     * Gets the element id.
     */
    this.id = id;
    /**
     * Gets the element label.
     * @type {String}
     */
    this.label = label;
  }
}

/**
 * Represents a graph Vertex.
 * @extends Element
 * @memberOf module:datastax/graph
 */
class Vertex extends Element {
  properties: { [s: string]: Array<any> };

  /**
   * @param id
   * @param {String} label
   * @param {Object<string, Array>} properties
   */
  constructor(id: any, label: string, properties: { [s: string]: Array<any> }) {
    super(id, label);
    /**
     * Gets the vertex properties.
     * @type {Object<string, Array>}
     */
    this.properties = properties;
  }
}

/**
 * Represents a graph Edge.
 * @extends Element
 * @memberOf module:datastax/graph
 */
class Edge extends Element {
  outV: any;
  outVLabel: string;
  inV: any;
  inVLabel: string;
  properties: { [s: string]: any };

  /**
   * @param id
   * @param outV
   * @param {String} outVLabel
   * @param {String} label
   * @param inV
   * @param {String} inVLabel
   * @param {Object<string, Property>} properties
   */
  constructor(
    id: any,
    outV: any,
    outVLabel: string,
    label: string,
    inV: any,
    inVLabel: string,
    properties: { [s: string]: Property }
  ) {
    super(id, label);
    /**
     * Gets the id of outgoing vertex of the edge.
     */
    this.outV = outV;
    /**
     * Gets the label of the outgoing vertex.
     */
    this.outVLabel = outVLabel;
    /**
     * Gets the id of the incoming vertex of the edge.
     */
    this.inV = inV;

    /**
     * Gets the label of the incoming vertex.
     */
    this.inVLabel = inVLabel;
    /**
     * Gets the properties of the edge as an associative array.
     * @type {Object}
     */
    this.properties = {};
    this.adaptProperties(properties);
  }

  private adaptProperties(properties: { [s: string]: Property }): void {
    if (properties) {
      const keys = Object.keys(properties);
      for (const key of keys) {
        this.properties[key] = properties[key].value;
      }
    }
  }
}

/**
 * Represents a graph vertex property.
 * @extends Element
 * @memberOf module:datastax/graph
 */
class VertexProperty extends Element {
  value: any;
  key: string;
  properties: object;

  /**
   * @param id
   * @param {String} label
   * @param value
   * @param {Object} properties
   */
  constructor(id: any, label: string, value: any, properties: object) {
    super(id, label);
    this.value = value;
    this.key = this.label;
    this.properties = properties;
  }
}

/**
 * Represents a property.
 * @memberOf module:datastax/graph
 */
class Property {
  key: string;
  value: any;

  /**
   * @param key
   * @param value
   */
  constructor(key: string, value: any) {
    this.key = key;
    this.value = value;
  }
}

/**
 * Represents a walk through a graph as defined by a traversal.
 * @memberOf module:datastax/graph
 */
class Path {
  labels: Array<any>;
  objects: Array<any>;

  /**
   * @param {Array} labels
   * @param {Array} objects
   */
  constructor(labels: Array<any>, objects: Array<any>) {
    this.labels = labels;
    this.objects = objects;
  }
}

export {
  Edge,
  Element,
  Path,
  Property,
  Vertex,
  VertexProperty
};