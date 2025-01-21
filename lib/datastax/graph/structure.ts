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

const util = require('util');

/**
 * @classdesc
 * Represents a graph Element.
 * @param id
 * @param label
 * @abstract
 * @memberOf module:datastax/graph
 * @constructor
 */
function Element(id, label) {
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

/**
 * @classdesc
 * Represents a graph Vertex.
 * @param id
 * @param {String} label
 * @param {Object<string, Array>} properties
 * @extends {Element}
 * @memberOf module:datastax/graph
 * @constructor
 */
function Vertex(id, label, properties) {
  Element.call(this, id, label);
  /**
   * Gets the vertex properties.
   * @type {Object<string, Array>}
   */
  this.properties = properties;
}

util.inherits(Vertex, Element);

/**
 * @classdesc
 * Represents a graph Edge.
 * @param id
 * @param outV
 * @param {outVLabel} outVLabel
 * @param {String} label
 * @param inV
 * @param {String} inVLabel
 * @param {Object<string, Property>} properties
 * @extends {Element}
 * @memberOf module:datastax/graph
 * @constructor
 */
function Edge(id, outV, outVLabel, label, inV, inVLabel, properties) {
  Element.call(this, id, label);
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
  (function adaptProperties(self) {
    if (properties) {
      const keys = Object.keys(properties);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        self.properties[k] = properties[k].value;
      }
    }
  })(this);
}

util.inherits(Edge, Element);

/**
 * @classdesc
 * Represents a graph vertex property.
 * @param id
 * @param {String} label
 * @param value
 * @param {Object} properties
 * @extends {Element}
 * @memberOf module:datastax/graph
 * @constructor
 */
function VertexProperty(id, label, value, properties) {
  Element.call(this, id, label);
  this.value = value;
  this.key = this.label;
  this.properties = properties;
}

util.inherits(VertexProperty, Element);

/**
 * @classdesc
 * Represents a property.
 * @param key
 * @param value
 * @memberOf module:datastax/graph
 * @constructor
 */
function Property(key, value) {
  this.key = key;
  this.value = value;
}

/**
 * @classdesc
 * Represents a walk through a graph as defined by a traversal.
 * @param {Array} labels
 * @param {Array} objects
 * @memberOf module:datastax/graph
 * @constructor
 */
function Path(labels, objects) {
  this.labels = labels;
  this.objects = objects;
}

module.exports = {
  Edge,
  Element,
  Path,
  Property,
  Vertex,
  VertexProperty
};