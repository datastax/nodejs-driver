/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

/**
 * Graph module.
 * @module graph
 */

const util = require('util');

/**
 * Represents a graph Element.
 * @param id
 * @param label
 * @abstract
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
 * Represents a graph Vertex.
 * @param id
 * @param {String} label
 * @param {Object<string, Array>} properties
 * @extends {Element}
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
 * Represents a graph Edge.
 * @param id
 * @param outV
 * @param {outVLabel} outVLabel
 * @param {String} label
 * @param inV
 * @param {String} inVLabel
 * @param {Object<string, Property>} properties
 * @extends {Element}
 * @constructor
 */
function Edge(id, outV, outVLabel, label, inV, inVLabel, properties) {
  Element.call(this, id, label);
  /**
   * Gets the edge outgoing vertex.
   */
  this.outV = outV;
  /**
   * Gets the label of the outgoing vertex.
   */
  this.outVLabel = outVLabel;
  /**
   * Gets the edge incoming vertex.
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
 * Represents a graph vertex property.
 * @param id
 * @param {String} label
 * @param value
 * @param {Object} properties
 * @extends {Element}
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
 * Represents a property.
 * @param key
 * @param value
 * @constructor
 */
function Property(key, value) {
  this.key = key;
  this.value = value;
}

/**
 * Represents a walk through a graph as defined by a traversal.
 * @param {Array} labels
 * @param {Array} objects
 * @constructor
 */
function Path(labels, objects) {
  this.labels = labels;
  this.objects = objects;
}

exports.Edge = Edge;
exports.Element = Element;
exports.Path = Path;
exports.Property = Property;
exports.GraphResultSet = require('./result-set');
exports.Vertex = Vertex;
exports.VertexProperty = VertexProperty;