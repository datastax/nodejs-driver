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

var util = require('util');

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
 * @param properties
 * @extends {Element}
 * @constructor
 */
function Vertex(id, label, properties) {
  Element.call(this, id, label);
  /**
   * Gets the vertex properties.
   * @type {Array}
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
 * @extends {Element}
 * @constructor
 */
function Edge(id, outV, outVLabel, label, inV, inVLabel) {
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
}

util.inherits(Edge, Element);

/**
 * Represents a graph vertex property.
 * @param id
 * @param {String} label
 * @param value
 * @extends {Element}
 * @constructor
 */
function VertexProperty(id, label, value) {
  Element.call(this, id, label);
  this.value = value;
  this.key = this.label;
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