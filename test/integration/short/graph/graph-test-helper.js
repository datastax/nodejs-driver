/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const utils = require('../../../../lib/utils');

module.exports = {

  /**
   * Creates the modern schema and graph
   * @param {Client} client
   * @param {Function} callback
   */
  createModernGraph: function (client, callback) {
    utils.series([
      next => client.executeGraph(modernSchema, null, {graphName: "name1"}, next),
      next => client.executeGraph(modernGraph, null, {graphName: "name1"}, next)
    ], callback);
  },

  /**
   * Sets the schema mode to "production".
   * @param {Client} client
   * @param {Function} callback
   */
  makeStrict: function (client, callback) {
    client.executeGraph(makeStrictQuery, null, { graphName: 'name1'}, callback);
  },

  /**
   * Sets the allow_scan flag.
   * @param {Client} client
   * @param {Function} callback
   */
  allowScans: function (client, callback) {
    client.executeGraph(allowScansQuery, null, { graphName: 'name1'}, callback);
  }
};


const makeStrictQuery = 'schema.config().option("graph.schema_mode").set("production")';

const allowScansQuery = 'schema.config().option("graph.allow_scan").set("true")';

const modernSchema =
  makeStrictQuery + '\n' +
  allowScansQuery + '\n' +
  'schema.propertyKey("name").Text().ifNotExists().create();\n' +
  'schema.propertyKey("age").Int().ifNotExists().create();\n' +
  'schema.propertyKey("lang").Text().ifNotExists().create();\n' +
  'schema.propertyKey("weight").Float().ifNotExists().create();\n' +
  'schema.vertexLabel("person").properties("name", "age").ifNotExists().create();\n' +
  'schema.vertexLabel("software").properties("name", "lang").ifNotExists().create();\n' +
  'schema.edgeLabel("created").properties("weight").connection("person", "software").ifNotExists().create();\n' +
  'schema.edgeLabel("knows").properties("weight").connection("person", "person").ifNotExists().create();';

const modernGraph =
  'Vertex marko = graph.addVertex(label, "person", "name", "marko", "age", 29);\n' +
  'Vertex vadas = graph.addVertex(label, "person", "name", "vadas", "age", 27);\n' +
  'Vertex lop = graph.addVertex(label, "software", "name", "lop", "lang", "java");\n' +
  'Vertex josh = graph.addVertex(label, "person", "name", "josh", "age", 32);\n' +
  'Vertex ripple = graph.addVertex(label, "software", "name", "ripple", "lang", "java");\n' +
  'Vertex peter = graph.addVertex(label, "person", "name", "peter", "age", 35);\n' +
  'marko.addEdge("knows", vadas, "weight", 0.5f);\n' +
  'marko.addEdge("knows", josh, "weight", 1.0f);\n' +
  'marko.addEdge("created", lop, "weight", 0.4f);\n' +
  'josh.addEdge("created", ripple, "weight", 1.0f);\n' +
  'josh.addEdge("created", lop, "weight", 0.4f);\n' +
  'peter.addEdge("created", lop, "weight", 0.2f);';