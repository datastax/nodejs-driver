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
const cassandra = require('cassandra-driver');
const async = require('async');

/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 *
 * Inserts some vertex and edges: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
 */

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  profiles: [
    // Set the graph name in the default execution profile
    new cassandra.ExecutionProfile('default', {
      graphOptions: { name: 'example_graph' }
    })
  ]
});

const modernSchema =
  'schema.config().option("graph.allow_scan").set("true");\n' +
  'schema.propertyKey("name").Text().ifNotExists().create();\n' +
  'schema.propertyKey("age").Int().ifNotExists().create();\n' +
  'schema.propertyKey("relationship_weight").Float().ifNotExists().create();\n' +
  'schema.vertexLabel("person").properties("name", "age").ifNotExists().create();\n' +
  'schema.edgeLabel("knows").properties("relationship_weight").connection("person", "person").ifNotExists().create();';

const modernGraph =
  'Vertex marko = graph.addVertex(label, "person", "name", "marko", "age", 29);\n' +
  'Vertex vadas = graph.addVertex(label, "person", "name", "vadas", "age", 27);\n' +
  'marko.addEdge("knows", vadas, "relationship_weight", 0.5f);\n';

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createGraph(next) {
    const query = 'system.graph("example_graph").ifNotExists().create();';
    // As the graph "example_graph" does not exist yet and
    // it is a system query, we need to set the graph name to `null`
    client.executeGraph(query, null, { graphName: null }, next);
  },
  function createSchema(next) {
    client.executeGraph(modernSchema, next);
  },
  function createVerticesAndEdges(next) {
    client.executeGraph(modernGraph, next);
  },
  function retrieveVertices(next) {
    client.executeGraph('g.V()', function (err, result) {
      if (err) {
        return next(err);
      }
      const vertex = result.first();
      console.log('First vertex: ', vertex);
      next();
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});