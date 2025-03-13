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
import utils from "../../../../lib/utils";


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


/**
 * Creates the modern schema and graph
 * @param {Client} client
 * @param {Function} callback
 */
const createModernGraph = function (client, callback) {
  utils.series([
    next => client.executeGraph(modernSchema, null, { graphName: "name1" }, next),
    next => client.executeGraph(modernGraph, null, { graphName: "name1" }, next)
  ], callback);
};

/**
 * Sets the schema mode to "production".
 * @param {Client} client
 */
const makeStrict = function (client) {
  return client.executeGraph(makeStrictQuery, null, { graphName: 'name1' });
};

/**
 * Sets the allow_scan flag.
 * @param {Client} client
 */
const allowScans = function (client) {
  return client.executeGraph(allowScansQuery, null, { graphName: 'name1' });
};

export default {
  createModernGraph,
  makeStrict,
  allowScans
}