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
const sinon = require('sinon');
const { assert } = require('chai');

const util = require('util');
const Client = require('../../../../lib/client');
const helper = require('../../../test-helper');
const vdescribe = helper.vdescribe;
const vit = helper.vit;
const { Point, LineString, Polygon } = require('../../../../lib/geometry');
const types = require('../../../../lib/types');
const { InetAddress, Uuid, Tuple } = types;
const ExecutionProfile = require('../../../../lib/execution-profile').ExecutionProfile;
const utils = require('../../../../lib/utils');
const graphModule = require('../../../../lib/datastax/graph');
const { asInt, asFloat, asUdt, t, Edge, direction } = graphModule;
const { graphProtocol } = require('../../../../lib/datastax/graph/options');
const graphTestHelper = require('./graph-test-helper');

const graphLanguageBytecode = 'bytecode-json';
let schemaCounter = 1000;

vdescribe('dse-5.0', 'Client @SERVER_API', function () {
  this.timeout(120000);
  before(done => helper.ccm.startAll(1, { workloads: ['graph'] }, done));
  after(helper.ccm.remove.bind(helper.ccm));

  context('with classic graph engine', () => {
    before(async () => {
      const client = new Client(helper.getOptions());
      try {
        await client.connect();
        await client.execute(helper.queries.basic);
        const query = `system.graph("name1")
          .ifNotExists()
          ${helper.isDseGreaterThan('6.8') ? '.classicEngine()' : ''}
          .create()`;
        await client.executeGraph(query, null, { graphName: null });
        await graphTestHelper.makeStrict(client);
        await graphTestHelper.allowScans(client);
      } finally {
        await client.shutdown();
      }
    });

    describe('#connect()', function () {
      it('should obtain DSE workload', function (done) {
        const client = newInstance();
        client.connect(function (err) {
          assert.ifError(err);
          const host = client.hosts.values()[0];
          if (helper.isDseGreaterThan('5.1')) {
            assert.deepEqual(host.workloads, [ 'Cassandra', 'Graph' ]);
          }
          else {
            assert.deepEqual(host.workloads, [ 'Cassandra' ]);
          }
          done();
        });
      });
    });

    describe('#executeGraph()', function () {
      it('should execute a simple graph query', wrapClient(function (client, done) {
        client.executeGraph('g.V()', null, null, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.info);
          //defined but null
          assert.strictEqual(result.pageState, null);
          done();
        });
      }));
      context('with modern schema', function () {

        // See reference graph here
        // http://tinkerpop.apache.org/docs/current/reference/
        before(wrapClient((client, done) => graphTestHelper.createModernGraph(client, done)));

        it('should retrieve graph vertices', wrapClient(function (client, done) {
          const query = 'g.V().has("name", "marko").out("knows")';
          client.executeGraph(query, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 2);
            result.forEach(function (vertex) {
              assert.strictEqual(vertex.type, 'vertex');
              assert.ok(vertex.properties.name);
            });
            done();
          });
        }));
        it('should retrieve graph edges', wrapClient(function (client, done) {
          client.executeGraph('g.E().hasLabel("created")', function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.length > 1);
            result.forEach(function (edge) {
              assert.strictEqual(edge.type, 'edge');
              assert.ok(edge.label);
              assert.strictEqual(typeof edge.properties.weight, 'number');
            });
            done();
          });
        }));
        it('should support named parameters', wrapClient(function (client, done) {
          const query = 'g.V().has("name", myName)';
          client.executeGraph(query, {myName: "marko"}, null, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            const vertex = result.first();
            assert.ok(vertex);
            assert.strictEqual(vertex.type, 'vertex');
            done();
          });
        }));
        it('should support multiple named parameters', wrapClient(function (client, done) {
          client.executeGraph('[a, b]', {a: 10, b: 20}, null, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 2);
            const arr = result.toArray();
            assert.strictEqual(arr[0], 10);
            assert.strictEqual(arr[1], 20);
            done();
          });
        }));
        it('should handle vertex id as parameter', wrapClient(function (client, done) {
          client.executeGraph("g.V().hasLabel('person').has('name', name)", {name: "marko"}, null, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            const vertex = result.first();
            assert.strictEqual(vertex.properties.name[0].value, 'marko');
            client.executeGraph("g.V(vertex_id)", {vertex_id: vertex.id}, null, function(err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              assert.deepEqual(result.first(), vertex);
              done();
            });
          });
        }));
        it('should handle edge id as parameter', wrapClient(function (client, done) {
          client.executeGraph("g.E().has('weight', weight)", {weight:0.2}, null, function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            const edge = result.first();
            assert.strictEqual(edge.properties.weight, 0.2);
            assert.strictEqual(edge.inVLabel, 'software');
            const inVid = edge.inV;
            assert.strictEqual(edge.outVLabel, 'person');

            client.executeGraph("g.E(edge_id).inV()", {edge_id: edge.id}, null, function(err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              const lop = result.first();
              assert.deepEqual(lop.id, inVid);
              done();
            });
          });
        }));
        it('should handle result object of mixed types', wrapClient(function(client, done) {
          const query = "g.V().hasLabel('software').as('a', 'b', 'c')." +
            "select('a','b', 'c')." +
            "by('name')." +
            "by('lang')." +
            "by(__.in('created').fold())";
          client.executeGraph(query, function (err, result) {
            assert.ifError(err);
            assert.ok(result);

            // Ensure that we got 'lop' and 'ripple' for property a.
            assert.strictEqual(result.length, 2);
            const results = result.toArray();
            const names = results.map(function (v) {
              return v.a;
            });
            assert.ok(names.indexOf('lop') !== -1);
            assert.ok(names.indexOf('ripple') !== -1);

            results.forEach(function (result) {
              // The row should represent a map with a, b, and c keys.
              assert.ok(result.a);
              assert.ok(result.b);
              assert.ok(result.c);
              assert.ok(!result.e);
              // both software are written in java.
              assert.strictEqual(result.b, "java");

              if(result.a === 'lop') {
                // 'c' should contain marko, josh, peter.
                assert.strictEqual(result.c.length, 3);
                const creators = result.c.map(function (v) {
                  return v.properties.name[0].value;
                });
                assert.ok(creators.indexOf('marko') !== -1);
                assert.ok(creators.indexOf('josh') !== -1);
                assert.ok(creators.indexOf('peter') !== -1);
              } else {
                // ripple, 'c' should contain josh.
                assert.strictEqual(result.c.length, 1);
                assert.strictEqual(result.c[0].properties.name[0].value, 'josh');
              }

            });
            done();
          });
        }));
        it('should retrieve path with labels', wrapClient(function (client, done) {
          // find all path traversals for a person whom Marko knows that has created software and what
          // that software is.
          // The paths should be:
          // marko -> knows -> josh -> created -> lop
          // marko -> knows -> josh -> created -> ripple
          const query = "g.V().hasLabel('person').has('name', 'marko').as('a')" +
            ".outE('knows').as('b').inV().as('c', 'd')" +
            ".outE('created').as('e', 'f', 'g').inV().as('h').path()";

          client.executeGraph(query, function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            const results = result.toArray();
            // There should only be two paths.
            assert.strictEqual(results.length, 2);
            results.forEach(function(path) {
              // ensure the labels are organized as requested.
              const labels = path.labels;
              assert.strictEqual(labels.length, 5);
              assert.deepEqual(labels, [['a'], ['b'], ['c', 'd'], ['e', 'f', 'g'], ['h']]);

              // ensure the returned path matches what was expected and that each object
              // has the expected contents.
              const objects = path.objects;
              assert.strictEqual(objects.length, 5);
              const marko = objects[0];
              const knows = objects[1];
              const josh = objects[2];
              const created = objects[3];
              const software = objects[4];

              // marko
              assert.strictEqual(marko.label, 'person');
              assert.strictEqual(marko.type, 'vertex');
              assert.strictEqual(marko.properties.name[0].value, 'marko');
              assert.strictEqual(marko.properties.age[0].value, 29);

              // knows
              assert.strictEqual(knows.label, 'knows');
              assert.strictEqual(knows.type, 'edge');
              assert.strictEqual(knows.properties.weight, 1);
              assert.strictEqual(knows.outVLabel, 'person');
              assert.deepEqual(knows.outV, marko.id);
              assert.strictEqual(knows.inVLabel, 'person');
              assert.deepEqual(knows.inV, josh.id);

              // josh
              assert.strictEqual(josh.label, 'person');
              assert.strictEqual(josh.type, 'vertex');
              assert.strictEqual(josh.properties.name[0].value, 'josh');
              assert.strictEqual(josh.properties.age[0].value, 32);

              // who created
              assert.strictEqual(created.label, 'created');
              assert.strictEqual(created.type, 'edge');
              assert.strictEqual(created.outVLabel, 'person');
              assert.deepEqual(created.outV, josh.id);
              assert.strictEqual(created.inVLabel, 'software');
              assert.deepEqual(created.inV, software.id);

              // software
              if(software.properties.name[0].value === 'lop') {
                assert.strictEqual(created.properties.weight, 0.4);
              } else {
                assert.strictEqual(created.properties.weight, 1.0);
                assert.strictEqual(software.properties.name[0].value, 'ripple');
              }

              assert.strictEqual(software.label, 'software');
              assert.strictEqual(software.type, 'vertex');
              assert.strictEqual(software.properties.lang[0].value, 'java');
            });
            done();
          });
        }));
        it('should return zero results', wrapClient(function (client, done) {
          client.executeGraph("g.V().hasLabel('notALabel')", function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.toArray().length, 0);
            done();
          });
        }));

        context('with bytecode-json as graph language', function () {
          it('should retrieve Vertex instances', wrapClient(function (client, done) {
            const query = JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [['V'], ['hasLabel', 'person']]
              }
            });
            client.executeGraph(query, null, { graphLanguage: graphLanguageBytecode }, function (err, result) {
              assert.ifError(err);
              helper.assertInstanceOf(result, graphModule.GraphResultSet);
              const arr = result.toArray();
              assert.ok(arr.length > 0);
              arr.forEach(function (v) {
                helper.assertInstanceOf(v, graphModule.Vertex);
                assert.ok(v.label);
                helper.assertInstanceOf(v.properties['age'], Array);
                assert.strictEqual(v.properties['age'].length, 1);
                helper.assertInstanceOf(v.properties['age'][0], graphModule.VertexProperty);
                assert.strictEqual(typeof v.properties['age'][0].value, 'number');
              });
              done();
            });
          }));

          it('should support object instances as traversals with query writers', async () => {
            const traversal = { something: '1' };

            const queryWriter = sinon.spy(_ => JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [['V'], ['hasLabel', 'person']]
              }
            }));

            const queryWriterFactory = sinon.spy(() => queryWriter);

            const client = newInstance();
            const options = {
              queryWriterFactory,
              graphLanguage: graphLanguageBytecode,
              graphResults: graphProtocol.graphson2
            };

            const result = await client.executeGraph(traversal, null, options);
            const arr = result.toArray();
            assert.isArray(arr);
            assert.isAbove(arr.length, 0);
            assert.strictEqual(queryWriterFactory.callCount, 1);
            assert.deepEqual(queryWriterFactory.getCall(0).args, [ graphProtocol.graphson2 ]);
            assert.strictEqual(queryWriter.callCount, 1);
            assert.deepEqual(queryWriter.getCall(0).args, [ traversal ]);
            await client.shutdown();
          });

          vit('dse-5.0.9', 'should parse bulked results', wrapClient(function (client, done) {
            const query = JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [['V'], ['hasLabel', 'person'], ['has', 'name', 'marko'], ['outE'], ['label']]
              }
            });
            client.executeGraph(query, null, { graphLanguage: 'bytecode-json' }, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.deepEqual(result.toArray(), ['created', 'knows', 'knows']);
              done();
            });
          }));
          it('should parse nested VertexProperties', wrapClient(function (client, done) {
            let vertex;
            utils.series([
              function createSchema (next) {
                const schemaQuery = '' +
                  'schema.propertyKey("graphson2_sub_prop").Text().create()\n' +
                  'schema.propertyKey("graphson2_meta_prop").Text().properties("graphson2_sub_prop").create()\n' +
                  'schema.vertexLabel("graphson2_meta_v").properties("graphson2_meta_prop").create()';
                client.executeGraph(schemaQuery, next);
              },
              function createVertex (next) {
                const query = "g.addV('graphson2_meta_v').property('graphson2_meta_prop', 'hello')";
                client.executeGraph(query, function (err, result) {
                  assert.ifError(err);
                  assert.ok(result);
                  assert.strictEqual(result.length, 1);
                  vertex = result.first();
                  next();
                });
              },
              function extendProperty (next) {
                const query = "g.V(vId).next().property('graphson2_meta_prop').property('graphson2_sub_prop', 'hi')";
                client.executeGraph(query, {vId:vertex.id}, function (err, result) {
                  assert.ifError(err);
                  assert.ok(result);
                  assert.strictEqual(result.length, 1);
                  next();
                });
              },
              function validateVertex (next) {
                const query = JSON.stringify({
                  '@type': 'g:Bytecode',
                  '@value': {
                    'step': [['V', vertex.id]]
                  }
                });
                client.executeGraph(query, null, { graphLanguage: 'bytecode-json' }, function (err, result) {
                  assert.ifError(err);
                  const nVertex = result.first();
                  const meta_prop = nVertex.properties.graphson2_meta_prop[0];
                  assert.strictEqual(meta_prop.value, 'hello');
                  assert.deepEqual(meta_prop.properties, { graphson2_sub_prop: 'hi' });
                  next();
                });
              }
            ], done);
          }));
          it('should retrieve Edge instances', wrapClient(function (client, done) {
            const query = JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [['E'], ['hasLabel', 'created']]
              }
            });
            client.executeGraph(query, null, { graphLanguage: 'bytecode-json' }, function (err, result) {
              assert.ifError(err);
              helper.assertInstanceOf(result, graphModule.GraphResultSet);
              const arr = result.toArray();
              arr.forEach(function (e) {
                helper.assertInstanceOf(e, graphModule.Edge);
                assert.ok(e.outV);
                assert.ok(e.outVLabel);
                assert.ok(e.inV);
                assert.ok(e.inVLabel);
                assert.ok(e.properties);
                assert.strictEqual(typeof e.properties.weight, 'number');
              });
              done();
            });
          }));
          it('should retrieve a Int64 scalar', wrapClient(function (client, done) {
            const query = JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [["V"], ["count"]]
              }
            });
            client.executeGraph(query, null, { graphLanguage: 'bytecode-json' }, function (err, result) {
              assert.ifError(err);
              helper.assertInstanceOf(result, graphModule.GraphResultSet);
              const count = result.first();
              helper.assertInstanceOf(count, types.Long);
              done();
            });
          }));
          it('should allow graph language to be set from the execution profile', wrapClient(function (client, done) {
            const query = JSON.stringify({
              '@type': 'g:Bytecode',
              '@value': {
                'step': [["V"]]
              }
            });
            client.executeGraph(query, null, { executionProfile: 'graph-profile1' }, function (err, result) {
              assert.ifError(err);
              helper.assertInstanceOf(result, graphModule.GraphResultSet);
              const arr = result.toArray();
              arr.forEach(function (v) {
                helper.assertInstanceOf(v, graphModule.Vertex);
              });
              done();
            });
          }, { profiles: [ new ExecutionProfile('graph-profile1', { graphOptions: { language: 'bytecode-json' } }) ]}));
        });
      });
      it('should use list as a parameter', wrapClient(function(client, done) {
        const characters = ['Mario', "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi"];

        utils.series([
          function createSchema (seriesNext) {
            const schemaQuery = '' +
              'schema.propertyKey("characterName").Text().create();\n' +
              'schema.vertexLabel("character").properties("characterName").create();';
            client.executeGraph(schemaQuery, seriesNext);
          },
          function loadGraph (seriesNext) {
            const query =
              "characters.each { character -> \n" +
              "    graph.addVertex(label, 'character', 'characterName', character);\n" +
              "}";
            client.executeGraph(query, {characters:characters}, null, seriesNext);
          },
          function retrieveCharacters (seriesNext) {
            client.executeGraph("g.V().hasLabel('character').values('characterName')", function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              const results = result.toArray();
              assert.strictEqual(results.length, characters.length);
              characters.forEach(function (c) {
                assert.ok(results.indexOf(c) !== -1);
              });
              seriesNext();
            });
          }
        ], done);
      }));
      it('should use map as a parameter', wrapClient(function(client, done) {
        const name = 'Albert Einstein';
        const year = 1879;
        const field = "Physics";
        const citizenship = ['Kingdom of Württemberg', 'Switzerland', 'Austria', 'Germany', 'United States'];
        let id;

        utils.series([
          function createSchema (next) {
            const schemaQuery = '' +
              'schema.propertyKey("year_born").Int().create()\n' +
              'schema.propertyKey("field").Text().create()\n' +
              'schema.propertyKey("scientist_name").Text().create()\n' +
              'schema.propertyKey("country_name").Text().create()\n' +
              'schema.vertexLabel("scientist").properties("scientist_name", "year_born", "field").create()\n' +
              'schema.vertexLabel("country").properties("country_name").create()\n' +
              'schema.edgeLabel("had_citizenship").connection("scientist", "country").create()';
            client.executeGraph(schemaQuery, next);
          },
          function createEinstein (next) {
            // Create a vertex for Einstein and then add a vertex for each country of citizenship and an outgoing
            // edge from Einstein to country he had citizenship in.
            const query =
              "Vertex scientist = graph.addVertex(label, 'scientist', 'scientist_name', m.name, 'year_born', m.year_born, 'field', m.field)\n" +
              "m.citizenship.each { c -> \n" +
              "    Vertex country = graph.addVertex(label, 'country', 'country_name', c);\n" +
              "    scientist.addEdge('had_citizenship', country);\n" +
              "}";

            client.executeGraph(query, {m: {name: name, year_born: year, citizenship: citizenship, field: field}}, null, next);
          },
          function lookupVertex (next) {
            // Ensure Einstein was properly added.
            client.executeGraph("g.V().hasLabel('scientist').has('scientist_name', name)", {name: name}, null, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              const vertex = result.first();
              assert.ok(vertex);
              assert.equal(vertex.type, "vertex");
              assert.equal(vertex.label, "scientist");
              assert.equal(vertex.properties.scientist_name[0].value, name);
              assert.equal(vertex.properties.field[0].value, field);
              assert.equal(vertex.properties.year_born[0].value, year);
              // Ensure edges are retrievable by vertex id.
              id = vertex.id;
              next();
            });
          },
          function validateEdges (next) {
            client.executeGraph("g.V(vId).outE('had_citizenship').inV().values('country_name')", {vId: id}, null, function (err, result) {
              assert.ok(result);
              assert.deepEqual(result.toArray(), citizenship);
              next();
            });
          }
        ], done);
      }));
      it('should be able to create and retrieve a multi-cardinality vertex property', wrapClient(function(client, done) {
        let id;
        utils.series([
          function createSchema (next) {
            const schemaQuery = 'schema.propertyKey("multi_prop").Text().multiple().create()\n' +
              'schema.vertexLabel("multi_v").properties("multi_prop").create()';
            client.executeGraph(schemaQuery, next);
          },
          function createVertex (next) {
            const query = "g.addV('multi_v').property('multi_prop', 'Hello').property('multi_prop', 'Sweet').property('multi_prop', 'World')";
            client.executeGraph(query, function (err, result) {
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              const vertex = result.first();
              const props = vertex.properties.multi_prop.map(function (v) {
                return v.value;
              });
              assert.deepEqual(props, ['Hello', 'Sweet', 'World']);
              id = vertex.id;
              next();
            });
          },
          function retrievePropertyOnly (next) {
            client.executeGraph("g.V(vId).properties('multi_prop')", {vId:id}, function (err, result) {
              assert.ok(result);
              assert.strictEqual(result.length, 3);
              const results = result.toArray();
              const props = results.map(function (v) {
                return v.value;
              });
              assert.deepEqual(props, ['Hello', 'Sweet', 'World']);
              next();
            });
          }
        ], done);
      }));
      it('should be able to create and retrieve vertex property with meta properties', wrapClient(function(client, done) {
        let vertex;
        utils.series([
          function createSchema (next) {
            const schemaQuery = '' +
              'schema.propertyKey("sub_prop").Text().create()\n' +
              'schema.propertyKey("sub_prop2").Text().create()\n' +
              'schema.propertyKey("meta_prop").Text().properties("sub_prop", "sub_prop2").create()\n' +
              'schema.vertexLabel("meta_v").properties("meta_prop").create()';
            client.executeGraph(schemaQuery, next);
          },
          function createVertex (next) {
            client.executeGraph("g.addV('meta_v').property('meta_prop', 'hello')", function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              vertex = result.first();
              next();
            });
          },
          function extendProperty (next) {
            client.executeGraph("g.V(vId).next().property('meta_prop').property('sub_prop', 'hi')", {vId:vertex.id}, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              assert.deepEqual(result.first(), {key: 'sub_prop', value: 'hi'});
              next();
            });
          },
          function extendProperty2 (next) {
            client.executeGraph("g.V(vId).next().property('meta_prop').property('sub_prop2', 'hi2')", {vId:vertex.id}, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              assert.deepEqual(result.first(), {key: 'sub_prop2', value: 'hi2'});
              next();
            });
          },
          function validateVertex (next) {
            client.executeGraph("g.V(vId)", {vId: vertex.id}, function (err, result) {
              assert.ifError(err);
              const nVertex = result.first();
              const meta_prop = nVertex.properties.meta_prop[0];
              assert.strictEqual(meta_prop.value, 'hello');
              assert.deepEqual(meta_prop.properties, {sub_prop: 'hi', sub_prop2: 'hi2'});
              next();
            });
          }
        ], done);
      }));
      it('should handle multiple vertex creation queries simultaneously', wrapClient(function(client, done) {
        const addQuery = "g.addV('simu').property('username', username).property('uuid', uuid).property('number', number)";
        const vertexCount = 100;
        const users = [];

        const flattenProperties = function(vertex) {
          // Convert properties map to a map of the key name and the value of the first element.
          return Object.keys(vertex.properties).reduce(function(m, k) {
            m[k] = vertex.properties[k][0].value;
            return m;
          }, {});
        };

        const validateVertex = function (user, vertex) {
          assert.strictEqual(vertex.type, 'vertex');
          assert.strictEqual(vertex.label, 'simu');
          assert.deepEqual(flattenProperties(vertex), user);
        };

        utils.series([
          function createSchema (next) {
            const schemaQuery = '' +
              'schema.propertyKey("username").Text().create()\n' +
              'schema.propertyKey("uuid").Uuid().create()\n' +
              'schema.propertyKey("number").Double().create()\n' +
              'schema.vertexLabel("simu").properties("username", "uuid", "number").create()';
            client.executeGraph(schemaQuery, next);
          },
          function createInitialVertex (next) {
            // This is needed as DSE Graph doesn't currently support making concurrent schema changes at a time and there
            // is no way to express vertex property relationship without first creating a vertex with those properties.
            const uid = -1;
            const user = {username: 'User' + uid, uuid: Uuid.random().toString(), number: uid};
            users.push(user);
            client.executeGraph(addQuery, user, function(err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              const vertex = result.first();
              validateVertex(user, vertex);
              next();
            });
          },
          function createVerticesConcurrently (next) {
            // Concurrently create 'vertexCount' vertices and ensure the vertex returned is as expected.
            utils.times(vertexCount, function(n, next) {
              const user = {username: 'User' + n, uuid: Uuid.random().toString(), number: n};
              users.push(user);
              client.executeGraph(addQuery, user, function(err, result) {
                assert.ifError(err);
                assert.ok(result);
                assert.strictEqual(result.length, 1);
                const vertex = result.first();
                validateVertex(user, vertex);
                next(err, vertex);
              });
            }, next);
          },
          function retrieveAllVertices (next) {
            // Retrieve all vertices in one query.
            client.executeGraph("g.V().hasLabel('simu')", function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, vertexCount+1);
              // Sort returned vertices and tracked users and compare them 1 at a time.
              const results = result.toArray().sort(function(a, b) {
                return a.properties.number[0].value - b.properties.number[0].value;
              });
              const sortedUsers = users.sort(function(a, b) {
                return a.number - b.number;
              });

              results.forEach(function(vertex, index) {
                validateVertex(sortedUsers[index], vertex);
              });
              next();
            });
          }
        ], done);
      }));
      context('with no callback specified', function () {
        it('should return a promise', function () {
          const client = newInstance();
          const p = client.executeGraph('g.V()', null, null);
          helper.assertInstanceOf(p, Promise);
          return p.then(function (result) {
            helper.assertInstanceOf(result, graphModule.GraphResultSet);
          });
        });
      });

      // In DSE 5.1 geo types must now bounded in the graph schema, however
      // older versions of DSE do not support this, so the type must be conditionally
      // derived.
      const is51 = helper.isDseGreaterThan('5.1');
      const pointType = is51 ? 'Point().withBounds(-40, -40, 40, 40)' : 'Point()';
      const lineType = is51 ? 'Linestring().withGeoBounds()' : 'Linestring()';
      const polygonType = is51 ? 'Polygon().withGeoBounds()' : 'Polygon()';

      const values = [
        // Validate that all supported property types by DSE graph are properly encoded / decoded.
        ['Boolean()', [true, false]],
        ['Int()', [2147483647, -2147483648, 0, 42]],
        ['Smallint()', [-32768, 32767, 0, 42]],
        ['Bigint()', [9007199254740991, -9007199254740991, 0]], // MAX_SAFE_INTEGER / MIN_SAFE_INTEGER
        ['Float()', [3.1415927]],
        ['Double()', [Math.PI]],
        ['Decimal()', ["8675309.9998"]],
        ['Varint()', ["8675309"]],
        ['Timestamp()', ["2016-02-04T02:26:31.657Z"]],
        ['Duration()', ['P2DT3H4M', '5 s', '6 seconds', '1 minute', '1 hour'], ['PT51H4M', 'PT5S', 'PT6S', 'PT1M', 'PT1H']],
        ['Blob()', ['SGVsbG8gV29ybGQ=']], // 'Hello World'.
        ['Text()', ["", "75", "Lorem Ipsum"]],
        ['Uuid()', [Uuid.random()]],
        ['Inet()', [InetAddress.fromString("127.0.0.1"), InetAddress.fromString("::1"), InetAddress.fromString("2001:db8:85a3:0:0:8a2e:370:7334")], ["127.0.0.1", "0:0:0:0:0:0:0:1", "2001:db8:85a3:0:0:8a2e:370:7334"]],
        [pointType, [new Point(0, 1).toString(), new Point(-5, 20).toString()]],
        [lineType, [new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40)).toString()]],
        [polygonType, [new Polygon(
          [new Point(35, 10), new Point(45, 45), new Point(15, 40), new Point(10, 20), new Point(35, 10)],
          [new Point(20, 30), new Point(35, 35), new Point(30, 20), new Point(20, 30)]
        ).toString()]]
      ];
      if (is51) {
        values.push.apply(values, [
          ['Date()', [ new types.LocalDate(2017, 2, 3), new types.LocalDate(-5, 2, 8) ]],
          //TODO: Wait for DSP-12318 to be resolved
          //['Time()', [ types.LocalTime.fromString('4:53:03.000000021') ]]
        ]);
      }
      values.forEach(function (args) {
        const id = schemaCounter++;
        const propType = args[0];
        const input = args[1];
        const expected = args.length >= 3 ? args[2] : input;
        it(util.format('should create and retrieve vertex with property of type %s', propType), wrapClient(function(client, done) {
          const vertexLabel = "vertex" + id;
          const propertyName = "prop" + id;
          const schemaQuery = '' +
            'schema.propertyKey(propertyName).' + propType + '.create()\n' +
            'schema.vertexLabel(vertexLabel).properties(propertyName).create()';

          utils.series([
            function createSchema(next) {
              client.executeGraph(schemaQuery, {vertexLabel: vertexLabel, propertyName: propertyName}, null, next);
            },
            function addVertex(next) {
              utils.timesSeries(input.length, function(index, callback) {
                const value = input[index];
                const params = {vertexLabel: vertexLabel, propertyName: propertyName, val: value};
                // Add vertex and ensure it is properly decoded.
                client.executeGraph("g.addV(vertexLabel).property(propertyName, val)", params, null, function (err, result) {
                  assert.ifError(err);
                  validateVertexResult(result, expected[index], vertexLabel, propertyName);

                  // Ensure the vertex is retrievable.
                  // make an exception for Blob type as retrieval by property value does not currently work (DSP-10145).
                  // TODO: Fix when DSP-10145 is fixed.
                  const query = propType === 'Time()' ?
                    "g.V().hasLabel(vertexLabel).has(propertyName)" :
                    "g.V().hasLabel(vertexLabel).has(propertyName, val)";
                  client.executeGraph(query, params, null, function (err, result) {
                    assert.ifError(err);
                    validateVertexResult(result, expected[index], vertexLabel, propertyName);
                    callback();
                  });
                });
              }, next);
            }
          ], done);
        }));
      });
    });
  });

  vdescribe('dse-6.8', 'with core graph engine', () => {
    const graphName = 'test_core_graph';
    const client = new Client(Object.assign({
      profiles: [
        new ExecutionProfile('profile1', { graphOptions: { name: graphName }})
      ]
    }, helper.getOptions()));

    before(async () => {
      await client.connect();
      await client.execute(helper.queries.basic);
      const query = `system.graph("${graphName}")
        .ifNotExists()
        .withReplication("{'class': 'SimpleStrategy', 'replication_factor' : 1}")
        .create()`;
      await client.executeGraph(query);
    });

    before(() => client.metadata.refreshKeyspaces());

    before(() => {
      const query = `schema.vertexLabel('label_test').ifExists().drop()`;
      return client.executeGraph(query, null, { graphName });
    });

    before(async () => {
      const createVertexLabel1Query = `schema.vertexLabel('label_test').ifNotExists().partitionBy('id', Int)
          .property('prop_double', Double)
          .property('prop_text', Text)
          .property('prop_uuid', Uuid)
          .property('prop_long', Bigint)
          .property('prop_blob', Blob)
          .property('prop_duration', Duration)
          .create()`;
      const createVertexLabel2Query = `schema.vertexLabel('label_test2').ifNotExists().partitionBy('id', Int)
          .property('prop_text', Text)
          .create()`;
      const createEdgeLabelQuery = `schema.edgeLabel('created')
        .from('label_test').to('label_test2')
        .property('tag', Text)
        .create()`;
      await client.executeGraph(createVertexLabel1Query, null, { graphName });
      await client.executeGraph(createVertexLabel2Query, null, { graphName });
      await client.executeGraph(createEdgeLabelQuery, null, { graphName });
    });

    after(() => client.shutdown());

    describe('#executeGraph', () => {
      it('should query a graph using core engine', async () => {
        await client.executeGraph('g.V()', null, { graphName });
      });

      it('should internally connect the Client instance if its not already connected', async () => {
        const client = helper.shutdownAfterThisTest(new Client(helper.getOptions()));
        await client.executeGraph('g.V()', null, { graphName });
        assert.isTrue(client.connected);
      });

      it('should default to the correct graph results based on the execution profile', async () => {
        await client.executeGraph('g.V()', null, { executionProfile: 'profile1' });
      });

      it('should default to the correct graph results based on the default graph options', async () => {
        const client = new Client(Object.assign({
          graphOptions: { name: graphName }
        }, helper.getOptions()));

        await client.connect();

        const id = schemaCounter++;
        const insertTraversal = `g.addV('label_test').property('id', ${id}).property('prop_text', 'value${id}')`;

        // With no graph name or execution profile defined, should use the default graph options
        await client.executeGraph(insertTraversal);
        const selectTraversal = `g.V().hasLabel('label_test').has('id', ${id}).values('prop_text')`;
        const rs = await client.executeGraph(selectTraversal);
        assert.deepEqual(rs.toArray(), [`value${id}`]);

        await client.shutdown();

        helper.shutdownAfterThisTest(client);
      });

      it('should be able to add a vertex and traverse it', async () => {
        const id = schemaCounter++;
        const insertTraversal = `g.addV('label_test').property('id', ${id}).property('prop_text', 'value1')`;
        await client.executeGraph(insertTraversal, null, { graphName });

        const selectTraversal = `g.V().hasLabel('label_test').has('id', ${id}).values('prop_text')`;
        const rs = await client.executeGraph(selectTraversal, null, { executionProfile: 'profile1' });
        assert.deepEqual(rs.toArray(), ['value1']);
      });

      it('should be able to add a edge and retrieve it', async () => {
        const id1 = schemaCounter++;
        const id2 = schemaCounter++;
        const addTraversal = `g.addV('label_test')
            .property('id', ${id1})
            .property('prop_text', 'value1')
            .as('first')
          .addV('label_test2')
            .property('id', ${id2})
            .property('prop_text', 'value2')
            .as('second')
          .addE('created').from('first').to('second').property('tag', 'sample')`;
        await client.executeGraph(addTraversal, null, { graphName });

        const retrieveMapTraversal = `g.V().hasLabel('label_test').has('id', ${id1}).outE('created').elementMap()`;
        let rs = await client.executeGraph(retrieveMapTraversal, null, { executionProfile: 'profile1' });
        const map = rs.first();
        assert.instanceOf(map, Map);
        assert.equal(map.get('tag'), 'sample');
        assert.equal(map.get(t.label), 'created');
        const outMap = map.get(direction.out);
        const inMap = map.get(direction.in);
        assert.instanceOf(outMap, Map);
        assert.equal(outMap.get(t.label), 'label_test');
        assert.instanceOf(inMap, Map);
        assert.equal(inMap.get(t.label), 'label_test2');

        const retrieveEdgeTraversal = `g.V().hasLabel('label_test').has('id', ${id1}).outE('created')`;
        rs = await client.executeGraph(retrieveEdgeTraversal, null, { executionProfile: 'profile1' });
        const edge = rs.first();
        assert.instanceOf(edge, Edge);
        assert.equal(edge.label, 'created');
        assert.equal(edge.outVLabel, 'label_test');
        assert.equal(edge.inVLabel, 'label_test2');
        assert.typeOf(edge.outV, 'string');
      });

      it('should support named parameters', async () => {
        const id = schemaCounter++;
        const value = 'value2';

        const insertTraversal = `g.addV('label_test').property('id', myId).property('prop_text', myValue)`;
        // Use execution profile and graphName interchangeably
        await client.executeGraph(insertTraversal, { myId: id, myValue: value}, { executionProfile: 'profile1' });

        const selectTraversal = `g.V().hasLabel('label_test').has('id', myId).values('prop_text')`;
        const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });
        assert.deepEqual(rs.toArray(), [value]);
      });

      it('should support BulkSet', async () => {
        const traversal = `g.V().hasLabel('label_test').aggregate('x').by('prop_text').cap('x')`;
        const rs = await client.executeGraph(traversal, null, { graphName });
        const resultArray = rs.toArray();
        assert.isArray(resultArray);
        assert.lengthOf(resultArray, 1);

        // BulkSet types are deserialized as Arrays by TinkerPop
        assert.isArray(resultArray[0]);
        assert.isAbove(resultArray[0].length, 0);
      });

      it('should support Duration type', async () => {
        const id = schemaCounter++;
        const value = new types.Duration(0, 3, types.Long.fromString('12344553453012'));

        const insertTraversal = `g.addV('label_test').property('id', myId).property('prop_duration', myValue)`;
        await client.executeGraph(insertTraversal, { myId: id, myValue: value}, { graphName });

        const selectTraversal = `g.V().hasLabel('label_test').has('id', myId).values('prop_duration')`;
        const rs = await client.executeGraph(selectTraversal, { myId: id }, { executionProfile: 'profile1' });
        assert.deepEqual(rs.toArray(), [value]);
      });

      it('should support elementMap() tokens', async () => {
        const id = schemaCounter++;
        const insertTraversal = `g.addV('label_test').property('id', ${id}).property('prop_text', 'sample')`;
        await client.executeGraph(insertTraversal, null, { graphName });

        const traversalText = `g.V().has('label_test', 'id', ${id}).elementMap()`;
        const rs = await client.executeGraph(traversalText, null, { graphName });
        const map = rs.first();
        assert.strictEqual(map.get('id'), id);
        assert.strictEqual(map.get('prop_text'), 'sample');
        assert.strictEqual(typeof map.get(t.id), 'string');
        assert.strictEqual(map.get(t.label), 'label_test');
      });

      context('with complex types', () => {
        before(() => {
          const createSchemaQuery = `
          schema.type('address').
            property('address1', Text).
            property('address2', Text).
            property('zip_code', Int).
            create();
          schema.type('contact_details').
            property('address', frozen(typeOf('address'))).
            property('description', Text).
            property('telephone', frozen(listOf(Text))).
            create();
          schema.vertexLabel('label_complex').ifNotExists().partitionBy('id', Int)
            .property('prop_tuple1', tupleOf(Int, Varchar))
            .property('prop_tuple2', tupleOf(Uuid, Bigint, Text))
            .property('prop_udt1', typeOf('address'))
            .property('prop_udt2', typeOf('contact_details'))
            .property('prop_list1', listOf(Int))
            .property('prop_list2', listOf(Text))
            .property('prop_map1', mapOf(Text, Int))
            .property('prop_map2', mapOf(Text, Float))
            .create();`;
          return client.executeGraph(createSchemaQuery, null, { graphName });
        });

        before(() => client.execute(
          `INSERT INTO ${graphName}.label_complex (id, prop_tuple1) VALUES (1, (10, 'ten'))`));

        before(() => client.execute(
          `INSERT INTO ${graphName}.label_complex
           (id, prop_udt1) VALUES (2, {address1: 'Apollo St.', zip_code: 12345 })`));

        it('should deserialize tuples', async () => {
          const selectTraversal = `g.V().hasLabel('label_complex').has('id', 1).values('prop_tuple1')`;
          const rs = await client.executeGraph(selectTraversal, null, { graphName });
          const value = rs.first();
          assert.instanceOf(value, Tuple);
          assert.deepEqual(value, new types.Tuple(10, 'ten'));
        });

        it('should serialize tuples', async () => {
          const id = schemaCounter++;
          const insertTraversal =
            `g.addV('label_complex').property('id', myId)
              .property('prop_tuple1', tuple1).property('prop_tuple2', tuple2)`;

          const tuple1 = new Tuple(asInt(1), 'one');
          const tuple2 = new Tuple(Uuid.random(), types.Long.fromString('1152921504606847000'), 'two');

          await client.executeGraph(insertTraversal, { myId: id, tuple1, tuple2 }, { graphName });

          const selectTraversal =
            `g.V().hasLabel('label_complex').has('id', myId).valueMap('prop_tuple1', 'prop_tuple2')`;
          const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });
          const result = rs.first();
          assert.instanceOf(result, Map);
          result.forEach(v => assert.instanceOf(v[0], Tuple));
          assert.deepEqual(result.get('prop_tuple1')[0].elements, [1, 'one']);
          assert.deepEqual(result.get('prop_tuple2')[0], tuple2);
        });

        it('should deserialize udts', async () => {
          const selectTraversal = `g.V().hasLabel('label_complex').has('id', 2).values('prop_udt1')`;
          const rs = await client.executeGraph(selectTraversal, null, { graphName });
          assert.deepEqual(rs.first(), { address1: 'Apollo St.', address2: null, zip_code: 12345 });
        });

        it('should serialize udts', async () => {
          const id = schemaCounter++;
          const insertTraversal =
            `g.addV('label_complex').property('id', myId)
              .property('prop_udt1', udt1)`;

          const udtInfo = await client.metadata.getUdt(graphName, 'address');
          assert.ok(udtInfo);
          const udt1 = asUdt({ address1: 'Priam St.', address2: 'Second line', zip_code: 55512 }, udtInfo);
          await client.executeGraph(insertTraversal, { myId: id, udt1 }, { graphName });

          const selectTraversal = `g.V().hasLabel('label_complex').has('id', myId).values('prop_udt1')`;
          const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });
          assert.deepEqual(rs.first(), udt1.value);
        });

        it('should serialize nested udts', async () => {
          const id = schemaCounter++;
          const insertTraversal = `g.addV('label_complex').property('id', myId).property('prop_udt2', udt2)`;

          const udtInfoAddress = await client.metadata.getUdt(graphName, 'address');
          const udtInfoContact = await client.metadata.getUdt(graphName, 'contact_details');
          assert.ok(udtInfoAddress);
          assert.ok(udtInfoContact);
          const udt1 = asUdt({ address1: 'Helenus St.', address2: 'Paris line', zip_code: 55513 }, udtInfoAddress);
          const udt2 = asUdt({ address: udt1, description: 'Main contact', telephone: [ '09128903' ] }, udtInfoContact);
          await client.executeGraph(insertTraversal, { myId: id, udt2 }, { graphName });

          const selectTraversal = `g.V().hasLabel('label_complex').has('id', myId).values('prop_udt2')`;
          const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });
          const result = rs.first();
          assert.deepEqual(result.address, udt1.value);
          assert.strictEqual(result.description, udt2.value.description);
          assert.deepEqual(result.telephone, udt2.value.telephone);
        });

        it('should support map values', async () => {
          const id = schemaCounter++;
          const insertTraversal =
            `g.addV('label_complex').property('id', myId).property('prop_map1', map1).property('prop_map2', map2)`;
          const map1 = new Map([[ 'a', asInt(2) ]]);
          const map2 = new Map([[ 'b', asFloat(1.51) ]]);
          await client.executeGraph(insertTraversal, { myId: id, map1, map2 }, { graphName });

          const selectTraversal = `g.V().hasLabel('label_complex').has('id', myId).values('prop_map1', 'prop_map2')`;
          const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });

          const result = rs.toArray();
          assert.deepEqual(result[0].size, 1);
          assert.strictEqual(result[0].get('a'), map1.get('a').value);
          assert.deepEqual(result[1].size, 1);
          assert.strictEqual(result[1].get('b'), map2.get('b').value);
        });

        it('should support list values', async () => {
          const id = schemaCounter++;
          const insertTraversal =
            `g.addV('label_complex').property('id', myId).property('prop_list1', list1).property('prop_list2', list2)`;
          const list1 = [ asInt(100) ];
          const list2 = [ 'one', 'two' ];
          await client.executeGraph(insertTraversal, { myId: id, list1, list2 }, { graphName });

          const selectTraversal = `g.V().hasLabel('label_complex').has('id', myId).values('prop_list1', 'prop_list2')`;
          const rs = await client.executeGraph(selectTraversal, { myId: id }, { graphName });
          const result = rs.toArray();
          assert.deepEqual(result[0], list1.map(x => x.value));
          assert.deepEqual(result[1], list2);
        });
      });
    });
  });
});

function validateVertexResult(result, expectedResult, vertexLabel, propertyName) {
  assert.strictEqual(result.length, 1);
  const vertex = result.first();
  assert.equal(vertex.label, vertexLabel);
  assert.equal(vertex.properties[propertyName][0].value, expectedResult);
}

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    const client = newInstance(options);
    utils.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
}

function newInstance(options) {
  const opts = helper.getOptions(utils.extend(options || {}, { graphOptions : { name: 'name1' }}));
  const client = new Client(opts);
  helper.shutdownAfterThisTest(client);
  return client;
}
