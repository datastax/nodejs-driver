/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var assert = require('assert');
var DseClient = require('../../lib/dse-client');
var helper = require('../helper');
var vdescribe = helper.vdescribe;
var schemaCounter = 0;
var Point = require('../../lib/geometry/point');
var LineString = require('../../lib/geometry/line-string');
var Polygon = require('../../lib/geometry/polygon');
var cassandra = require('cassandra-driver');
var InetAddress = cassandra.types.InetAddress;
var Uuid = cassandra.types.Uuid;
var cl = cassandra.types.consistencies;
var loadBalancing = require('../../lib/policies/load-balancing');
var DseLoadBalancingPolicy = loadBalancing.DseLoadBalancingPolicy;
var utils = require('../../lib/utils');

var makeStrict = 'schema.config().option("graph.schema_mode").set("production")';

var allowScans = 'schema.config().option("graph.allow_scan").set("true")';

var modernSchema =
  makeStrict + '\n' +
  allowScans + '\n' +
  'schema.propertyKey("name").Text().ifNotExists().create();\n' +
  'schema.propertyKey("age").Int().ifNotExists().create();\n' +
  'schema.propertyKey("lang").Text().ifNotExists().create();\n' +
  'schema.propertyKey("weight").Float().ifNotExists().create();\n' +
  'schema.vertexLabel("person").properties("name", "age").ifNotExists().create();\n' +
  'schema.vertexLabel("software").properties("name", "lang").ifNotExists().create();\n' +
  'schema.edgeLabel("created").properties("weight").connection("person", "software").ifNotExists().create();\n' +
  'schema.edgeLabel("knows").properties("weight").connection("person", "person").ifNotExists().create();';

var modernGraph =
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

vdescribe('5.0', 'DseClient', function () {
  this.timeout(60000);
  before(function (done) {
    var client = new DseClient(helper.getOptions());
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {workloads: ['graph']}, next);
      },
      client.connect.bind(client),
      function testCqlQuery(next) {
        client.execute(helper.queries.basic, next);
      },
      function createGraph(next) {
        client.executeGraph('system.graph("name1").ifNotExists().create()', null, { graphName: null}, next);
      },
      function _makeStrict(next) {
        client.executeGraph(makeStrict, null, { graphName: 'name1'}, next);
      },
      function _allowScans(next) {
        client.executeGraph(allowScans, null, { graphName: 'name1'}, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
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
      //See reference graph here
      //http://www.tinkerpop.com/docs/3.0.0.M1/
      before(wrapClient(function (client, done) {
        client.executeGraph(modernSchema, function (err) {
          assert.ifError(err);
          client.executeGraph(modernGraph, done);
        });
      }));
      it('should retrieve graph vertices', wrapClient(function (client, done) {
        var query = 'g.V().has("name", "marko").out("knows")';
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
        var query = 'g.V().has("name", myName)';
        client.executeGraph(query, {myName: "marko"}, null, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          var vertex = result.first();
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
          var arr = result.toArray();
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
          var vertex = result.first();
          assert.strictEqual(vertex.properties.name[0].value, 'marko');
          client.executeGraph("g.V(vertex_id)", {vertex_id: vertex.id}, null, function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            assert.deepEqual(result.first(), vertex);
            done();
          })
        });
      }));
      it('should handle edge id as parameter', wrapClient(function (client, done) {
        client.executeGraph("g.E().has('weight', weight)", {weight:0.2}, null, function(err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          var edge = result.first();
          assert.strictEqual(edge.properties.weight, 0.2);
          assert.strictEqual(edge.inVLabel, 'software');
          var inVid = edge.inV;
          assert.strictEqual(edge.outVLabel, 'person');

          client.executeGraph("g.E(edge_id).inV()", {edge_id: edge.id}, null, function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            var lop = result.first();
            assert.deepEqual(lop.id, inVid);
            done();
          });
        });
      }));
      it('should handle result object of mixed types', wrapClient(function(client, done) {
        var query = "g.V().hasLabel('software').as('a', 'b', 'c')." +
            "select('a','b', 'c')." +
            "by('name')." +
            "by('lang')." +
            "by(__.in('created').fold())";
        client.executeGraph(query, function (err, result) {
          assert.ifError(err);
          assert.ok(result);

          // Ensure that we got 'lop' and 'ripple' for property a.
          assert.strictEqual(result.length, 2);
          var results = result.toArray();
          var names = results.map(function (v) {
            return v.a;
          });
          assert.ok(names.indexOf('lop') != -1);
          assert.ok(names.indexOf('ripple') != -1);

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
              var creators = result.c.map(function (v) {
                return v.properties.name[0].value;
              });
              assert.ok(creators.indexOf('marko') != -1);
              assert.ok(creators.indexOf('josh') != -1);
              assert.ok(creators.indexOf('peter') != -1);
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
        var query = "g.V().hasLabel('person').has('name', 'marko').as('a')" +
            ".outE('knows').as('b').inV().as('c', 'd')" +
            ".outE('created').as('e', 'f', 'g').inV().as('h').path()";

        client.executeGraph(query, function(err, result) {
          assert.ifError(err);
          assert.ok(result);
          var results = result.toArray();
          // There should only be two paths.
          assert.strictEqual(results.length, 2);
          results.forEach(function(path) {
            // ensure the labels are organized as requested.
            var labels = path.labels;
            assert.strictEqual(labels.length, 5);
            assert.deepEqual(labels, [['a'], ['b'], ['c', 'd'], ['e', 'f', 'g'], ['h']]);

            // ensure the returned path matches what was expected and that each object
            // has the expected contents.
            var objects = path.objects;
            assert.strictEqual(objects.length, 5);
            var marko = objects[0];
            var knows = objects[1];
            var josh = objects[2];
            var created = objects[3];
            var software = objects[4];

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
    });
    it('should use list as a parameter', wrapClient(function(client, done) {
      var characters = ['Mario', "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi"];

      utils.series([
        function createSchema (seriesNext) {
          var schemaQuery = '' +
              'schema.propertyKey("characterName").Text().create();\n' +
              'schema.vertexLabel("character").properties("characterName").create();';
          client.executeGraph(schemaQuery, seriesNext);
        },
        function loadGraph (seriesNext) {
          var query =  '' +
            "characters.each { character -> \n" +
            "    graph.addVertex(label, 'character', 'characterName', character);\n" +
            "}";
          client.executeGraph(query, {characters:characters}, null, seriesNext);
        },
        function retrieveCharacters (seriesNext) {
          client.executeGraph("g.V().hasLabel('character').values('characterName')", function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            var results = result.toArray();
            assert.strictEqual(results.length, characters.length);
            characters.forEach(function (c) {
              assert.ok(results.indexOf(c) != -1);
            });
            seriesNext();
          });
        }
      ], done);
    }));
    it('should use map as a parameter', wrapClient(function(client, done) {
      var name = 'Albert Einstein';
      var year = 1879;
      var field = "Physics";
      var citizenship = ['Kingdom of Württemberg', 'Switzerland', 'Austria', 'Germany', 'United States'];
      var id;

      utils.series([
        function createSchema (next) {
          var schemaQuery = '' +
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
          var query =
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
            var vertex = result.first();
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
      var id;
      utils.series([
        function createSchema (next) {
          var schemaQuery = 'schema.propertyKey("multi_prop").Text().multiple().create()\n' +
            'schema.vertexLabel("multi_v").properties("multi_prop").create()';
          client.executeGraph(schemaQuery, next);
        },
        function createVertex (next) {
          var query = "g.addV(label, 'multi_v', 'multi_prop', 'Hello', 'multi_prop', 'Sweet', 'multi_prop', 'World')";
          client.executeGraph(query, function (err, result) {
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            var vertex = result.first();
            var props = vertex.properties.multi_prop.map(function (v) {
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
            var results = result.toArray();
            var props = results.map(function (v) {
              return v.value;
            });
            assert.deepEqual(props, ['Hello', 'Sweet', 'World']);
            next();
          });
        }
      ], done);
    }));
    it('should be able to create and retrieve vertex property with meta properties', wrapClient(function(client, done) {
      var vertex;
      utils.series([
        function createSchema (next) {
          var schemaQuery = '' +
              'schema.propertyKey("sub_prop").Text().create()\n' +
              'schema.propertyKey("sub_prop2").Text().create()\n' +
              'schema.propertyKey("meta_prop").Text().properties("sub_prop", "sub_prop2").create()\n' +
              'schema.vertexLabel("meta_v").properties("meta_prop").create()';
          client.executeGraph(schemaQuery, next);
        },
        function createVertex (next) {
          client.executeGraph("g.addV(label, 'meta_v', 'meta_prop', 'hello')", function (err, result) {
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
            var nVertex = result.first();
            var meta_prop = nVertex.properties.meta_prop[0];
            assert.strictEqual(meta_prop.value, 'hello');
            assert.deepEqual(meta_prop.properties, {sub_prop: 'hi', sub_prop2: 'hi2'});
            next();
          });
        }
      ], done);
    }));
    it('should handle multiple vertex creation queries simultaneously', wrapClient(function(client, done) {
      var addQuery = "g.addV(label, 'simu', 'username', username, 'uuid', uuid, 'number', number)";
      var vertexCount = 100;
      var users = [];

      var flattenProperties = function(vertex) {
        // Convert properties map to a map of the key name and the value of the first element.
        return Object.keys(vertex.properties).reduce(function(m, k) {
          m[k] = vertex.properties[k][0].value;
          return m;
        }, {});
      };

      var validateVertex = function (user, vertex) {
        assert.strictEqual(vertex.type, 'vertex');
        assert.strictEqual(vertex.label, 'simu');
        assert.deepEqual(flattenProperties(vertex), user);
      };

      utils.series([
        function createSchema (next) {
          var schemaQuery = '' +
              'schema.propertyKey("username").Text().create()\n' +
              'schema.propertyKey("uuid").Uuid().create()\n' +
              'schema.propertyKey("number").Double().create()\n' +
              'schema.vertexLabel("simu").properties("username", "uuid", "number").create()';
          client.executeGraph(schemaQuery, next);
        },
        function createInitialVertex (next) {
          // This is needed as DSE Graph doesn't currently support making concurrent schema changes at a time and there
          // is no way to express vertex property relationship without first creating a vertex with those properties.
          var uid = -1;
          var user = {username: 'User' + uid, uuid: Uuid.random().toString(), number: uid};
          users.push(user);
          client.executeGraph(addQuery, user, function(err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, 1);
            var vertex = result.first();
            validateVertex(user, vertex);
            next();
          });
        },
        function createVerticesConcurrently (next) {
          // Concurrently create 'vertexCount' vertices and ensure the vertex returned is as expected.
          utils.times(vertexCount, function(n, next) {
            var user = {username: 'User' + n, uuid: Uuid.random().toString(), number: n};
            users.push(user);
            client.executeGraph(addQuery, user, function(err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              var vertex = result.first();
              validateVertex(user, vertex);
              next(err, vertex);
            })
          }, next);
        },
        function retrieveAllVertices (next) {
          // Retrieve all vertices in one query.
          client.executeGraph("g.V().hasLabel('simu')", function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.length, vertexCount+1);
            // Sort returned vertices and tracked users and compare them 1 at a time.
            var results = result.toArray().sort(function(a, b) {
              return a.properties.number[0].value - b.properties.number[0].value;
            });
            var sortedUsers = users.sort(function(a, b) {
              return a.number - b.number;
            });

            results.forEach(function(vertex, index) {
              validateVertex(sortedUsers[index], vertex);
            });
            next();
          });
        }
      ], done)
    }));
    [
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
      ['Duration()', ['P2DT3H4M', '5 s', '5 seconds', '1 minute', '1 hour'], ['PT51H4M', 'PT5S', 'PT5S', 'PT1M', 'PT1H']],
      ['Blob()', ['0x48656c6c6f20576f726c64'], ['SGVsbG8gV29ybGQ=']], // 'Hello World'.  TODO: Test with base64 input when supported.
      ['Text()', ["", "75", "Lorem Ipsum"]],
      ['Uuid()', [Uuid.random()]],
      ['Inet()', [InetAddress.fromString("127.0.0.1"), InetAddress.fromString("::1"), InetAddress.fromString("2001:db8:85a3:0:0:8a2e:370:7334")], ["127.0.0.1", "0:0:0:0:0:0:0:1", "2001:db8:85a3:0:0:8a2e:370:7334"]],
      // TODO: Should driver support encoding geo types to WKT when encoding parameters?  At the moment it uses geojson
      // which DSE Graph does not support.
      ['Point()', [new Point(0, 1).toString(), new Point(-5, 20).toString()]],
      ['Linestring()', [new LineString(new Point(30, 10), new Point(10, 30), new Point(40, 40)).toString()]],
      ['Polygon()', [new Polygon(
        [new Point(35, 10), new Point(45, 45), new Point(15, 40), new Point(10, 20), new Point(35, 10)],
        [new Point(20, 30), new Point(35, 35), new Point(30, 20), new Point(20, 30)]
      ).toString()]]
    ].forEach(function (args) {
      var id = schemaCounter++;
      var propType = args[0];
      var input = args[1];
      var expected = args.length >= 3 ? args[2] : input;
      it(util.format('should create and retrieve vertex with property of type %s', propType), wrapClient(function(client, done) {
        var vertexLabel = "vertex" + id;
        var propertyName = "prop" + id;
        var schemaQuery = '' +
          'schema.propertyKey(propertyName).' + propType + '.create()\n' +
          'schema.vertexLabel(vertexLabel).properties(propertyName).create()';

        utils.series([
          function createSchema(next) {
            client.executeGraph(schemaQuery, {vertexLabel: vertexLabel, propertyName: propertyName}, null, next);
          },
          function addVertex(next) {
            utils.timesSeries(input.length, function(index, callback) {
              var value = input[index];
              var params = {vertexLabel: vertexLabel, propertyName: propertyName, val: value};
              // Add vertex and ensure it is properly decoded.
              client.executeGraph("g.addV(label, vertexLabel, propertyName, val)", params, null, function (err, result) {
                assert.ifError(err);
                validateVertexResult(result, expected[index], vertexLabel, propertyName);

                // Ensure the vertex is retrievable.
                client.executeGraph("g.V().hasLabel(vertexLabel).has(propertyName, val).next()", params, null, function (err, result) {
                  assert.ifError(err);
                  validateVertexResult(result, expected[index], vertexLabel, propertyName);
                  callback();
                })
              });
            }, next);
          }
        ], done);
      }));
    });
  });
});

vdescribe('5.0', 'DseClient with down node', function () {
  this.timeout(240000);
  before(function (done) {
    var client = new DseClient(helper.getOptions());
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(3, {workloads: ['graph']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        var replicationConfig = "{'class' : 'SimpleStrategy', 'replication_factor' : 3}";
        var query = 'system.graph("name1")\n' +
          '.option("graph.replication_config").set(replicationConfig)' +
          '.option("graph.system_replication_config").set(replicationConfig)' +
          '.ifNotExists().create()';
        client.executeGraph(query, {replicationConfig: replicationConfig}, { graphName: null}, next);
      },
      function createSchema(next) {
        client.executeGraph(modernSchema, null, { graphName: "name1" }, next);
      },
      function loadGraph(next) {
        client.executeGraph(modernGraph, null, { graphName: "name1" }, next);
      },
      function simpleQuery(next) {
        // execute a graph traversal, which triggers some schema operations.
        client.executeGraph('g.V().limit(1)', null, { graphName: "name1" }, next);
      },
      function stopNode(next) {
        helper.ccm.stopNode(2, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  describe('#executeGraph()', function () {
    it('should be able to make a read query with ONE read consistency, ALL write consistency', wrapClient(function (client, done) {
      client.executeGraph('g.V().limit(1)', null, {readTimeout: 5000, graphReadConsistency: cl.one, graphWriteConsistency: cl.all}, function (err, result) {
        assert.ifError(err);
        assert.ok(result.first());
        done();
      });
    }));
    it('should fail to make a read query with ALL read consistency', wrapClient(function (client, done) {
      client.executeGraph('g.V().limit(1)', null, {graphReadConsistency: cl.all}, function (err, result) {
        assert.ok(err);
        assert.strictEqual(err.message, "Cannot achieve consistency level ALL");
        assert.strictEqual(result, undefined);
        done();
      });
    }));
    it('should be able to make a write query with ALL read consistency, TWO write consistency', wrapClient(function (client, done) {
      client.executeGraph('graph.addVertex(label, "person", "name", "don", "age", 37);', null, {readTimeout: 5000, graphReadConsistency: cl.all, graphWriteConsistency: cl.two}, function (err, result) {
        assert.ifError(err);
        assert.ok(result.first());
        done();
      });
    }));
    it('should fail to make a write query with ALL write consistency', wrapClient(function (client, done) {
      client.executeGraph('graph.addVertex(label, "person", "name", "joe", "age", 42);', null, {graphWriteConsistency: cl.all}, function (err, result) {
        assert.ok(err);
        assert.strictEqual(err.message, "Cannot achieve consistency level ALL");
        assert.strictEqual(result, undefined);
        done();
      });
    }));
  });
});

vdescribe('5.0', 'DseClient with spark workload', function () {
  this.timeout(300000);
  before(function (done) {
    var client = new DseClient(helper.getOptions());
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {workloads: ['graph', 'spark']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        var replicationConfig = "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
        var query = 'system.graph("name1")\n' +
          '.option("graph.replication_config").set(replicationConfig)' +
          '.option("graph.system_replication_config").set(replicationConfig)' +
          '.ifNotExists().create()';
        client.executeGraph(query, {replicationConfig: replicationConfig}, {graphName: null}, function(err) {
          assert.ifError(err);
          next();
        });
      },
      function updateDseLeases(next) {
        // Set the dse_leases keyspace to RF of 2, this will prevent election of new job tracker until all nodes
        // are available, preventing weird cases where 1 node thinks the wrong node is a master.
        client.execute(
          "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'GraphAnalytics': '2'}",
          next);
      },
      function addNode(next) {
        helper.ccm.bootstrapNode(2, next);
      },
      function setNodeWorkload(next) {
        helper.ccm.setWorkload(2, ['graph', 'spark'], next);
      },
      function startNode(next) {
        helper.ccm.startNode(2, next);
      },
      function waitForWorkers(next) {
        helper.waitForWorkers(2, next);
      },
      function createSchema(next) {
        client.executeGraph(modernSchema, null, {graphName: "name1"}, next);
      },
      function loadGraph(next) {
        client.executeGraph(modernGraph, null, {graphName: "name1"}, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  describe('#executeGraph()', function () {
    it('should make an OLAP query using \'a\' traversal source', wrapClient(function (client, done) {
        utils.timesSeries(10, function (n, timesNext) {
          client.executeGraph('g.V().count()', null, {readTimeout: 120000, graphSource: 'a'}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info);
            assert.strictEqual(6, result.first());
            timesNext(err, result.info.queriedHost);
          });
        }, done);
    }));
    it('should contact spark master directly to make an OLAP query when using DseLoadBalancingPolicy', wrapClient(function (client, done) {
      helper.findSparkMaster(client, function (serr, sparkMaster) {
        assert.ifError(serr);
        utils.timesSeries(10, function (n, timesNext) {
          client.executeGraph('g.V().count()', null, {readTimeout: 120000, graphSource: 'a'}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info);
            var queriedHost = result.info.queriedHost;
            var portSep = queriedHost.lastIndexOf(":");
            queriedHost = portSep != -1 ? queriedHost.substr(0, portSep) : queriedHost;
            assert.strictEqual(queriedHost, sparkMaster);
            assert.strictEqual(6, result.first());
            timesNext(err, result.info.queriedHost);
          });
        }, done);
      });
    }, {policies: {loadBalancing: DseLoadBalancingPolicy.createDefault()}}));
  });
});

function validateVertexResult(result, expectedResult, vertexLabel, propertyName) {
  assert.strictEqual(result.length, 1);
  var vertex = result.first();
  assert.equal(vertex.label, vertexLabel);
  assert.equal(vertex.properties[propertyName][0].value, expectedResult);
}

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    var opts = helper.getOptions(helper.extend(options || {}, {graphOptions : { name: 'name1' }}));
    var client = new DseClient(opts);
    utils.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
}


