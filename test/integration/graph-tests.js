'use strict';
var async = require('async');
var util = require('util');
var assert = require('assert');
var DseClient = require('../../lib/dse-client');
var helper = require('../helper');

describe('DseClient', function () {
  this.timeout(60000);
  before(function (done) {
    var client = new DseClient(helper.getOptions());
    async.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {}, next);
      },
      client.connect.bind(client),
      function testCqlQuery(next) {
        client.execute(helper.queries.basic, next);
      },
      function createGraph(next) {
        client.executeGraph('system.createGraph("name1").ifNotExist().build()', null, { graphName: null}, next);
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
    context('with classic schema', function () {
      //See reference graph here
      //http://www.tinkerpop.com/docs/3.0.0.M1/
      before(wrapClient(function (client, done) {
        var query =
          'Vertex marko = graph.addVertex("name", "marko", "age", 29);' +
          'Vertex vadas = graph.addVertex("name", "vadas", "age", 27);' +
          'Vertex lop = graph.addVertex("name", "lop", "lang", "java");' +
          'Vertex josh = graph.addVertex("name", "josh", "age", 32);' +
          'Vertex ripple = graph.addVertex("name", "ripple", "lang", "java");' +
          'Vertex peter = graph.addVertex("name", "peter", "age", 35);' +
          'marko.addEdge("knows", vadas, "weight", 0.5f);' +
          'marko.addEdge("knows", josh, "weight", 1.0f);' +
          'marko.addEdge("created", lop, "weight", 0.4f);' +
          'josh.addEdge("created", ripple, "weight", 1.0f);' +
          'josh.addEdge("created", lop, "weight", 0.4f);' +
          'peter.addEdge("created", lop, "weight", 0.2f);';
        client.executeGraph(query, done);
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
        client.executeGraph('g.E()', function (err, result) {
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
        client.executeGraph(query, { myName : "marko"}, null, function (err, result) {
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
        client.executeGraph('[a, b]', { a : 10, b: 20}, null, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.strictEqual(result.length, 2);
          var arr = result.toArray();
          assert.strictEqual(arr[0], 10);
          assert.strictEqual(arr[1], 20);
          done();
        });
      }));
      it('should setting the graph alias', wrapClient(function (client, done) {
        client.executeGraph('zz.V()', null, { graphAlias: 'zz'}, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          done();
        });
      }));
    });
  });
});

function wrapClient(handler) {
  return (function wrappedTestCase(done) {
    var client = new DseClient(helper.getOptions({ graphOptions: { name: 'name1' }}));
    async.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
}


