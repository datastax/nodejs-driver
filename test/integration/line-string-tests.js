'use strict';
var assert = require('assert');
var util = require('util');
var async = require('async');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DseClient = require('../../lib/dse-client');
var vdescribe = helper.vdescribe;
var Point = require('../../lib/geometry/point');
var LineString = require('../../lib/geometry/line-string');
var types = cassandra.types;
var Uuid = types.Uuid;
var Tuple = types.Tuple;

vdescribe('5.0', 'LineString', function () {
  this.timeout(120000);
  before(function (done) {
    var client = new DseClient(helper.getOptions());
    async.series([
      function (next) {
        helper.ccm.startAll(1, {}, next);
      },
      client.connect.bind(client),
      function createAll(next) {
        var queries = [
          "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = false",
          "use ks1",
          "CREATE TABLE lines (id text, value 'LineStringType', PRIMARY KEY (id))",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (0 0, 1 1)', 'LINESTRING (0 0, 1 1)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (1 3, 2 6, 3 9)', 'LINESTRING (1 3, 2 6, 3 9)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (-1.2 -100, 0.99 3)', 'LINESTRING (-1.2 -100, 0.99 3)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING EMPTY', 'LINESTRING EMPTY')",
          "CREATE TABLE keyed (id 'LineStringType', value text, PRIMARY KEY (id))",
          "INSERT INTO keyed (id, value) VALUES ('LINESTRING (0 0, 1 1)', 'hello')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse lines', function (done) {
    var client = new DseClient(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.lines', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['LINESTRING (0 0, 1 1)', new LineString(new Point(0, 0), new Point(1, 1))],
            ['LINESTRING (1 3, 2 6, 3 9)', new LineString(new Point(1, 3), new Point(2, 6), new Point(3, 9))],
            ['LINESTRING (-1.2 -100, 0.99 3)', new LineString(new Point(-1.2, -100), new Point(0.99, 3))],
            ['LINESTRING EMPTY', new LineString()]
          ]
            .forEach(function (item) {
              var l = map[item[0]];
              helper.assertInstanceOf(l, LineString);
              assert.strictEqual(l.points.length, item[1].points.length);
              l.points.forEach(function (p1, i) {
                var p2 = item[1].points[i];
                assert.strictEqual(p1.x, p2.x);
                assert.strictEqual(p1.y, p2.y);
              });
              assert.strictEqual(l.toString(), item[0]);
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode lines for %s queries', name), function (done) {
      var client = new DseClient(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new LineString(new Point(1.2, 3.9), new Point(6.2, 18.9)),
            new LineString(new Point(-1.2, 1.9), new Point(111, 22)),
            new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9))
          ];
          var insertQuery = 'INSERT INTO ks1.lines (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.lines WHERE id = ?';
          var counter = 0;
          async.each(values, function (line, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, line], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates, line.points.map(function (p) {
                  return [p.x, p.y];
                }));
                eachNext();
              });
            });
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    it(util.format('should be able to retrieve data where line is partition key for %s queries', name), function (done) {
      var client = new DseClient(helper.getOptions());
      var id = new LineString(new Point(0, 0), new Point(1, 1));
      async.series([
        client.connect.bind(client),
        function (next) {
          var selectQuery = 'SELECT value FROM ks1.keyed WHERE id = ?';
          client.execute(selectQuery, [id], function (err, result) {
            assert.ifError(err);
            var row = result.first();
            assert.ok(row);
            assert.strictEqual(row['value'], 'hello');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('with collections, tuples and udts', function () {
    var line = new LineString(new Point(0, 0), new Point(1, 1));
    var line2 = new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9));
    before(function (done) {
      var client = new DseClient(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function createAll(next) {
          var queries = [
            "use ks1",
            "CREATE TYPE linet (f text, v 'LineStringType')",
            "CREATE TABLE tbl_udts (id uuid PRIMARY KEY, udt_col frozen<linet>)",
            "CREATE TABLE tbl_tuple (id uuid PRIMARY KEY, tuple_col tuple<int, 'LineStringType'>)",
            "CREATE TABLE tbl_list (id uuid PRIMARY KEY, list_col list<'LineStringType'>)",
            "CREATE TABLE tbl_set (id uuid PRIMARY KEY, set_col set<'LineStringType'>)",
            "CREATE TABLE tbl_map (id uuid PRIMARY KEY, map_col map<text, 'LineStringType'>)"
          ];
          async.eachSeries(queries, function (q, eachNext) {
            client.execute(q, eachNext);
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    [0, 1].forEach(function (prepare) {
      var name = prepare ? 'prepared' : 'simple';
      it(util.format('should create and retrieve lines in a udt for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_udts (id, udt_col) values (?, ?)';
        var selectQuery = 'SELECT udt_col FROM ks1.tbl_udts WHERE id = ?';
        var id = Uuid.random();
        var udt = { f: 'hello', v: line};

        async.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, udt], {prepare: prepare, hints: [null, 'udt<ks1.linet>']}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                assert.deepEqual(row['udt_col'], udt);
                next();
              });
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
      var tupleTestCase = it;
      if (prepare === 0) {
        //tuples are not supported in simple statements in the core driver
        //mark it as pending
        tupleTestCase = xit;
      }
      tupleTestCase(util.format('should create and retrieve lines in a tuple for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_tuple (id, tuple_col) values (?, ?)';
        var selectQuery = 'SELECT tuple_col FROM ks1.tbl_tuple WHERE id = ?';
        var id = Uuid.random();
        var tuple = new Tuple(0, line);

        async.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, tuple], {prepare: prepare}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                assert.deepEqual(row['tuple_col'], tuple);
                next();
              });
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
      ['list', 'set'].forEach(function (colType) {
        it(util.format('should create and retrieve lines in a %s for %s queries', colType, name), function (done) {
          var client = new DseClient(helper.getOptions());
          var insertQuery = util.format('INSERT INTO ks1.tbl_%s (id, %s_col) values (?, ?)', colType, colType);
          var selectQuery = util.format('SELECT %s_col FROM ks1.tbl_%s WHERE id = ?', colType, colType);
          var id = Uuid.random();
          var data = [line, line2];
          async.series([
            client.connect.bind(client),
            function (next) {
              client.execute(insertQuery, [id, data], {prepare: prepare}, function (err) {
                assert.ifError(err);
                client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                  assert.ifError(err);
                  var row = result.first();
                  assert.ok(row);
                  assert.deepEqual(row[util.format('%s_col', colType)], data);
                  next();
                });
              });
            },
            client.shutdown.bind(client)
          ], done);
        });
      });
      it(util.format('should create and retrieve lines in a map for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_map (id, map_col) values (?, ?)';
        var selectQuery = 'SELECT map_col FROM ks1.tbl_map WHERE id = ?';
        var id = Uuid.random();
        var map = { line : line };

        async.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, map], {prepare: prepare, hints: [null, types.dataTypes.map]}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                assert.deepEqual(row['map_col'], map);
                next();
              });
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
    });
  });
  after(helper.ccm.remove.bind(helper.ccm));
});