'use strict';
var assert = require('assert');
var util = require('util');
var async = require('async');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DseClient = require('../../lib/dse-client');
var vdescribe = helper.vdescribe;
var Point = require('../../lib/geometry/point');
var Circle = require('../../lib/geometry/circle');
var types = cassandra.types;
var Uuid = types.Uuid;
var Tuple = types.Tuple;

vdescribe('5.0', 'Circle', function () {
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
          "CREATE TABLE circles (id text, value 'CircleType', PRIMARY KEY (id))",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((0 0) 1)', 'CIRCLE((0 0) 1)')",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((1 3) 2.2)', 'CIRCLE((1 3) 2.2)')",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((-1.2 -100) 0.99)', 'CIRCLE((-1.2 -100) 0.99)')",
          "CREATE TABLE keyed (id 'CircleType', value text, PRIMARY KEY (id))",
          "INSERT INTO keyed (id, value) VALUES ('CIRCLE((0 0) 1)', 'hello')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse circles', function (done) {
    var client = new DseClient(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.circles', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['CIRCLE((0 0) 1)', new Circle(new Point(0, 0), 1)],
            ['CIRCLE((1 3) 2.2)', new Circle(new Point(1, 3), 2.2)],
            ['CIRCLE((-1.2 -100) 0.99)', new Circle(new Point(-1.2, -100), 0.99)]
          ]
            .forEach(function (item) {
              var c = map[item[0]];
              helper.assertInstanceOf(c, Circle);
              assert.strictEqual(c.radius, item[1].radius);
              assert.ok(c.equals(item[1]));
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode circles for %s queries', name), function (done) {
      var client = new DseClient(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new Circle(new Point(1.2, 3.9), 6.2),
            new Circle(new Point(-1.2, 1.9), 111),
            new Circle(new Point(0.21222, 32.9), 10.21222)
          ];
          var insertQuery = 'INSERT INTO ks1.circles (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.circles WHERE id = ?';
          var counter = 0;
          async.each(values, function (circle, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, circle], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates, [ circle.center.x, circle.center.y ]);
                assert.strictEqual(value.radius, circle.radius);
                eachNext();
              });
            });
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    it(util.format('should be able to retrieve data where circle is partition key for %s queries', name), function (done) {
      var client = new DseClient(helper.getOptions());
      var id = new Circle(new Point(0, 0), 1);
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
    var circle = new Circle(new Point(1.2, 3.9), 6.2);
    var circle2 = new Circle(new Point(-1.0, 5.0), 7.0);
    before(function (done) {
      var client = new DseClient(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function createAll(next) {
          var queries = [
            "use ks1",
            "CREATE TYPE circlet (f text, v 'CircleType')",
            "CREATE TABLE tbl_udts (id uuid PRIMARY KEY, udt_col frozen<circlet>)",
            "CREATE TABLE tbl_tuple (id uuid PRIMARY KEY, tuple_col tuple<int, 'CircleType'>)",
            "CREATE TABLE tbl_list (id uuid PRIMARY KEY, list_col list<'CircleType'>)",
            "CREATE TABLE tbl_set (id uuid PRIMARY KEY, set_col set<'CircleType'>)",
            "CREATE TABLE tbl_map (id uuid PRIMARY KEY, map_col map<text, 'CircleType'>)"
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
      it(util.format('should create and retrieve circles in a udt for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_udts (id, udt_col) values (?, ?)';
        var selectQuery = 'SELECT udt_col FROM ks1.tbl_udts WHERE id = ?';
        var id = Uuid.random();
        var udt = { f: 'hello', v: circle};

        async.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, udt], {prepare: prepare, hints: [null, 'udt<ks1.circlet>']}, function (err) {
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
      tupleTestCase(util.format('should create and retrieve circles in a tuple for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_tuple (id, tuple_col) values (?, ?)';
        var selectQuery = 'SELECT tuple_col FROM ks1.tbl_tuple WHERE id = ?';
        var id = Uuid.random();
        var tuple = new Tuple(0, circle);

        async.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, tuple], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], { prepare: prepare }, function (err, result) {
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
        it(util.format('should create and retrieve circles in a %s for %s queries', colType, name), function (done) {
          var client = new DseClient(helper.getOptions());
          var insertQuery = util.format('INSERT INTO ks1.tbl_%s (id, %s_col) values (?, ?)', colType, colType);
          var selectQuery = util.format('SELECT %s_col FROM ks1.tbl_%s WHERE id = ?', colType, colType);
          var id = Uuid.random();
          var data = [circle2, circle];
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
      it(util.format('should create and retrieve circles in a map for %s queries', name), function (done) {
        var client = new DseClient(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_map (id, map_col) values (?, ?)';
        var selectQuery = 'SELECT map_col FROM ks1.tbl_map WHERE id = ?';
        var id = Uuid.random();
        var map = { circle : circle };

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