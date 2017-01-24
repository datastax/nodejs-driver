/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');
var helper = require('../../../test-helper');
var Client = require('../../../../lib/dse-client');
var vdescribe = helper.vdescribe;
var geometry = require('../../../../lib/geometry');
var types = require('../../../../lib/types');
var utils = require('../../../../lib/utils');
var Point = geometry.Point;
var Uuid = types.Uuid;
var Tuple = types.Tuple;

vdescribe('dse-5.0', 'Point', function () {
  this.timeout(120000);
  before(function (done) {
    var client = new Client(helper.getOptions());
    utils.series([
      function (next) {
        helper.ccm.startAll(1, {}, next);
      },
      client.connect.bind(client),
      function createAll(next) {
        var queries = [
          "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = false",
          "use ks1",
          "CREATE TABLE points (id text, value 'PointType', PRIMARY KEY (id))",
          "INSERT INTO points (id, value) VALUES ('POINT(0 0)', 'POINT(0 0)')",
          "INSERT INTO points (id, value) VALUES ('POINT(2 4)', 'POINT(2 4)')",
          "INSERT INTO points (id, value) VALUES ('POINT(-1.2 -100)', 'POINT(-1.2 -100)')",
          "CREATE TABLE keyed (id 'PointType', value text, PRIMARY KEY (id))",
          "INSERT INTO keyed (id, value) VALUES ('POINT(1 0)', 'hello')"
        ];
        utils.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse points', function (done) {
    var client = new Client(helper.getOptions());
    utils.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.points', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['POINT(0 0)', 0, 0],
            ['POINT(2 4)', 2, 4],
            ['POINT(-1.2 -100)', -1.2, -100]
          ]
            .forEach(function (item) {
              var p = map[item[0]];
              helper.assertInstanceOf(p, Point);
              assert.strictEqual(p.x, item[1]);
              assert.strictEqual(p.y, item[2]);
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode points for %s queries', name), function (done) {
      var client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new Point(1.2, 3.9),
            new Point(-1.2, 1.9),
            new Point(0.21222, 3122.9)
          ];
          var insertQuery = 'INSERT INTO ks1.points (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.points WHERE id = ?';
          var counter = 0;
          utils.each(values, function (p, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, p], { prepare: prepare}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates, [p.x, p.y]);
                eachNext();
              });
            });
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    it(util.format('should be able to retrieve data where point is partition key for %s queries', name), function (done) {
      var client = new Client(helper.getOptions());
      var id = new Point(1, 0);
      utils.series([
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
    var point = new Point(0, 0);
    var point2 = new Point(1, 1);
    before(function (done) {
      var client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function createAll(next) {
          var queries = [
            "use ks1",
            "CREATE TYPE pointt (f text, v 'PointType')",
            "CREATE TABLE tbl_udts (id uuid PRIMARY KEY, udt_col frozen<pointt>)",
            "CREATE TABLE tbl_tuple (id uuid PRIMARY KEY, tuple_col tuple<int, 'PointType'>)",
            "CREATE TABLE tbl_list (id uuid PRIMARY KEY, list_col list<'PointType'>)",
            "CREATE TABLE tbl_set (id uuid PRIMARY KEY, set_col set<'PointType'>)",
            "CREATE TABLE tbl_map (id uuid PRIMARY KEY, map_col map<text, 'PointType'>)"
          ];
          utils.eachSeries(queries, function (q, eachNext) {
            client.execute(q, eachNext);
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    [0, 1].forEach(function (prepare) {
      var name = prepare ? 'prepared' : 'simple';
      it(util.format('should create and retrieve points in a udt for %s queries', name), function (done) {
        var client = new Client(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_udts (id, udt_col) values (?, ?)';
        var selectQuery = 'SELECT udt_col FROM ks1.tbl_udts WHERE id = ?';
        var id = Uuid.random();
        var udt = { f: 'hello', v: point};

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, udt], {prepare: prepare, hints: [null, 'udt<ks1.pointt>']}, function (err) {
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
      tupleTestCase(util.format('should create and retrieve points in a tuple for %s queries', name), function (done) {
        var client = new Client(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_tuple (id, tuple_col) values (?, ?)';
        var selectQuery = 'SELECT tuple_col FROM ks1.tbl_tuple WHERE id = ?';
        var id = Uuid.random();
        var tuple = new Tuple(0, point);

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, tuple], { prepare: prepare }, function (err) {
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
        it(util.format('should create and retrieve points in a %s for %s queries', colType, name), function (done) {
          var client = new Client(helper.getOptions());
          var insertQuery = util.format('INSERT INTO ks1.tbl_%s (id, %s_col) values (?, ?)', colType, colType);
          var selectQuery = util.format('SELECT %s_col FROM ks1.tbl_%s WHERE id = ?', colType, colType);
          var id = Uuid.random();
          var data = [point, point2];
          utils.series([
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
      it(util.format('should create and retrieve points in a map for %s queries', name), function (done) {
        var client = new Client(helper.getOptions());
        var insertQuery = 'INSERT INTO ks1.tbl_map (id, map_col) values (?, ?)';
        var selectQuery = 'SELECT map_col FROM ks1.tbl_map WHERE id = ?';
        var id = Uuid.random();
        var map = { point : point };

        utils.series([
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