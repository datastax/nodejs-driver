/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');
const helper = require('../../../test-helper');
const Client = require('../../../../lib/dse-client');
const vdescribe = helper.vdescribe;
const geometry = require('../../../../lib/geometry');
const types = require('../../../../lib/types');
const utils = require('../../../../lib/utils');
const Point = geometry.Point;
const LineString = geometry.LineString;
const Uuid = types.Uuid;
const Tuple = types.Tuple;

vdescribe('dse-5.0', 'LineString', function () {
  this.timeout(120000);
  before(function (done) {
    const client = new Client(helper.getOptions());
    utils.series([
      function (next) {
        helper.ccm.startAll(1, {}, next);
      },
      client.connect.bind(client),
      function createAll(next) {
        const queries = [
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
        utils.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse lines', function (done) {
    const client = new Client(helper.getOptions());
    utils.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.lines', function (err, result) {
          assert.ifError(err);
          const map = helper.keyedById(result);
          [
            ['LINESTRING (0 0, 1 1)', new LineString(new Point(0, 0), new Point(1, 1))],
            ['LINESTRING (1 3, 2 6, 3 9)', new LineString(new Point(1, 3), new Point(2, 6), new Point(3, 9))],
            ['LINESTRING (-1.2 -100, 0.99 3)', new LineString(new Point(-1.2, -100), new Point(0.99, 3))],
            ['LINESTRING EMPTY', new LineString()]
          ]
            .forEach(function (item) {
              const l = map[item[0]];
              helper.assertInstanceOf(l, LineString);
              assert.strictEqual(l.points.length, item[1].points.length);
              l.points.forEach(function (p1, i) {
                const p2 = item[1].points[i];
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
    const name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode lines for %s queries', name), function (done) {
      // Re-enable test when DSP-15650 is fixed.
      if (helper.isDseGreaterThan('6')) {
        this.skip();
      }
      const client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function test(next) {
          const values = [
            new LineString(new Point(1.2, 3.9), new Point(6.2, 18.9)),
            new LineString(new Point(-1.2, 1.9), new Point(111, 22)),
            new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9))
          ];
          const insertQuery = 'INSERT INTO ks1.lines (id, value) VALUES (?, ?)';
          const selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.lines WHERE id = ?';
          let counter = 0;
          utils.each(values, function (line, eachNext) {
            const id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, line], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                const row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                const value = JSON.parse(row['json_value']);
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
      const client = new Client(helper.getOptions());
      const id = new LineString(new Point(0, 0), new Point(1, 1));
      utils.series([
        client.connect.bind(client),
        function (next) {
          const selectQuery = 'SELECT value FROM ks1.keyed WHERE id = ?';
          client.execute(selectQuery, [id], function (err, result) {
            assert.ifError(err);
            const row = result.first();
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
    const line = new LineString(new Point(0, 0), new Point(1, 1));
    const line2 = new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9));
    before(function (done) {
      const client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function createAll(next) {
          const queries = [
            "use ks1",
            "CREATE TYPE linet (f text, v 'LineStringType')",
            "CREATE TABLE tbl_udts (id uuid PRIMARY KEY, udt_col frozen<linet>)",
            "CREATE TABLE tbl_tuple (id uuid PRIMARY KEY, tuple_col tuple<int, 'LineStringType'>)",
            "CREATE TABLE tbl_list (id uuid PRIMARY KEY, list_col list<'LineStringType'>)",
            "CREATE TABLE tbl_set (id uuid PRIMARY KEY, set_col set<'LineStringType'>)",
            "CREATE TABLE tbl_map (id uuid PRIMARY KEY, map_col map<text, 'LineStringType'>)"
          ];
          utils.eachSeries(queries, function (q, eachNext) {
            client.execute(q, eachNext);
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    [0, 1].forEach(function (prepare) {
      const name = prepare ? 'prepared' : 'simple';
      it(util.format('should create and retrieve lines in a udt for %s queries', name), function (done) {
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_udts (id, udt_col) values (?, ?)';
        const selectQuery = 'SELECT udt_col FROM ks1.tbl_udts WHERE id = ?';
        const id = Uuid.random();
        const udt = { f: 'hello', v: line};

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, udt], {prepare: prepare, hints: [null, 'udt<ks1.linet>']}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                const row = result.first();
                assert.ok(row);
                assert.deepEqual(row['udt_col'], udt);
                next();
              });
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
      let tupleTestCase = it;
      if (prepare === 0) {
        //tuples are not supported in simple statements in the core driver
        //mark it as pending
        tupleTestCase = xit;
      }
      tupleTestCase(util.format('should create and retrieve lines in a tuple for %s queries', name), function (done) {
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_tuple (id, tuple_col) values (?, ?)';
        const selectQuery = 'SELECT tuple_col FROM ks1.tbl_tuple WHERE id = ?';
        const id = Uuid.random();
        const tuple = new Tuple(0, line);

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, tuple], {prepare: prepare}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                const row = result.first();
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
          const client = new Client(helper.getOptions());
          const insertQuery = util.format('INSERT INTO ks1.tbl_%s (id, %s_col) values (?, ?)', colType, colType);
          const selectQuery = util.format('SELECT %s_col FROM ks1.tbl_%s WHERE id = ?', colType, colType);
          const id = Uuid.random();
          const data = [line, line2];
          utils.series([
            client.connect.bind(client),
            function (next) {
              client.execute(insertQuery, [id, data], {prepare: prepare}, function (err) {
                assert.ifError(err);
                client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                  assert.ifError(err);
                  const row = result.first();
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
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_map (id, map_col) values (?, ?)';
        const selectQuery = 'SELECT map_col FROM ks1.tbl_map WHERE id = ?';
        const id = Uuid.random();
        const map = { line : line };

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, map], {prepare: prepare, hints: [null, types.dataTypes.map]}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], {prepare: prepare}, function (err, result) {
                assert.ifError(err);
                const row = result.first();
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