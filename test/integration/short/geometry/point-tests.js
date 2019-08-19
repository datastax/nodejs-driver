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
const assert = require('assert');
const util = require('util');
const helper = require('../../../test-helper');
const Client = require('../../../../lib/dse-client');
const vdescribe = helper.vdescribe;
const geometry = require('../../../../lib/geometry');
const types = require('../../../../lib/types');
const utils = require('../../../../lib/utils');
const Point = geometry.Point;
const Uuid = types.Uuid;
const Tuple = types.Tuple;

vdescribe('dse-5.0', 'Point', function () {
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
    const client = new Client(helper.getOptions());
    utils.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.points', function (err, result) {
          assert.ifError(err);
          const map = helper.keyedById(result);
          [
            ['POINT(0 0)', 0, 0],
            ['POINT(2 4)', 2, 4],
            ['POINT(-1.2 -100)', -1.2, -100]
          ]
            .forEach(function (item) {
              const p = map[item[0]];
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
    const name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode points for %s queries', name), function (done) {
      // Re-enable test when DSP-15650 is fixed.
      if (helper.isDseGreaterThan('6')) {
        this.skip();
      }
      const client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function test(next) {
          const values = [
            new Point(1.2, 3.9),
            new Point(-1.2, 1.9),
            new Point(0.21222, 3122.9)
          ];
          const insertQuery = 'INSERT INTO ks1.points (id, value) VALUES (?, ?)';
          const selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.points WHERE id = ?';
          let counter = 0;
          utils.each(values, function (p, eachNext) {
            const id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, p], { prepare: prepare}, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                const row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                const value = JSON.parse(row['json_value']);
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
      const client = new Client(helper.getOptions());
      const id = new Point(1, 0);
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
    const point = new Point(0, 0);
    const point2 = new Point(1, 1);
    before(function (done) {
      const client = new Client(helper.getOptions());
      utils.series([
        client.connect.bind(client),
        function createAll(next) {
          const queries = [
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
      const name = prepare ? 'prepared' : 'simple';
      it(util.format('should create and retrieve points in a udt for %s queries', name), function (done) {
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_udts (id, udt_col) values (?, ?)';
        const selectQuery = 'SELECT udt_col FROM ks1.tbl_udts WHERE id = ?';
        const id = Uuid.random();
        const udt = { f: 'hello', v: point};

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, udt], {prepare: prepare, hints: [null, 'udt<ks1.pointt>']}, function (err) {
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
      tupleTestCase(util.format('should create and retrieve points in a tuple for %s queries', name), function (done) {
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_tuple (id, tuple_col) values (?, ?)';
        const selectQuery = 'SELECT tuple_col FROM ks1.tbl_tuple WHERE id = ?';
        const id = Uuid.random();
        const tuple = new Tuple(0, point);

        utils.series([
          client.connect.bind(client),
          function (next) {
            client.execute(insertQuery, [id, tuple], { prepare: prepare }, function (err) {
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
        it(util.format('should create and retrieve points in a %s for %s queries', colType, name), function (done) {
          const client = new Client(helper.getOptions());
          const insertQuery = util.format('INSERT INTO ks1.tbl_%s (id, %s_col) values (?, ?)', colType, colType);
          const selectQuery = util.format('SELECT %s_col FROM ks1.tbl_%s WHERE id = ?', colType, colType);
          const id = Uuid.random();
          const data = [point, point2];
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
      it(util.format('should create and retrieve points in a map for %s queries', name), function (done) {
        const client = new Client(helper.getOptions());
        const insertQuery = 'INSERT INTO ks1.tbl_map (id, map_col) values (?, ?)';
        const selectQuery = 'SELECT map_col FROM ks1.tbl_map WHERE id = ?';
        const id = Uuid.random();
        const map = { point : point };

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