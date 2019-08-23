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
const vdescribe = helper.vdescribe;
const utils = require('../../../../lib/utils');
const types = require('../../../../lib/types');
const dateRangeModule = require('../../../../lib/search/date-range');
const Client = require('../../../../lib/client');
const DateRange = dateRangeModule.DateRange;

vdescribe('dse-5.1', 'DateRange', function () {
  this.timeout('50000');
  const setupInfo = helper.setup(1, {
    queries: [
      "CREATE TABLE tbl_date_range (pk uuid PRIMARY KEY, c1 'DateRangeType')",
      "INSERT INTO tbl_date_range (pk, c1) VALUES (uuid(), '[2010-12-03 TO 2010-12-04]')",
      "CREATE TYPE IF NOT EXISTS test_udt (i int, range 'DateRangeType')",
      "CREATE TABLE tbl_udt_tuple (k uuid PRIMARY KEY, u test_udt, uf frozen<test_udt>, t tuple<'DateRangeType', int>, tf frozen<tuple<'DateRangeType', int>>)",
      "CREATE TABLE tbl_collection (k uuid PRIMARY KEY, l list<'DateRangeType'>, s set<'DateRangeType'>, m0 map<text, 'DateRangeType'>, m1 map<'DateRangeType', text>)",
      "CREATE TABLE tbl_date_range_pk (k 'DateRangeType' PRIMARY KEY, v int)"
    ]
  });
  const client = setupInfo.client;
  describe('serialization', function () {
    const values = [
      '[-271821-04-20T00:00:00.000Z TO 275760-09-13T00:00:00.000Z]', // min TO max dates supported
      '[-0021-04-20T01 TO 0077-10-13T02]', // 4 digits years
      '2010',
      '[2017-02 TO *]',
      '[-1 TO 1]',
      '[0 TO 1]',
      '[-2017 TO 2017-07-04T16]',
      '[2015-02 TO 2016-02]',
      '[2016-10-01T08 TO *]',
      '[* TO 2016-03-01T16:56:39.999]',
      '[2016-03-01T16:56 TO *]',
      '[* TO *]',
      '*'
    ];
    [true, false].forEach(function (prepare) {
      describe('with { prepare: ' + prepare + ' }', function() {
        it('should serialize and deserialize when used as parameter', function (done) {
          utils.eachSeries(values, function (v, next) {
            const query = 'INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)';
            const id = types.Uuid.random();
            const value = DateRange.fromString(v);
            client.execute(query, [ id, value ], { prepare: prepare }, function (err) {
              assert.ifError(err);
              const query = 'SELECT pk, c1 FROM tbl_date_range WHERE pk = ?';
              client.execute(query, [ id ], { prepare: prepare }, function (err, result) {
                assert.ifError(err);
                assert.strictEqual(result.rowLength, 1);
                const actual = result.first()['c1'];
                helper.assertInstanceOf(actual, DateRange);
                assert.strictEqual(actual.toString(), value.toString());
                assert.ok(actual.equals(value));
                next();
              });
            });
          }, done);
        });
        it('should be allowed in udt and tuple', function (done) {
          const id = types.Uuid.random();
          const u = { i: 0, range: DateRange.fromString('[2017-02 TO 2017-03]') };
          const uf = { i: 4, range: DateRange.fromString('[2016-02-04 TO 2016-05]')};
          const t = new types.Tuple(DateRange.fromString('*'), 3);
          const tf = new types.Tuple(DateRange.fromString('[* TO *]'), 9);
          const query = 'INSERT INTO tbl_udt_tuple (k, u, uf, t, tf) VALUES (?, ?, ?, ?, ?)';
          const options = { prepare: prepare };

          if (!prepare) {
            return this.skip();
          }

          client.execute(query, [ id, u, uf, t, tf ], options, function (err) {
            assert.ifError(err);
            const query = 'SELECT * FROM tbl_udt_tuple WHERE k = ?';
            client.execute(query, [ id ], { prepare: prepare }, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              const row = result.first();
              assert.deepEqual(row['u'], u);
              assert.deepEqual(row['uf'], uf);
              assert.deepEqual(row['t'], t);
              assert.deepEqual(row['tf'], tf);
              done();
            });
          });
        });
        it('should be allowed in collections', function (done) {
          // use map polyfill as we want to use non-string keys.
          const clientOptions = utils.deepExtend({ keyspace: setupInfo.keyspace, encoding: { map: helper.Map } }, helper.baseOptions);
          const client = new Client(clientOptions);
          const id = types.Uuid.random();

          // list<text>
          const l = values.map(function(range) {
            return DateRange.fromString(range);
          });

          // Comparison function for lexicographic compare of Buffer contents.
          // If Buffer.compare is not present, use manual definition.
          const bufferCompare = Buffer.compare || function(a, b) {
            if (a === b) {
              return 0;
            }
            const minLength = Math.min(a.length, b.length);
            let i;
            for(i = 0; i < minLength; i++) {
              if (a[i] !== b[i]) {
                break;
              }
            }

            if (i !== minLength) {
              return a[i] < b[i] ? -1 : 1;
            }

            const maxLength = Math.max(a.length, b.length);
            if (minLength !== maxLength) {
              return a.length < b.length ? -1 : 1;
            }

            // buffer contents are the same.
            return 0;
          };

          // set<'DateRangeType'>
          // For comparison sort by byte ordering to match order on server.
          const s = l.slice().sort(function (a,b) {
            return bufferCompare(a.toBuffer(), b.toBuffer());
          });

          // map<text, 'DateRangeType'>
          // For comparison with output sort by string representation so keys are sorted by string ordering.
          const m0 = values.slice().sort().reduce(function (acc, range) {
            acc.set(range, DateRange.fromString(range));
            return acc;
          }, new helper.Map());

          // map<'DateRangeType',text>
          // For comparison with output reduce from the set so keys are sorted by DateRange byte ordering.
          const m1 = s.reduce(function (acc, range) {
            acc.set(range, range.toString());
            return acc;
          }, new helper.Map());

          const options = { prepare: prepare };
          if (!prepare) {
            options['hints'] = [ 'uuid', 'list', 'set', 'map', 'map' ];
          }

          const query = 'INSERT INTO tbl_collection (k, l, s, m0, m1) VALUES (?, ?, ?, ?, ?)';
          utils.series([
            client.connect.bind(client),
            function (next) {
              client.execute(query, [ id, l, s, m0, m1 ], options, function (err) {
                assert.ifError(err);
                const query = 'SELECT * FROM tbl_collection WHERE k = ?';
                client.execute(query, [ id ], { prepare: prepare }, function (err, result) {
                  assert.ifError(err);
                  assert.strictEqual(result.rowLength, 1);
                  const row = result.first();
                  assert.deepEqual(row['l'], l);
                  assert.deepEqual(row['s'], s);
                  assert.deepEqual(row['m0'], m0);
                  assert.deepEqual(row['m1'], m1);
                  next();
                });
              });
            },
            client.shutdown.bind(client)
          ], done);
        });
        it('should use DateRange as primary key', function (done) {
          //tbl_date_range_pk 
          const key_values = [
            '[-271821-04-20T00:00:00.000Z TO 275760-09-13T00:00:00.000Z]', // min TO max dates supported
            '[2015-02 TO 2016-02]',
            '[2010-12-03 TO 2010-12-04]',
            '[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967Z]'
          ];
          const query = "INSERT INTO tbl_date_range_pk (k, v) VALUES (?, ?)";
          utils.eachSeries(key_values, function (v, next) {
            const value = DateRange.fromString(v);

            const options = { prepare: prepare };
            if (!prepare) {
              options['hints'] = [ null, 'int' ];
            }
            client.execute(query, [ value, 1 ], options, function (err) {
              assert.ifError(err);
              const searchQuery = 'SELECT k FROM tbl_date_range_pk WHERE k = ?';
              client.execute(searchQuery, [ value ], { prepare: prepare }, function (err, result) {
                assert.ifError(err);
                assert.strictEqual(result.rowLength, 1);
                const actual = result.first()['k'];
                helper.assertInstanceOf(actual, DateRange);
                assert.strictEqual(actual.toString(), value.toString());
                assert.ok(actual.equals(value));
                client.execute('DELETE FROM tbl_date_range_pk where k = ?', [ value ], {prepare: prepare} , function(err) {
                  assert.ifError(err);
                  next();
                });
              });
            });
          }, done);
        });
        it('should disallow invalid order', function (done) {
          const query = "INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)";
          const id = types.Uuid.random();
          const value = DateRange.fromString('[2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z]');
          client.execute(query, [ id, value ], { prepare: prepare }, function (err) {
            if (!err) {
              assert.ifError('Should throw error when DataRange order is inverted');
            }
            done();
          });
        });
        it('should select DateRange using JSON', function (done) {
          utils.eachSeries(values, function (v, next) {
            const query = 'INSERT INTO tbl_date_range (pk, c1) VALUES (?, ?)';
            const id = types.Uuid.random();
            const value = DateRange.fromString(v);
            client.execute(query, [ id, value ], { prepare: prepare }, function (err) {
              assert.ifError(err);
              const query = 'SELECT JSON c1 FROM tbl_date_range WHERE pk = ?';
              client.execute(query, [ id ], { prepare: prepare }, function (err, result) {
                assert.ifError(err);
                assert.strictEqual(result.rowLength, 1);
                const jsonRowStr = result.first()['[json]'];
                assert.strictEqual(jsonRowStr, util.format('{"c1": "%s"}', value));
                const jsonRow = JSON.parse(jsonRowStr);
                const actual = DateRange.fromString(jsonRow.c1);
                assert.ok(actual.equals(value));
                next();
              });
            });
          }, done);
        });
      });
    });
  });
});
