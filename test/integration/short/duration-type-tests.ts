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
import assert from "assert";
import util from "util";
import helper from "../../test-helper";
import types from "../../../lib/types/index";
import utils from "../../../lib/utils";


const vdescribe = helper.vdescribe;
const Duration = types.Duration;

vdescribe('3.10', 'Duration @SERVER_API', function () {

  this.timeout(120000);

  const setupInfo = helper.setup(1, {
    queries: ['CREATE TABLE tbl_duration (pk uuid PRIMARY KEY, c1 duration)']
  });

  const client = setupInfo.client;

  describe('serialization', function () {
    it('should serialize and deserialize duration type instances', function (done) {
      const values = [
        '1y2mo',
        '-1y2mo',
        '1Y2MO',
        '2w',
        '2d10h',
        '2d',
        '30h',
        '30h20m',
        '20m',
        '56s',
        '567ms',
        '1950us',
        '1950µs',
        '1950000ns',
        '1950000NS',
        '-1950000ns',
        '9223372036854775807ns',
        '-9223372036854775808ns',
        '1y3mo2h10m',
        'P1Y2D',
        'P1Y2M',
        'P2W',
        'P1YT2H',
        '-P1Y2M',
        'P2D',
        'PT30H',
        'PT30H20M',
        'PT20M',
        'PT56S',
        'P1Y3MT2H10M',
        'P0001-00-02T00:00:00',
        'P0001-02-00T00:00:00',
        'P0001-00-00T02:00:00',
        '-P0001-02-00T00:00:00',
        'P0000-00-02T00:00:00',
        'P0000-00-00T30:00:00',
        'P0000-00-00T30:20:00',
        'P0000-00-00T00:20:00',
        'P0000-00-00T00:00:56',
        'P0001-03-00T02:10:00'
      ];

      utils.eachSeries([ true, false ], function (prepare, prepareNext) {
        utils.eachSeries(values, function (v, next) {
          const query = 'INSERT INTO tbl_duration (pk, c1) VALUES (?, ?)';
          const id = types.Uuid.random();
          const value = Duration.fromString(v);
          client.execute(query, [ id, value ], { prepare: prepare }, function (err) {
            assert.ifError(err);
            const query = 'SELECT pk, c1 FROM tbl_duration WHERE pk = ?';
            client.execute(query, [ id ], { prepare: prepare }, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              const actual = result.first()['c1'];
              helper.assertInstanceOf(actual, Duration);
              assert.ok(actual.equals(value), util.format('Assertion failed: %j !== %j', actual, value));
              next();
            });
          });
        }, prepareNext);
      }, done);
    });
  });

  describe('metadata', function () {
    it('should parse column metadata', function (done) {
      client.metadata.getTable(setupInfo.keyspace, 'tbl_duration', function (err, tableInfo) {
        assert.ifError(err);
        assert.ok(tableInfo);
        assert.strictEqual(tableInfo.columns.length, 2);
        const c1 = tableInfo.columnsByName['c1'];
        assert.ok(c1);
        assert.strictEqual(c1.type.code, types.dataTypes.duration);
        assert.strictEqual(c1.type.info, null);
        done();
      });
    });
  });
});
