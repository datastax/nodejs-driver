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

const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils.js');
const errors = require('../../../lib/errors.js');
const vit = helper.vit;

describe('Client', function () {
  this.timeout(120000);
  describe('#batch(queries, {prepare: 0}, callback)', function () {
    const keyspace = helper.getRandomName('ks');
    const table1 = keyspace + '.' + helper.getRandomName('tblA');
    const table2 = keyspace + '.' + helper.getRandomName('tblB');
    before(function (done) {
      const client = newInstance();
      utils.series([
        helper.ccmHelper.start(1),
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 1)),
        helper.toTask(client.execute, client, helper.createTableCql(table1)),
        helper.toTask(client.execute, client, helper.createTableCql(table2))
      ], done);
    });
    after(helper.ccmHelper.remove);
    vit('2.0', 'should execute a batch of queries with no params', function (done) {
      const insertQuery = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'%s\')';
      const selectQuery = 'SELECT * FROM %s WHERE id = %s';
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const client = newInstance();
      const queries = [
        util.format(insertQuery, table1, id1, 'one'),
        util.format(insertQuery, table2, id2, 'two')
      ];
      utils.series([
        function (next) {
          client.batch(queries, next);
        },
        function assertValue1(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'one');
            next();
          });
        },
        function assertValue2(next) {
          client.execute(util.format(selectQuery, table2, id2), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'two');
            next();
          });
        }
      ], done);
    });
    vit('2.0', 'should execute a batch of queries with params', function (done) {
      const insertQuery = 'INSERT INTO %s (id, double_sample) VALUES (?, ?)';
      const selectQuery = 'SELECT * FROM %s WHERE id = %s';
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const client = newInstance();
      const queries = [
        {query: util.format(insertQuery, table1), params: [id1, 1000]},
        {query: util.format(insertQuery, table2), params: [id2, 2000.2]}
      ];
      utils.series([
        function (next) {
          client.batch(queries, next);
        },
        function assertValue1(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.equal(result.rows[0].text_sample, null);
            assert.equal(result.rows[0].double_sample, 1000);
            next();
          });
        },
        function assertValue2(next) {
          client.execute(util.format(selectQuery, table2, id2), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].double_sample, 2000.2);
            next();
          });
        }
      ], done);
    });
    vit('2.0', 'should callback with error when there is a ResponseError', function (done) {
      const client = newInstance();
      client.batch(['INSERT WILL FAIL'], function (err) {
        assert.ok(err);
        assert.ok(err instanceof errors.ResponseError);
        assert.ok(err instanceof errors.ResponseError);
        done();
      });
    });
    vit('2.0', 'should fail if non-existent profile provided', function (done) {
      const client = newInstance();
      client.batch(['INSERT WILL FAIL'], {executionProfile: 'none'}, function (err) {
        assert.ok(err);
        assert.ok(err instanceof errors.ArgumentError);
        done();
      });
    });
    vit('2.0', 'should validate that arguments are valid', function () {
      const client = newInstance();
      const badArgumentCalls = [
        function () {
          return client.batch();
        },
        function () {
          return client.batch(['SELECT'], {});
        },
        function () {
          return client.batch({}, {});
        }
      ];

      const promises = badArgumentCalls.map(function (method) {
        return method()
          .then(function () {
            throw new Error('Expected rejected promise for method ' + method.toString());
          })
          .catch(function (err) {
            // should be an Argument Error
            if (!(err instanceof errors.ArgumentError) && !(err instanceof errors.ResponseError)) {
              throw new Error('Expected ArgumentError or ResponseError for method ' + method.toString());
            }
          });
      });
      setImmediate(client.shutdown.bind(client));
      return Promise.all(promises);
    });
    vit('2.0', 'should allow parameters without hints', function (done) {
      const client = newInstance();
      const query = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table1);
      utils.series([
        client.connect.bind(client),
        function (next) {
          client.batch([{query: query, params: [types.Uuid.random(), null]}], next);
        },
        function (next) {
          client.batch(
            [{query: query, params: [types.Uuid.random(), null]}],
            {logged: false, consistency: types.consistencies.quorum},
            next);
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.0', 'should use hints when provided', function (done) {
      const client = newInstance();
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const queries = [
        { query: util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table1),
          params: [id1, 'sample1']
        },
        { query: util.format('INSERT INTO %s (id, int_sample, bigint_sample) VALUES (?, ?, ?)', table1),
          params: [id2, -1, -1]
        }
      ];
      const hints = [
        null,
        [null, 'int', 'bigint']
      ];
      client.batch(queries, {hints: hints, consistency: types.consistencies.quorum}, function (err) {
        assert.ifError(err);
        const query = util.format('SELECT * FROM %s where id IN (%s, %s)', table1, id1, id2);
        client.execute(query, [], {consistency: types.consistencies.quorum}, function (err, result) {
          assert.ifError(err);
          assert.ok(result && result.rows);
          assert.strictEqual(result.rows.length, 2);
          assert.strictEqual(helper.find(result.rows, row => row.id.equals(id2))['int_sample'], -1);
          done();
        });
      });
    });
    vit('2.0', 'should callback in err when wrong hints are provided', function (done) {
      const client = newInstance();
      const queries = [{
        query: util.format('INSERT INTO %s (id, text_sample, double_sample) VALUES (?, ?, ?)', table1),
        params: [types.Uuid.random(), 'what', 1]
      }];
      utils.series([
        client.connect.bind(client),
        function hintsArrayAsObject(next) {
          client.batch(queries, {hints: {}}, function (err) {
            //it should not fail, dismissed
            next(err);
          });
        },
        function hintsDifferentAmount(next) {
          client.batch(queries, {hints: [['uuid']]}, function (err) {
            //it should not fail
            next(err);
          });
        },
        function hintsEmptyArray(next) {
          client.batch(queries, {hints: [[]]}, function (err) {
            next(err);
          });
        },
        function hintsArrayWrongSubtype(next) {
          client.batch(queries, {hints: [{what: true}]}, function (err) {
            next(err);
          });
        },
        function hintsInvalidStrings(next) {
          client.batch(queries, {hints: [['zzz', 'mmmm']]}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should support protocol level timestamp', function (done) {
      const insertQuery = 'INSERT INTO %s (id, text_sample) VALUES (?, ?)';
      const selectQuery = 'SELECT id, text_sample, writetime(text_sample) FROM %s WHERE id = %s';
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const client = newInstance();
      const timestamp = types.Long.fromString('1428311323417123');
      const queries = [
        {query: util.format(insertQuery, table1), params: [id1, 'value 1 with timestamp']},
        {query: util.format(insertQuery, table2), params: [id2, 'value 2 with timestamp']}
      ];
      utils.series([
        function (next) {
          client.batch(queries, { timestamp: timestamp}, next);
        },
        function assertValue1(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.first());
            assert.strictEqual(result.first()['text_sample'], 'value 1 with timestamp');
            helper.assertInstanceOf(result.first()['writetime(text_sample)'], types.Long);
            assert.strictEqual(result.first()['writetime(text_sample)'].toString(), timestamp.toString());
            next();
          });
        },
        function assertValue2(next) {
          client.execute(util.format(selectQuery, table2, id2), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.first());
            assert.strictEqual(result.first()['text_sample'], 'value 2 with timestamp');
            assert.strictEqual(result.first()['writetime(text_sample)'].toString(), timestamp.toString());
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should support serial consistency', function (done) {
      const insertQuery = 'INSERT INTO %s (id, text_sample) VALUES (?, ?)';
      const selectQuery = 'SELECT id, text_sample, writetime(text_sample) FROM %s WHERE id = %s';
      const id1 = types.Uuid.random();
      const client = newInstance();
      const queries = [
        {query: util.format(insertQuery, table1), params: [id1, 'value with serial']}
      ];
      utils.series([
        client.connect.bind(client),
        function (next) {
          client.batch(queries, { serialConsistency: types.consistencies.localSerial}, next);
        },
        function assertValue(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.first());
            assert.strictEqual(result.first()['text_sample'], 'value with serial');
            next();
          });
        },
        client.shutdown.bind(client),
      ], done);
    });
    describe('with no callback specified', function () {
      vit('2.0', 'should return a promise when batch completes', function () {
        const insertQuery = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'%s\')';
        const selectQuery = 'SELECT * FROM %s WHERE id = %s';
        const id1 = types.Uuid.random();
        const id2 = types.Uuid.random();
        const client = newInstance();
        const queries = [
          util.format(insertQuery, table1, id1, 'one'),
          util.format(insertQuery, table2, id2, 'two')
        ];

        return client.batch(queries)
          .then(function (result) {
            assert.ok(result);
            return client.execute(util.format(selectQuery, table1, id1));
          })
          .then(function (result) {
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'one');
            return client.execute(util.format(selectQuery, table2, id2));
          })
          .then(function (result) {
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'two');
          });
      });
    });
  });
  describe('#batch(queries, {prepare: 1}, callback)', function () {
    const keyspace = helper.getRandomName('ks');
    const table1Short = helper.getRandomName('tblA');
    const table1 = keyspace + '.' + table1Short;
    const table2 = keyspace + '.' + helper.getRandomName('tblB');
    before(function (done) {
      const client = newInstance();
      const createTableCql = 'CREATE TABLE %s (' +
        ' id uuid,' +
        ' time timeuuid,' +
        ' text_sample text,' +
        ' int_sample int,' +
        ' bigint_sample bigint,' +
        ' float_sample float,' +
        ' double_sample double,' +
        ' decimal_sample decimal,' +
        ' varint_sample decimal,' +
        ' timestamp_sample timestamp,' +
        ' PRIMARY KEY (id, time))';
      utils.series([
        helper.ccmHelper.start(3),
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3, false)),
        helper.toTask(client.execute, client, util.format(createTableCql, table1)),
        helper.toTask(client.execute, client, util.format(createTableCql, table2))
      ], done);
    });
    after(helper.ccmHelper.remove);
    vit('2.0', 'should prepare and send the request', function (done) {
      const client = newInstance();
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const consistency = types.consistencies.quorum;
      const queries = [{
        query: util.format('INSERT INTO %s (id, time, text_sample) VALUES (?, ?, ?)', table1),
        params: [id1, types.timeuuid(), 'sample1']
      },{
        query: util.format('INSERT INTO %s (id, time, int_sample, varint_sample) VALUES (?, ?, ?, ?)', table2),
        params: [id2, types.timeuuid(), -101, '151']
      }];
      client.batch(queries, {prepare: true, consistency: consistency}, function (err) {
        assert.ifError(err);
        const query = 'SELECT * FROM %s where id = %s';
        utils.series([
          function (next) {
            client.execute(util.format(query, table1, id1), [], {consistency: consistency}, function (err, result) {
              assert.ifError(err);
              const row1 = result.first();
              assert.strictEqual(row1['text_sample'], 'sample1');
              next();
            });
          },
          function (next) {
            client.execute(util.format(query, table2, id2), [], {consistency: consistency}, function (err, result) {
              assert.ifError(err);
              const row2 = result.first();
              assert.strictEqual(row2['int_sample'], -101);
              assert.strictEqual(row2['varint_sample'].toString(), '151');
              next();
            });
          }
        ], done);
      });
    });
    vit('2.0', 'should callback in error when the one of the queries contains syntax error', function (done) {
      const client = newInstance();
      const queries1 = [{
        query: util.format('INSERT INTO %s (id, time, text_sample) VALUES (?, ?, ?)', table2),
        params: [types.Uuid.random(), types.timeuuid(), 'sample1']
      },{
        query: util.format('INSERT WILL FAIL'),
        params: [types.Uuid.random(), types.timeuuid(), -101, -1]
      }];
      const queries2 = [queries1[1], queries1[1]];
      utils.times(10, function (n, next) {
        const queries = (n % 2 === 0) ? queries1 : queries2;
        client.batch(queries, {prepare: true}, function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
          next();
        });
      }, done);
    });
    vit('2.0', 'should callback in error when the type does not match', function (done) {
      const client = newInstance();
      const queries = [{
        query: util.format('INSERT INTO %s (id, time, int_sample) VALUES (?, ?, ?)', table1),
        params: [types.Uuid.random(), types.timeuuid(), {notValid: true}]
      }];
      utils.times(10, function (n, next) {
        client.batch(queries, {prepare: true}, function (err) {
          helper.assertInstanceOf(err, TypeError);
          next();
        });
      }, done);
    });
    vit('2.0', 'should handle multiple prepares in parallel', function (done) {
      const consistency = types.consistencies.quorum;
      const id1Tbl1 = types.Uuid.random();
      const id1Tbl2 = types.Uuid.random();
      const id2Tbl1 = types.Uuid.random();
      const id2Tbl2 = types.Uuid.random();
      //Avoid using the same queries from test to test, include hardcoded values
      const query1Table1 = util.format('INSERT INTO %s (id, time, decimal_sample, int_sample) VALUES (?, ?, ?, 201)', table1);
      const query1Table2 = util.format('INSERT INTO %s (id, time, timestamp_sample, int_sample) VALUES (?, ?, ?, 202)', table2);
      const query2Table1 = util.format('INSERT INTO %s (id, time, decimal_sample, int_sample) VALUES (?, ?, ?, 301)', table1);
      const query2Table2 = util.format('INSERT INTO %s (id, time, float_sample, int_sample) VALUES (?, ?, ?, 302)', table2);
      const client = newInstance();
      utils.parallel([
        function (next) {
          utils.timesLimit(120, 100, function (n, eachNext) {
            const queries = [{
              query: query1Table1,
              params: [id1Tbl1, types.timeuuid(), types.BigDecimal.fromNumber(new Date().getTime())]
            }, {
              query: query1Table2,
              params: [id1Tbl2, types.timeuuid(), new Date()]
            }];
            client.batch(queries, {prepare: true, consistency: consistency}, eachNext);
          }, next);
        },
        function (next) {
          utils.timesLimit(120, 100, function (n, eachNext) {
            const queries = [{
              query: query2Table1,
              params: [id2Tbl1, types.timeuuid(), types.BigDecimal.fromNumber(new Date().getTime())]
            }, {
              query: query2Table2,
              params: [id2Tbl2, types.timeuuid(), new Date().getTime() / 15]
            }];
            client.batch(queries, {prepare: true, consistency: consistency}, eachNext);
          }, next);
        }
      ], function (err) {
        if(err) {
          return done(err);
        }
        //verify results in both tables
        const q = 'SELECT * FROM %s where id IN (%s, %s)';
        utils.series([
          function (next) {
            const query = util.format(q, table1, id1Tbl1, id2Tbl1);
            client.execute(query, [], { consistency: consistency }, function (err, result) {
              assert.ifError(err);
              const rows1 = result.rows;
              assert.strictEqual(rows1.length, 240);
              next();
            });
          },
          function (next) {
            const query = util.format(q, table2, id1Tbl2, id2Tbl2);
            client.execute(query, [], { consistency: consistency }, function (err, result) {
              assert.ifError(err);
              const rows2 = result.rows;
              assert.strictEqual(rows2.length, 240);
              next();
            });
          }
        ], done);
      });
    });
    vit('2.0', 'should allow named parameters', function (done) {
      const client = newInstance();
      const id1 = types.Uuid.random();
      const id2 = types.Uuid.random();
      const consistency = types.consistencies.quorum;
      const queries = [{
        query: util.format('INSERT INTO %s (id, time, text_sample) VALUES (:paramId, :time, :text_sample)', table1),
        params: { text_SAMPLE: 'named params', paramID: id1, time: types.TimeUuid.now()}
      },{
        query: util.format('INSERT INTO %s (id, time, int_sample, varint_sample) VALUES (?, ?, ?, ?)', table2),
        params: [id2, types.TimeUuid.now(), 501, '2010']
      }];
      client.batch(queries, {prepare: true, consistency: consistency}, function (err) {
        assert.ifError(err);
        const query = 'SELECT * FROM %s where id = %s';
        utils.series([
          function (next) {
            client.execute(util.format(query, table1, id1), [], {consistency: consistency}, function (err, result) {
              assert.ifError(err);
              const row1 = result.first();
              assert.strictEqual(row1['text_sample'], 'named params');
              next();
            });
          },
          function (next) {
            client.execute(util.format(query, table2, id2), [], {consistency: consistency}, function (err, result) {
              assert.ifError(err);
              const row2 = result.first();
              assert.strictEqual(row2['int_sample'], 501);
              assert.strictEqual(row2['varint_sample'].toString(), '2010');
              next();
            });
          }
        ], done);
      });
    });
    vit('2.0', 'should execute batch containing the same query multiple times', function (done) {
      const client = newInstance({
        queryOptions: { consistency: types.consistencies.quorum }
      });
      const id = types.Uuid.random();
      const query = util.format('INSERT INTO %s (id, time, int_sample) VALUES (?, ?, ?)', table1);
      const queries = [
        { query: query, params: [id, types.TimeUuid.now(), 1000]},
        { query: query, params: [id, types.TimeUuid.now(), 2000]}
      ];
      client.batch(queries, { prepare: true }, function (err) {
        assert.ifError(err);
        //Check values inserted
        const selectQuery = util.format('SELECT int_sample FROM %s WHERE id = ?', table1);
        client.execute(selectQuery, [id], function (err, result) {
          assert.ifError(err);
          assert.strictEqual(result.rows.length, 2);
          assert.ok(helper.find(result.rows, 'int_sample', 1000));
          assert.ok(helper.find(result.rows, 'int_sample', 2000));
          done();
        });
      });
    });
    vit('dse-6.0', 'should use keyspace if set on options', () => {
      const client = newInstance({});
      const insertQuery = 'INSERT INTO %s (id, time, double_sample) VALUES (?, ?, ?)';
      const selectQuery = 'SELECT * FROM %s WHERE id=?';
      const id = types.Uuid.random();
      const ts1 = types.timeuuid();
      const ts2 = types.timeuuid();
      const queries = [
        {query: util.format(insertQuery, table1Short), params: [id, ts1, 1000]},
        {query: util.format(insertQuery, table1Short), params: [id, ts2, 2000]}
      ];

      return client.batch(queries, {prepare: true, keyspace: keyspace})
        .then((result) =>
          client.execute(util.format(selectQuery, table1Short), [id], {prepare: true, keyspace: keyspace})
        )
        .then((result) => {
          assert.ok(result);
          assert.ok(result.rows);
          assert.strictEqual(result.rows.length, 2);
          assert.equal(result.rows[0].double_sample, 1000);
          assert.equal(result.rows[1].double_sample, 2000);
          return client.shutdown();
        });
    });
    it('should not use keyspace if set on options for lower protocol versions', function () {
      if (helper.isDseGreaterThan('6.0')) {
        return this.skip();
      }
      const client = newInstance({});
      const insertQuery = 'INSERT INTO %s (id, time, double_sample) VALUES (?, ?, ?)';
      const id = types.Uuid.random();
      const ts1 = types.timeuuid();
      const ts2 = types.timeuuid();
      const queries = [
        {query: util.format(insertQuery, table1Short), params: [id, ts1, 1000]},
        {query: util.format(insertQuery, table1Short), params: [id, ts2, 2000]}
      ];
      return client.batch(queries, {prepare: true, keyspace: keyspace})
        .then((result) => {
          throw new Error('should have failed');
        })
        .catch(function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          return client.shutdown();
        });
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  return helper.shutdownAfterThisTest(new Client(utils.deepExtend({}, helper.baseOptions, options)));
}
