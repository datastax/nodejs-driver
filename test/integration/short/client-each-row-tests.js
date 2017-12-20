"use strict";
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
  describe('#eachRow(query, params, {prepare: 0})', function () {
    const setupInfo = helper.setup(1);
    it('should callback per row and the end callback', function (done) {
      const client = newInstance();
      const query = helper.queries.basic;
      let counter = 0;
      client.eachRow(query, [], {prepare: false}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 1);
        client.shutdown();
        done();
      });
    });
    it('should allow calls without end callback', function (done) {
      const client = setupInfo.client;
      const query = helper.queries.basic;
      client.eachRow(query, [], {}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        done();
      });
    });
    it('should end callback when no rows', function (done) {
      const client = setupInfo.client;
      const query = helper.queries.basicNoResults;
      let counter = 0;
      client.eachRow(query, [], {}, function () {
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 0);
        done();
      });
    });
    it('should end callback when VOID result', function (done) {
      const client = setupInfo.client;
      const keyspace = helper.getRandomName('ks');
      const query = helper.createKeyspaceCql(keyspace, 1);
      let counter = 0;
      client.eachRow(query, [], {}, function () {
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 0);
        done();
      });
    });
    it('should call rowCallback per each row', function (done) {
      const client = setupInfo.client;
      const table = helper.getRandomName('table');
      var length = 300;
      var noop = function () {};
      let counter = 0;
      utils.series([
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          const query = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'text%s\')';
          utils.timesSeries(length, function (n, timesNext) {
            client.eachRow(util.format(query, table, types.Uuid.random(), n), [], noop, timesNext);
          }, next);
        },
        function select(next) {
          client.eachRow(util.format('SELECT * FROM %s', table), [], function (n, row) {
            assert.strictEqual(n, counter++);
            assert.ok(row instanceof types.Row);
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(counter, length);
            //rowLength should be exposed
            assert.strictEqual(counter, result.rowLength);
            next();
          });
        }], done);
    });
    it('should fail if non-existent profile provided', function (done) {
      const client = setupInfo.client;
      utils.series([
        function queryWithBadProfile(next) {
          let counter = 0;
          client.eachRow(helper.queries.basicNoResults, [], {executionProfile: 'none'}, function() {
            counter++;
          }, function (err) {
            assert.ok(err);
            helper.assertInstanceOf(err, errors.ArgumentError);
            assert.strictEqual(counter, 0);
            next();
          });
        }
      ], done);
    });
    vit('2.0', 'should autoPage', function (done) {
      const table = helper.getRandomName('table');
      const client = setupInfo.client;
      var noop = function () {};
      utils.series([
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n.toString()], noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          //It should fetch 3 times, a total of 100 rows (45+45+10)
          const query = util.format('SELECT * FROM %s', table);
          let rowCount = 0;
          client.eachRow(query, [], {fetchSize: 45, autoPage: true}, function () {
            rowCount++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(rowCount, 100);
            assert.strictEqual(rowCount, result.rowLength);
            seriesNext();
          });
        },
        function selectDataOnePage(seriesNext) {
          //It should fetch 1 time, a total of 100 rows (even if asked more)
          const query = util.format('SELECT * FROM %s', table);
          let rowCount = 0;
          client.eachRow(query, [], {fetchSize: 2000, autoPage: true}, function () {
            rowCount++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(rowCount, 100);
            assert.strictEqual(rowCount, result.rowLength);
            seriesNext();
          });
        }
      ], done);
    });
  });
  describe('#eachRow(query, params, {prepare: 1})', function () {
    const table = helper.getRandomName('table');
    const setupInfo = helper.setup(3, {
      ccmOptions: { jvmArgs: ['-Dcassandra.wait_for_tracing_events_timeout_secs=-1'] },
      replicationFactor: 3,
      queries: [ helper.createTableCql(table) ]
    });
    const queryOptions = { prepare: true, consistency: types.consistencies.quorum };
    it('should callback per row and the end callback', function (done) {
      const client = setupInfo.client;
      const query = helper.queries.basic;
      let counter = 0;
      client.eachRow(query, [], {prepare: true}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 1);
        done();
      });
    });
    it('should call rowCallback per each row', function (done) {
      const client = setupInfo.client;
      const table = helper.getRandomName('table');
      var length = 500;
      var noop = function () {};
      let counter = 0;
      utils.series([
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.timesSeries(length, function (n, timesNext) {
            client.eachRow(query, [ types.Uuid.random(), 'text-' + n ], queryOptions, noop, timesNext);
          }, next);
        },
        function select(next) {
          client.eachRow(util.format('SELECT * FROM %s', table), [], queryOptions, function (n, row) {
            assert.strictEqual(n, counter++);
            assert.ok(row instanceof types.Row);
          }, function (err) {
            assert.ifError(err);
            assert.strictEqual(counter, length);
            next();
          });
        }], done);
    });
    it('should allow maps with float values NaN and infinite values', function (done) {
      const client = setupInfo.client;
      var values = [
        [ 'finite', { val: 1 }],
        [ 'NaN', { val: NaN }],
        [ 'Infinite', { val: Number.POSITIVE_INFINITY }],
        [ 'Negative Infinite', { val: Number.NEGATIVE_INFINITY }]
      ];
      const expectedValues = {};
      utils.series([
        function createTable(next) {
          const query = 'CREATE TABLE tbl_map_floats (id text PRIMARY KEY, data map<text, float>)';
          client.execute(query, next);
        },
        function insertData(next) {
          const query = 'INSERT INTO tbl_map_floats (id, data) VALUES (?, ?)';
          utils.eachSeries(values, function (params, eachNext) {
            expectedValues[params[0]] = params[1].val;
            client.execute(query, params, queryOptions, eachNext);
          }, next);
        },
        function retrieveData(next) {
          client.eachRow('SELECT * FROM tbl_map_floats', [], queryOptions, function (n, row) {
            const expected = expectedValues[row['id']];
            if (isNaN(expected)) {
              assert.ok(isNaN(row['data'].val));
            }
            else {
              assert.strictEqual(row['data'].val, expected);
            }
          }, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.rowLength, values.length);
            next();
          });
        }
      ], done);
    });
    vit('2.0', 'should autoPage on parallel different tables', function (done) {
      const keyspace = helper.getRandomName('ks');
      var table1 = keyspace + '.' + helper.getRandomName('table');
      var table2 = keyspace + '.' + helper.getRandomName('table');
      const client = newInstance({ queryOptions: { consistency: types.consistencies.quorum }});
      var noop = function () {};
      utils.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table1), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table2), [], noop, helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table1);
          utils.timesLimit(200, 25, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n.toString()], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table2);
          utils.timesLimit(135, 25, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n+1], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          utils.parallel([
            function (parallelNext) {
              const query = util.format('SELECT * FROM %s', table1);
              let rowCount = 0;
              client.eachRow(query, [], {fetchSize: 39, autoPage: true, prepare: true}, function (n, row) {
                assert.ok(row['text_sample']);
                rowCount++;
              }, function (err, result) {
                validateResult(err, result, rowCount, 200, 39);
                parallelNext();
              });
            },
            function (parallelNext) {
              const query = util.format('SELECT * FROM %s', table2);
              let rowCount = 0;
              client.eachRow(query, [], { fetchSize: 23, autoPage: true, prepare: true }, function (n, row) {
                rowCount++;
                assert.ok(row['int_sample']);
              }, function (err, result) {
                validateResult(err, result, rowCount, 135, 23);
                parallelNext();
              });
            }
          ], seriesNext);
        }
      ], done);
      function validateResult(err, result, rowCount, expectedLength){
        assert.ifError(err);
        assert.strictEqual(rowCount, expectedLength);
        assert.strictEqual(rowCount, result.rowLength);
      }
    });
    vit('2.0', 'should use pageState and fetchSize', function (done) {
      const client = newInstance({
        keyspace: setupInfo.keyspace,
        queryOptions: { consistency: types.consistencies.quorum }
      });
      let metaPageState;
      let pageState;
      utils.series([
        helper.toTask(insertTestData, null, client, table, 131),
        function selectData(seriesNext) {
          //Only fetch 70
          let counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 70}, function () {
            counter++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(counter, 70);
            assert.strictEqual(result.rowLength, counter);
            pageState = result.pageState;
            metaPageState = result.meta.pageState;
            seriesNext();
          });
        },
        function selectDataRemaining(seriesNext) {
          //The remaining
          let counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 70, pageState: pageState}, function (n, row) {
            assert.ok(row);
            counter++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 61);
            assert.strictEqual(counter, result.rowLength);
            seriesNext();
          });
        },
        function selectDataRemainingWithMetaPageState(seriesNext) {
          //The remaining
          let counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 70, pageState: metaPageState}, function (n, row) {
            assert.ok(row);
            counter++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 61);
            assert.strictEqual(counter, result.rowLength);
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.0', 'should expose result.nextPage() method', function (done) {
      const client = newInstance({
        keyspace: setupInfo.keyspace,
        queryOptions: { consistency: types.consistencies.quorum }
      });
      let pageState;
      let nextPageRows;
      utils.series([
        client.connect.bind(client),
        helper.toTask(insertTestData, null, client, table, 110),
        function selectData(seriesNext) {
          //Only fetch 60 the first time, 50 the following
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 60}, function (n, row) {
            if (nextPageRows) {
              nextPageRows.push(row);
            }
          }, function (err, result) {
            assert.ifError(err);
            helper.assertInstanceOf(result, types.ResultSet);
            if (!nextPageRows) {
              //the first time, it should have a next page
              assert.strictEqual(typeof result.nextPage, 'function');
              assert.strictEqual(typeof result.pageState, 'string');
              nextPageRows = [];
              pageState = result.pageState;
              //call to retrieve the following page rows.
              result.nextPage();
              return;
            }
            //the following times, there shouldn't be any additional page
            assert.strictEqual(typeof result.nextPage, 'undefined');
            assert.equal(result.pageState, null);
            seriesNext();
          });
        },
        function selectDataRemaining(seriesNext) {
          //Select the remaining with pageState and compare the results.
          let counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 100, pageState: pageState}, function (n, row) {
            assert.ok(row);
            counter++;
            assert.strictEqual(row['id'].toString(), nextPageRows[n]['id'].toString());
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 50);
            assert.strictEqual(counter, result.rowLength);
            seriesNext();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.0', 'should not expose result.nextPage() method when no more rows', function (done) {
      const client = newInstance({
        keyspace: setupInfo.keyspace,
        queryOptions: { consistency: types.consistencies.quorum }
      });
      let counter = 0;
      var rowLength = 10;
      utils.series([
        client.connect.bind(client),
        helper.toTask(insertTestData, null, client, table, rowLength),
        function assertNextPageNull(next) {
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 1000}, function () {
            counter++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, rowLength);
            assert.strictEqual(counter, result.rowLength);
            assert.strictEqual(typeof result.nextPage, 'undefined');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the trace id when queryTrace flag is set', function (done) {
      const client = newInstance({
        keyspace: setupInfo.keyspace,
        queryOptions: { consistency: types.consistencies.quorum }
      });
      const id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function selectNotExistent(next) {
          const query = util.format('SELECT * FROM %s WHERE id = ?', table);
          let called = 0;
          client.eachRow(query, [types.Uuid.random()], {prepare: true, traceQuery: true}, function () {
            called++;
          }, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(called, 0);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function insertQuery(next) {
          const query = util.format('INSERT INTO %s (id) VALUES (?)', table);
          let called = 0;
          client.eachRow(query, [id], { prepare: true, traceQuery: true}, function () {
            called++;
          }, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(called, 0);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function selectSingleRow(next) {
          const query = util.format('SELECT * FROM %s WHERE id = ?', table);
          let called = 0;
          client.eachRow(query, [id], { prepare: true, traceQuery: true}, function () {
            called++;
          }, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(called, 1);
            assert.strictEqual(result.rowLength, 1);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        }
      ], done);
    });
    helper.vit('2.2', 'should include the warning in the ResultSet', function (done) {
      const client = newInstance({ keyspace: setupInfo.client.keyspace });
      let loggedMessage = false;
      client.on('log', function (level, className, message) {
        if (loggedMessage || level !== 'warning') {
          return;
        }
        message = message.toLowerCase();
        if (message.indexOf('batch') >= 0 && message.indexOf('exceeding')) {
          loggedMessage = true;
        }
      });
      const query = util.format(
        "BEGIN UNLOGGED BATCH INSERT INTO %s (id, text_sample) VALUES (:id1, :sample)\n" +
        "INSERT INTO %s (id, text_sample) VALUES (:id2, :sample) APPLY BATCH",
        table,
        table
      );
      var params = { id1: types.Uuid.random(), id2: types.Uuid.random(), sample: utils.stringRepeat('c', 2562) };
      client.eachRow(query, params, { prepare: true }, utils.noop, function (err, result) {
        assert.ifError(err);
        assert.ok(result.info.warnings);
        assert.strictEqual(result.info.warnings.length, 1);
        helper.assertContains(result.info.warnings[0], 'batch');
        helper.assertContains(result.info.warnings[0], 'exceeding');
        assert.ok(loggedMessage);
        client.shutdown(done);
      });
    });
    if (!helper.isWin()) {
      vit('2.0', 'should retrieve large result sets in parallel', function (done) {
        insertSelectTest(setupInfo.keyspace + '.' + table, 50000, 20, 50000, { prepare: true }, done);
      });
      vit('2.0', 'should query multiple times in parallel with query tracing enabled', function (done) {
        insertSelectTest(setupInfo.keyspace + '.' + table, 50000, 2000, 10, { prepare: true, traceQuery: true }, done);
      });
    }
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  options = options || {};
  options = utils.deepExtend(options, helper.baseOptions);
  return new Client(options);
}

function insertTestData(client, table, length, callback) {
  utils.series([
    function truncate(seriesNext) {
      client.eachRow('TRUNCATE ' + table, [], helper.noop, seriesNext);
    },
    function insertData(seriesNext) {
      const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
      var value = utils.stringRepeat('abcdefghij', 10);
      utils.timesLimit(length, 100, function (n, next) {
        client.eachRow(query, [types.Uuid.random(), value], {prepare: 1}, helper.noop, next);
      }, seriesNext);
    }
  ], callback);
}

function insertSelectTest(table, rowLength, times, selectLimit, selectOptions, done) {
  const client = newInstance({
    queryOptions: {
      consistency: types.consistencies.quorum,
      fetchSize: rowLength
    },
    socketOptions: {
      readTimeout: 100000
    }
  });
  client.on('log', helper.log(['warning', 'error']));
  const query = util.format('SELECT * FROM %s LIMIT %d', table, selectLimit);
  utils.series([
    client.connect.bind(client),
    helper.toTask(insertTestData, null, client, table, rowLength),
    function selectData(seriesNext) {
      utils.timesLimit(times, 5, function (n, timesNext) {
        let counter = 0;
        client.eachRow(query, [], selectOptions, function (n, row) {
          assert.ok(row);
          counter++;
        }, function (err, result) {
          assert.ifError(err);
          assert.strictEqual(result.rowLength, counter);
          timesNext();
        });
      }, seriesNext);
    }
  ], helper.finish(client, done));
}