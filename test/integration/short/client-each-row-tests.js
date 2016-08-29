"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');
var vit = helper.vit;

describe('Client', function () {
  this.timeout(120000);
  describe('#eachRow(query, params, {prepare: 0})', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should callback per row and the end callback', function (done) {
      var client = newInstance();
      var query = helper.queries.basic;
      var counter = 0;
      //fail if its preparing
      //noinspection JSAccessibilityCheck
      client._getPrepared = function () {throw new Error('Prepared should not be called')};
      client.eachRow(query, [], {prepare: false}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 1);
        done();
      });
    });
    it('should allow calls without end callback', function (done) {
      var client = newInstance();
      var query = helper.queries.basic;
      client.eachRow(query, [], {}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        done();
      });
    });
    it('should end callback when no rows', function (done) {
      var client = newInstance();
      var query = helper.queries.basicNoResults;
      var counter = 0;
      client.eachRow(query, [], {}, function () {
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 0);
        done();
      });
    });
    it('should end callback when VOID result', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var query = helper.createKeyspaceCql(keyspace, 1);
      var counter = 0;
      client.eachRow(query, [], {}, function () {
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 0);
        done();
      });
    });
    it('should call rowCallback per each row', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 300;
      var noop = function () {};
      var counter = 0;
      utils.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 1), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          var query = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'text%s\')';
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
      var client = newInstance();
      utils.series([
        function queryWithBadProfile(next) {
          var counter = 0;
          client.eachRow(helper.queries.basicNoResults, [], {executionProfile: 'none'}, function() {
            counter++;
          }, function (err) {
            assert.ok(err);
            helper.assertInstanceOf(err, errors.ArgumentError);
            assert.strictEqual(counter, 0);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.0', 'should autoPage', function (done) {
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var client = newInstance();
      var noop = function () {};
      utils.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 1), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n.toString()], noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          //It should fetch 3 times, a total of 100 rows (45+45+10)
          var query = util.format('SELECT * FROM %s', table);
          var rowCount = 0;
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
          var query = util.format('SELECT * FROM %s', table);
          var rowCount = 0;
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
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    var noop = function () {};
    before(function (done) {
      var client = newInstance();
      utils.series([
        helper.ccmHelper.start(3),
        function (next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], noop, next);
        },
        function (next) {
          client.eachRow(helper.createTableCql(table), [], noop, next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should callback per row and the end callback', function (done) {
      var client = newInstance();
      var query = helper.queries.basic;
      var counter = 0;
      //noinspection JSAccessibilityCheck
      var originalGetPrepared = client._getPrepared;
      var prepareCalled = false;
      //noinspection JSAccessibilityCheck
      client._getPrepared = function () {
        prepareCalled = true;
        originalGetPrepared.apply(client, arguments);
      };
      client.eachRow(query, [], {prepare: true}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 1);
        assert.strictEqual(prepareCalled, true);
        done();
      });
    });
    it('should call rowCallback per each row', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 500;
      var noop = function () {};
      var counter = 0;
      utils.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], {prepare: true}, noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], {prepare: true}, noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          var query = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'text%s\')';
          utils.timesSeries(length, function (n, timesNext) {
            client.eachRow(util.format(query, table, types.Uuid.random(), n), [], {prepare: true}, noop, timesNext);
          }, next);
        },
        function select(next) {
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: true}, function (n, row) {
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
      var client = newInstance({ keyspace: keyspace});
      var values = [
        [ 'finite', { val: 1 }],
        [ 'NaN', { val: NaN }],
        [ 'Infinite', { val: Number.POSITIVE_INFINITY }],
        [ 'Negative Infinite', { val: Number.NEGATIVE_INFINITY }]
      ];
      var queryOptions = { prepare: true, consistency: types.consistencies.quorum };
      var expectedValues = {};
      utils.series([
        client.connect.bind(client),
        function createTable(next) {
          var query = 'CREATE TABLE tbl_map_floats (id text PRIMARY KEY, data map<text, float>)';
          client.execute(query, next);
        },
        function insertData(next) {
          var query = 'INSERT INTO tbl_map_floats (id, data) VALUES (?, ?)';
          utils.eachSeries(values, function (params, eachNext) {
            expectedValues[params[0]] = params[1].val;
            client.execute(query, params, queryOptions, eachNext);
          }, next);
        },
        function retrieveData(next) {
          client.eachRow('SELECT * FROM tbl_map_floats', [], queryOptions, function (n, row) {
            var expected = expectedValues[row['id']];
            if (isNaN(expected)) {
              assert.ok(isNaN(row['data'].val));
            }
            else {
              assert.strictEqual(row['data'].val, expected)
            }
          }, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.strictEqual(result.rowLength, values.length);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.0', 'should autoPage on parallel different tables', function (done) {
      var keyspace = helper.getRandomName('ks');
      var table1 = keyspace + '.' + helper.getRandomName('table');
      var table2 = keyspace + '.' + helper.getRandomName('table');
      var client = newInstance({ queryOptions: { consistency: types.consistencies.quorum }});
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
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table1);
          utils.timesLimit(200, 25, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n.toString()], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table2);
          utils.timesLimit(135, 25, function (n, next) {
            client.eachRow(query, [types.Uuid.random(), n+1], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          utils.parallel([
            function (parallelNext) {
              var query = util.format('SELECT * FROM %s', table1);
              var rowCount = 0;
              client.eachRow(query, [], {fetchSize: 39, autoPage: true, prepare: true}, function (n, row) {
                assert.ok(row['text_sample']);
                rowCount++;
              }, function (err, result) {
                validateResult(err, result, rowCount, 200, 39);
                parallelNext();
              });
            },
            function (parallelNext) {
              var query = util.format('SELECT * FROM %s', table2);
              var rowCount = 0;
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var metaPageState;
      var pageState;
      utils.series([
        helper.toTask(insertTestData, null, client, table, 131),
        function selectData(seriesNext) {
          //Only fetch 70
          var counter = 0;
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
          var counter = 0;
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
          var counter = 0;
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var pageState;
      var nextPageRows;
      utils.series([
        client.connect.bind(client),
        helper.toTask(insertTestData, null, client, table, 110),
        function selectData(seriesNext) {
          //Only fetch 60 the first time, 50 the following
          var counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 60}, function (n, row) {
            counter++;
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
          var counter = 0;
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var counter = 0;
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function selectNotExistent(next) {
          var query = util.format('SELECT * FROM %s WHERE id = ?', table);
          var called = 0;
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
          var query = util.format('INSERT INTO %s (id) VALUES (?)', table);
          var called = 0;
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
          var query = util.format('SELECT * FROM %s WHERE id = ?', table);
          var called = 0;
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
      var client = newInstance();
      var loggedMessage = false;
      client.on('log', function (level, className, message) {
        if (loggedMessage) return;
        if (level !== 'warning') return;
        message = message.toLowerCase();
        if (message.indexOf('batch') >= 0 && message.indexOf('exceeding')) {
          loggedMessage = true;
        }
      });
      var query = util.format(
        "BEGIN UNLOGGED BATCH INSERT INTO %s (id, text_sample) VALUES (:id1, :sample)\n" +
        "INSERT INTO %s (id, text_sample) VALUES (:id2, :sample) APPLY BATCH",
        table,
        table
      );
      var params = { id1: types.Uuid.random(), id2: types.Uuid.random(), sample: utils.stringRepeat('c', 2562) };
      client.eachRow(query, params, {prepare: true}, function () {
        
      }, function (err, result) {
        assert.ifError(err);
        assert.ok(result.info.warnings);
        assert.strictEqual(result.info.warnings.length, 1);
        helper.assertContains(result.info.warnings[0], 'batch');
        helper.assertContains(result.info.warnings[0], 'exceeding');
        assert.ok(loggedMessage);
        client.shutdown(done);
      });
    });
    it('should retrieve large result sets in parallel', function (done) {
      var client = newInstance({ queryOptions: {
        consistency: types.consistencies.quorum,
        fetchSize: 20000
      }});
      client.on('log', function (level, className, message, furtherInfo) {
        if (level !== 'warning' && level !== 'error') {
          return;
        }
        console.error(level, className, message, furtherInfo);
      });
      var query = util.format('SELECT * FROM %s LIMIT 2000', table);
      utils.series([
        client.connect.bind(client),
        helper.toTask(insertTestData, null, client, table, 2000),
        function selectData(seriesNext) {
          utils.timesLimit(400, 6, function (n, timesNext) {
            var counter = 0;
            client.eachRow(query, [], { prepare: true }, function (n, row) {
              assert.ok(row);
              counter++;
            }, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, counter);
              timesNext();
            });
          }, seriesNext);
        }
      ], function (err) {
        client.shutdown();
        done(err);
      });
    });
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
      var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
      utils.timesLimit(length, 100, function (n, next) {
        client.eachRow(query, [types.Uuid.random(), n.toString()], {prepare: 1}, helper.noop, next);
      }, seriesNext);
    }
  ], callback);
}
