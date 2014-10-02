var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#eachRow(query, params, {prepare: 0})', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should callback per row and the end callback', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = \'system\'';
      var counter = 0;
      //fail if its preparing
      client._getPrepared = function () {throw new Error('Prepared should not be called')};
      client.eachRow(query, [], {prepare: false}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        assert.ok(row.keyspace_name, 'system');
        counter++;
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(counter, 1);
        done();
      });
    });
    it('should allow calls without end callback', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = \'system\'';
      var counter = 0;
      client.eachRow(query, [], {}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        assert.ok(row.keyspace_name, 'system');
        done();
      });
    });
    it('should end callback when no rows', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = \'' + helper.getRandomName() + '\'';
      var counter = 0;
      client.eachRow(query, [], {}, function (n, row) {
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
      client.eachRow(query, [], {}, function (n, row) {
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
      async.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          var query = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'text%s\')';
          async.timesSeries(length, function (n, timesNext) {
            client.eachRow(util.format(query, table, types.uuid(), n), [], noop, timesNext);
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

    it('should autoPage @c2_0', function (done) {
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var client = newInstance();
      var noop = function () {};
      async.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], noop, helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          async.times(100, function (n, next) {
            client.eachRow(query, [types.uuid(), n.toString()], noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          //It should fetch 3 times, a total of 100 rows (45+45+10)
          var query = util.format('SELECT * FROM %s', table);
          var rowCount = 0;
          client.eachRow(query, [], {fetchSize: 45, autoPage: true}, function (n, row) {
            rowCount++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(rowCount, 100);
            assert.strictEqual(rowCount, result.rowLength);
            assert.ok(result.rowLengthArray);
            assert.strictEqual(result.rowLengthArray[0], 45);
            assert.strictEqual(result.rowLengthArray[1], 45);
            assert.strictEqual(result.rowLengthArray[2], 10);
            seriesNext();
          });
        },
        function selectDataOnePage(seriesNext) {
          //It should fetch 1 time, a total of 100 rows (even if asked more)
          var query = util.format('SELECT * FROM %s', table);
          var rowCount = 0;
          client.eachRow(query, [], {fetchSize: 2000, autoPage: true}, function (n, row) {
            rowCount++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(rowCount, 100);
            assert.strictEqual(rowCount, result.rowLength);
            assert.ok(result.rowLengthArray);
            assert.strictEqual(result.rowLengthArray.length, 1);
            assert.strictEqual(result.rowLengthArray[0], 100);
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
      async.series([
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
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = \'system\'';
      var counter = 0;
      var originalGetPrepared = client._getPrepared;
      var prepareCalled = false;
      client._getPrepared = function () {
        prepareCalled = true;
        originalGetPrepared.apply(client, arguments);
      };
      client.eachRow(query, [], {prepare: true}, function (n, row) {
        assert.strictEqual(n, 0);
        assert.ok(row instanceof types.Row, null);
        assert.ok(row.keyspace_name, 'system');
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
      async.series([
        function createKs(next) {
          client.eachRow(helper.createKeyspaceCql(keyspace, 3), [], {prepare: true}, noop, helper.waitSchema(client, next));
        },
        function createTable(next) {
          client.eachRow(helper.createTableCql(table), [], {prepare: true}, noop, helper.waitSchema(client, next));
        },
        function insert(next) {
          var query = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'text%s\')';
          async.timesSeries(length, function (n, timesNext) {
            client.eachRow(util.format(query, table, types.uuid(), n), [], {prepare: true}, noop, timesNext);
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
    it('should autoPage on parallel different tables @c2_0', function (done) {
      var keyspace = helper.getRandomName('ks');
      var table1 = keyspace + '.' + helper.getRandomName('table');
      var table2 = keyspace + '.' + helper.getRandomName('table');
      var client = newInstance();
      //client.on('log', helper.log());
      var noop = function () {};
      async.series([
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
          async.times(200, function (n, next) {
            client.eachRow(query, [types.uuid(), n.toString()], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table2);
          async.times(135, function (n, next) {
            client.eachRow(query, [types.uuid(), n+1], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function selectDataMultiplePages(seriesNext) {
          async.parallel([
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
              client.eachRow(query, [], {fetchSize: 23, autoPage: true, prepare: true}, function (n, row) {
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
      function validateResult(err, result, rowCount, expectedLength, fetchSize){
        assert.ifError(err);
        assert.strictEqual(rowCount, expectedLength);
        assert.strictEqual(rowCount, result.rowLength);
        assert.ok(result.rowLengthArray);
        assert.strictEqual(result.rowLengthArray[0], fetchSize);
        assert.strictEqual(result.rowLengthArray[1], fetchSize);
      }
    });
    it('should use pageState and fetchSize @c2_0 @debug', function (done) {
      var client = newInstance();
      var pageState = null;
      async.series([
        function truncate(seriesNext) {
          client.eachRow('TRUNCATE ' + table, [], noop, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          async.times(131, function (n, next) {
            client.eachRow(query, [types.uuid(), n.toString()], {prepare: 1}, noop, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Only fetch 70
          var counter = 0;
          client.eachRow(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 70}, function (n, row) {
            counter++;
          }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(counter, 70);
            assert.strictEqual(result.rowLength, counter);
            pageState = result.meta.pageState;
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
        }
      ], done);
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}