var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 0}, callback)', function () {
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    before(function (done) {
      var client = newInstance();
      async.series([
        helper.ccmHelper.start(1),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table), next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should execute a basic query', function (done) {
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
        assert.equal(err, null);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        done();
      });
    });
    it('should callback with syntax error', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var query = 'SELECT WILL FAIL';
        client.execute(query, function (err, result) {
          assert.ok(err);
          assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
          assert.strictEqual(err.query, query);
          assert.equal(result, null);
          done();
        });
      });
    });
    it('should handle 500 parallel queries', function (done) {
      var client = newInstance();
      async.times(500, function (n, next) {
        client.execute('SELECT * FROM system.schema_keyspaces', [], next);
      }, done)
    });
    it('should guess known types @c2_0', function (done) {
      var client = newInstance();
      var columns = 'id, text_sample, double_sample, timestamp_sample, blob_sample, list_sample';
      //a precision a float32 can represent
      var values = [types.uuid(), 'text sample 1', 133, new Date(121212211), new Buffer(100), ['one', 'two']];
      //no hint
      insertSelectTest(client, table, columns, values, null, done);
    });
    it('should use parameter hints as number for simple types @c2_0', function (done) {
      var client = newInstance();
      var columns = 'id, text_sample, float_sample, int_sample';
      //a precision a float32 can represent
      var values = [types.uuid(), 'text sample', 1000.0999755859375, -12];
      var hints = [types.dataTypes.uuid, types.dataTypes.text, types.dataTypes.float, types.dataTypes.int];
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for simple types @c2_0', function (done) {
      var columns = 'id, text_sample, float_sample, int_sample';
      var values = [types.uuid(), 'text sample', -9, 1];
      var hints = [null, 'text', 'float', 'int'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for complex types partial @c2_0', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.uuid(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      var hints = [null, 'map', 'list', 'set'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for complex types complete @c2_0', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.uuid(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      //complete info
      var hints = [null, 'map<text, text>', 'list<text>', 'set<text>'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use pageState and fetchSize @c2_0', function (done) {
      var client = newInstance();
      var pageState = null;
      async.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          async.times(100, function (n, next) {
            client.execute(query, [types.uuid(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Only fetch 70
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 70}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 70);
            pageState = result.meta.pageState;
            seriesNext();
          });
        },
        function selectDataRemaining(seriesNext) {
          //The remaining
          client.execute(util.format('SELECT * FROM %s', table), [], {pageState: pageState}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 30);
            seriesNext();
          });
        }
      ], done);
    });
    it('should not autoPage @c2_0', function (done) {
      var client = newInstance({keyspace: keyspace});
      var pageState = null;
      async.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          async.times(100, function (n, next) {
            client.execute(query, [types.uuid(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //It should only return the first page
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 65, autoPage: true}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 65);
            pageState = result.meta.pageState;
            seriesNext();
          });
        }
      ], done);
    });
    it('should callback in err when wrong hints are provided @c2_0', function (done) {
      var client = newInstance();
      var query = util.format('SELECT * FROM %s WHERE id IN (?, ?, ?)', table);
      //valid params
      var params = [types.uuid(), types.uuid(), types.uuid()];
      async.series([
        client.connect.bind(client),
        function hintsArrayAsObject(next) {
          client.execute(query, params, {hints: {}}, function (err) {
            //it should not fail
            next(err);
          });
        },
        function hintsDifferentAmount(next) {
          client.execute(query, params, {hints: ['uuid']}, function (err) {
            //it should not fail
            next(err);
          });
        },
        function hintsArrayWrongSubtype(next) {
          client.execute(query, params, {hints: [[]]}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertNotInstanceOf(err, errors.NoHostAvailableError);
            next();
          });
        },
        function hintsInvalidStrings(next) {
          client.execute(query, params, {hints: ['zzz', 'mmmm']}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertNotInstanceOf(err, errors.NoHostAvailableError);
            next();
          });
        }
      ], done);
    });
    it('should encode CONTAINS parameter @c2_1', function (done) {
      var client = newInstance();
      client.execute(util.format('CREATE INDEX list_sample_index ON %s(list_sample)', table), function (err) {
        assert.ifError(err);
        var query = util.format('SELECT * FROM %s WHERE list_sample CONTAINS ? AND list_sample CONTAINS ? ALLOW FILTERING', table);
        //valid params
        var params = ['val1', 'val2'];
        client.execute(query, params, function (err) {
          //it should not fail
          assert.ifError(err);
          done();
        });
      });
    });
    it('should handle several concurrent executes while the pool is not ready', function (done) {
      var client = newInstance({pooling: {
        coreConnectionsPerHost: {
          //lots of connections per host
          '0': 100,
          '1': 1,
          '2': 0
        }}});
      var execute = function (next) {
        client.execute('SELECT * FROM ' + table, next);
      };
      async.parallel([
        function (parallelNext) {
          async.parallel(helper.fillArray(400, execute), parallelNext);
        },
        function (parallelNext) {
          async.times(200, function (n, next) {
            setTimeout(function () {
              execute(next);
            }, n * 5 + 50);
          }, parallelNext);
        }
      ], done);
    });
  });
});

function insertSelectTest(client, table, columns, values, hints, done) {
  var columnsSplit = columns.split(',');
  async.series([
    function (next) {
      var markers = '?';
      for (var i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      var query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 0, hints: hints}, next);
    },
    function (next) {
      var query = util.format('SELECT %s FROM %s WHERE id = %s', columns, table, values[0]);
      client.execute(query, null, {prepare: 0}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        var row = result.rows[0];
        assert.strictEqual(row.values().length, values.length);
        assert.strictEqual(row.keys().join(', '), columnsSplit.join(','));
        for (var i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}

/**
 * @param [options]
 * @returns {Client}
 */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
