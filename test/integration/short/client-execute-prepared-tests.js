var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 1}, callback)', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should execute a prepared query with parameters on all hosts', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = ?';
      async.timesSeries(3, function (n, next) {
        client.execute(query, ['system'], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 3);
          assert.notEqual(result, null);
          assert.notEqual(result.rows, null);
          next();
        });
      }, done);
    });
    it('should callback with error when query is invalid', function (done) {
      var client = newInstance();
      var query = 'SELECT WILL FAIL';
      client.execute(query, ['system'], {prepare: 1}, function (err, result) {
        assert.ok(err);
        assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
        assert.strictEqual(err.query, query);
        done();
      });
    });
    it('should prepare and execute a query without parameters', function (done) {
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_columnfamilies', null, {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows.length);
        done();
      });
    });
    it('should prepare and execute a queries in parallel', function (done) {
      var client = newInstance();
      var queries = [
        'SELECT * FROM system.schema_columnfamilies',
        'SELECT * FROM system.schema_keyspaces',
        'SELECT * FROM system.schema_keyspaces where keyspace_name = ?',
        'SELECT * FROM system.schema_columnfamilies where keyspace_name IN (?, ?)'
      ];
      var params = [
        null,
        null,
        ['system'],
        ['system', 'other']
      ];
      async.times(100, function (n, next) {
        var index = n % 4;
        client.execute(queries[index], params[index], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.rows.length);
          next();
        });
      }, done);
    });
    it('should serialize all guessed types', function (done) {
      var values = [types.uuid(), 'as', '111', null, new types.Long(0x1001, 0x0109AA), 1, new Buffer([1, 240]), true, new Date(1221111111), null, null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';

      serializationTest(values, columnNames, done);
    });
    it('should serialize all null values', function (done) {
      var values = [types.uuid(), null, null, null, null, null, null, null, null, null, null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';
      serializationTest(values, columnNames, done);
    });
    it('should use prepared metadata to determine the type of params in query', function (done) {
      var values = [types.uuid(), [1, 1000, 0], {k: '1'}, 1, -100019, ['arr'], new Buffer([192, 168, 1, 200])];
      var columnNames = 'id, list_sample2, map_sample, int_sample, float_sample, set_sample, inet_sample';
      serializationTest(values, columnNames, done);
    });
    it('should support IN clause with 1 marker @c2_0', function (done) {
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_keyspaces WHERE keyspace_name IN ?', [['system', 'another']], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows.length);
        done();
      });
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  //var logEmitter = function (name, type) { if (type === 'verbose1') { return; } console.log.apply(console, arguments);};
  var logEmitter = function () {};
  var options = utils.extend({logEmitter: logEmitter}, helper.baseOptions);
  return new Client(options);
}

function serializationTest(values, columns, done) {
  var client = newInstance();
  var keyspace = helper.getRandomName('ks');
  var table = keyspace + '.' + helper.getRandomName('table');
  async.series([
    function (next) {
      client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
    },
    function (next) {
      client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
    },
    function (next) {
      var markers = '?';
      var columnsSplit = columns.split(',');
      for (var i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      var query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 1}, next);
    },
    function (next) {
      var query = util.format('SELECT %s FROM %s WHERE id = ?', columns, table);
      client.execute(query, [values[0]], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        var row = result.rows[0];
        for (var i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}