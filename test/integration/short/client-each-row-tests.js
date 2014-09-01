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
          }, function (err) {
            assert.ifError(err);
            assert.strictEqual(counter, length);
            next();
          });
        }], done);
    });
  });

  describe('#eachRow(query, params, {prepare: 1})', function () {
    before(helper.ccmHelper.start(3));
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
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}