var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(120000);
  describe('constructor', function () {
    it('should throw an exception when contactPoints provided', function () {
      assert.throws(function () {
        var client = new Client({});
      });
      assert.throws(function () {
        var client = new Client(null);
      });
      assert.throws(function () {
        var client = new Client();
      });
    });
  });
  describe('#connect()', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should discover all hosts in the ring', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        if (err) return done(err);
        assert.strictEqual(client.hosts.length, 3);
        done();
      });
    });
    it('should allow multiple parallel calls to connect', function (done) {
      var client = newInstance();
      async.times(100, function (n, next) {
        client.connect(next);
      }, done);
    });
  });
  describe('#execute()', function () {
    before(helper.ccmHelper.start(2));
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
      client.execute('SELECT WILL FAIL', function (err, result) {
        assert.notEqual(err, null);
        assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
        assert.equal(result, null);
        done();
      });
    });
    it('should handle 500 parallel queries', function (done) {
      var client = newInstance();
      async.times(500, function (n, next) {
        client.execute('SELECT * FROM system.schema_keyspaces', [], next);
      }, done)
    });
    it('should change the active keyspace after USE statement', function (done) {
      var client = newInstance();
      client.execute('USE system', function (err, result) {
        if (err) return done(err);
        assert.strictEqual(client.keyspace, 'system');
        //all next queries, the instance should still "be" in the system keyspace
        async.times(100, function (n, next) {
          client.execute('SELECT * FROM schema_keyspaces', [], next);
        }, done)
      });
    });
    it('should create the amount of connections determined by the options', function (done) {
      var options = {
        pooling: {
          coreConnectionsPerHost: {
            '0': 3,
            '1': 0,
            '2': 0
          }
        }
      };
      var client = new Client(utils.extend({}, helper.baseOptions, options));
      //execute a couple of queries
      async.times(100, function (n, next) {
        setTimeout(function () {
          client.execute('SELECT * FROM system.schema_keyspaces', next);
          }, 100 + n * 2)
      }, function (err) {
        if (err) return done(err);
        assert.strictEqual(client.hosts.length, 2);
        var hosts = client.hosts.slice(0);
        assert.strictEqual(hosts[0].pool.coreConnectionsLength, 3);
        assert.strictEqual(hosts[1].pool.coreConnectionsLength, 3);
        assert.strictEqual(hosts[0].pool.connections.length, 3);
        assert.strictEqual(hosts[1].pool.connections.length, 3);
        done(err);
      });
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}