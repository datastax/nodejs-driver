var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');

describe('Client', function () {
  this.timeout(30000);
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
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}