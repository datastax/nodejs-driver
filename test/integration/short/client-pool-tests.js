var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');

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
  describe.skip('#connect() with auth', function () {
    //launch manual C* instance
    var PlainTextAuthProvider = require('../../../lib/auth/plain-text-auth-provider.js');
    it('should connect using the plain text authenticator', function (done) {
      var options = utils.extend({}, helper.baseOptions, {authProvider: new PlainTextAuthProvider('cassandra', 'cassandra')});
      var client = new Client(options);
      async.times(100, function (n, next) {
        client.connect(next);
      }, function (err) {
        done(err);
      });
    });
    it('should connect using return an AuthenticationError', function (done) {
      var options = utils.extend({}, helper.baseOptions, {authProvider: new PlainTextAuthProvider('not___EXISTS', 'not___EXISTS')});
      var client = new Client(options);
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        helper.assertInstanceOf(err.innerErrors, Array);
        helper.assertInstanceOf(err.innerErrors[0], errors.AuthenticationError);
        done();
      });
    });
  });
  describe('#execute()', function () {
    before(helper.ccmHelper.start(2));
    after(helper.ccmHelper.remove);
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
  describe('#shutdown()', function () {
    before(helper.ccmHelper.start(2));
    after(helper.ccmHelper.remove);
    it('should close all connections to all hosts', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        function makeSomeQueries(next) {
          //to ensure that the pool is all up!
          async.times(100, function (n, timesNext) {
            client.execute('SELECT * FROM system.schema_keyspaces', timesNext);
          }, next);
        },
        function shutDown(next) {
          var hosts = client.hosts.slice(0);
          assert.strictEqual(hosts.length, 2);
          assert.ok(hosts[0].pool.connections.length > 0);
          assert.ok(hosts[1].pool.connections.length > 0);
          client.shutdown(next);
        },
        function checkPool(next) {
          var hosts = client.hosts.slice(0);
          assert.strictEqual(hosts.length, 2);
          assert.strictEqual(hosts[0].pool.connections, null);
          assert.strictEqual(hosts[1].pool.connections, null);
          next();
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