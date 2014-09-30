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
    it('should throw an exception when contactPoints are not provided', function () {
      assert.throws(function () {
        var client = new Client({});
      });
      assert.throws(function () {
        var client = new Client({contactPoints: []});
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
    it('should select a tokenizer', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        if (err) return done(err);
        helper.assertInstanceOf(client.metadata.tokenizer, require('../../../lib/tokenizer.js').Murmur3Tokenizer);
        done();
      });
    });
    it('should allow multiple parallel calls to connect', function (done) {
      var client = newInstance();
      async.times(100, function (n, next) {
        client.connect(next);
      }, done);
    });
    it('should resolve host names', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {contactPoints: ['localhost']}));
      client.on('log', helper.log);
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.forEach(function (h) {
          assert.notEqual(h.address, 'localhost');
        });
        done();
      });
    });
    it('should use the keyspace provided', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'system'}));
      //on all hosts
      async.times(10, function (n, next) {
        assert.strictEqual(client.keyspace, 'system');
        //A query in the system ks
        client.execute('SELECT * FROM schema_keyspaces', function (err, result) {
          assert.ifError(err);
          assert.ok(result.rows);
          assert.ok(result.rows.length > 0);
          next();
        });
      }, done);
    });
  });
  describe('#connect() with auth', function () {
    before(function (done) {
      async.series([
        function (next) {
          //it wont hurt to remove
          helper.ccmHelper.exec(['remove'], function () {
            //ignore error
            next();
          });
        },
        function (next) {
          helper.ccmHelper.exec(['create', 'test', '-v', helper.getCassandraVersion()], next);
        },
        function (next) {
          helper.ccmHelper.exec(['updateconf', "authenticator: PasswordAuthenticator"], next);
        },
        function (next) {
          helper.ccmHelper.exec(['populate', '-n', '1'], next);
        },
        function (next) {
          helper.ccmHelper.exec(['start'], function () {
            //It takes a while for Cassandra to create the default user account
            setTimeout(function () {next();}, 25000);
          });
        }
      ], done)
    });
    after(helper.ccmHelper.remove);
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
    it('should return an AuthenticationError', function (done) {
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
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should fail to execute if the keyspace does not exists', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'NOT____EXISTS'}));
      //on all hosts
      async.times(10, function (n, next) {
        //No matter what, the keyspace does not exists
        client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
          helper.assertInstanceOf(err, Error);
          next();
        });
      }, done);
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
    it('should return ResponseError when executing USE with a wrong keyspace', function (done) {
      var client = newInstance();
      var count = 0;
      client.execute('USE ks_not_exist', function (err, result) {
        assert.ok(err instanceof errors.ResponseError);
        assert.equal(client.keyspace, null);
        done();
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
        assert.strictEqual(client.hosts.length, 3);
        var hosts = client.hosts.slice(0);
        assert.strictEqual(hosts[0].pool.coreConnectionsLength, 3);
        assert.strictEqual(hosts[1].pool.coreConnectionsLength, 3);
        assert.strictEqual(hosts[0].pool.connections.length, 3);
        assert.strictEqual(hosts[1].pool.connections.length, 3);
        done(err);
      });
    });
  });
  describe('failover', function () {
    beforeEach(helper.ccmHelper.start(3));
    afterEach(helper.ccmHelper.remove);
    it('should failover after a node goes down', function (done) {
      var client = newInstance();
      var hosts = {};
      async.series([
        function warmUpPool(seriesNext) {
          async.times(100, function (n, next) {
            client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
              assert.ifError(err);
              hosts[result._queriedHost] = true;
              next();
            });
          }, seriesNext);
        },
        function killNode(seriesNext) {
          setTimeout(function () {
            helper.ccmHelper.exec(['node1', 'stop', '--not-gently']);
            seriesNext();
          }, 0);
        },
        function testCase(seriesNext) {
          //3 hosts alive
          assert.strictEqual(Object.keys(hosts).length, 3);
          var counter = 0;
          async.times(1000, function (i, next) {
            client.execute('SELECT * FROM system.schema_keyspaces', function (err) {
              counter++;
              assert.ifError(err);
              next();
            });
          }, function (err) {
            assert.ifError(err);
            //Only 2 hosts alive at the end
            assert.strictEqual(
              client.hosts.slice(0).reduce(function (val, h) {
                return val + (h.isUp() ? 1 : 0);
              }, 0),
              2);
            seriesNext();
          });
        }
      ], done);
    });
    it('should failover when a node goes down with some outstanding requests', function (done) {
      var options = utils.extend({}, helper.baseOptions);
      options.pooling = {
        coreConnectionsPerHost: {
          '0': 1,
          '1': 1,
          '2': 0
        }
      };
      var client = new Client(options);
      var hosts = {};
      async.series([
        function warmUpPool(seriesNext) {
          async.times(10, function (n, next) {
            client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
              assert.ifError(err);
              hosts[result._queriedHost] = true;
              next();
            });
          }, seriesNext);
        },
        function testCase(seriesNext) {
          //3 hosts alive
          assert.strictEqual(Object.keys(hosts).length, 3);
          var counter = 0;
          var issued = 0;
          var killed = false;
          async.times(500, function (n, next) {
            //console.log('--starting', n);
            if (n === 10) {
              //kill a node when there are some outstanding requests
              helper.ccmHelper.exec(['node2', 'stop', '--not-gently'], function (err) {
                killed = true;
                assert.ifError(err);
                next();
              });
              return;
            }
            if (killed) {
              //Don't issue more requests
              return next();
            }
            issued++;
            client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
              assert.ifError(err);
              counter++;
              //console.log('issued vs counter', issued, counter);
              next();
            });
          }, function (err) {
            assert.ifError(err);
            //Only 2 hosts alive at the end
            assert.strictEqual(
              client.hosts.slice(0).reduce(function (val, h) {
                return val + (h.isUp() ? 1 : 0);
              }, 0),
              2);
            seriesNext();
          });
        }
      ], done);
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