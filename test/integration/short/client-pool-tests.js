var assert = require('assert');
var async = require('async');
var domain = require('domain');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');

describe('Client', function () {
  this.timeout(120000);
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
    it('should fail if contact points can not be resolved', function (done) {
      var client = newInstance({contactPoints: ['not-a-host']});
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        done();
      });
    });
    it('should fail if contact points can not be reached', function (done) {
      var client = newInstance({contactPoints: ['1.1.1.1']});
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
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
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.forEach(function (h) {
          assert.notEqual(h.address, 'localhost');
        });
        done();
      });
    });
    it('should fail if the keyspace does not exists', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'not-existent-ks'}));
      async.times(10, function (n, next) {
        client.connect(function (err) {
          assert.ok(err);
          //Not very nice way to check but here it is
          //Does the message contains Keyspace
          assert.ok(err.message.indexOf('Keyspace') > 0);
          next();
        });
      }, done);
    });
    it('should not use contactPoints that are not part of peers', function (done) {
      var contactPoints = helper.baseOptions.contactPoints.slice(0);
      contactPoints.push('host-not-existent-not-peer');
      contactPoints.push('1.1.1.1');
      var client = newInstance({contactPoints: contactPoints});
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        assert.strictEqual(client.hosts.slice(0)[0].address, contactPoints[0]);
        assert.notEqual(client.hosts.slice(0)[1].address, contactPoints[1]);
        assert.notEqual(client.hosts.slice(0)[2].address, contactPoints[2]);
        done();
      });
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
          helper.ccmHelper.exec(['populate', '-n', '2'], next);
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
      var options = {authProvider: new PlainTextAuthProvider('cassandra', 'cassandra')};
      var client = newInstance(options);
      async.times(100, function (n, next) {
        client.connect(next);
      }, function (err) {
        done(err);
      });
    });
    it('should connect using the plain text authenticator when calling execute', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('cassandra', 'cassandra'), keyspace: 'system'};
      var client = newInstance(options);
      async.times(100, function (n, next) {
        client.execute('SELECT * FROM schema_keyspaces', next);
      }, function (err) {
        done(err);
      });
    });
    it('should return an AuthenticationError', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('not___EXISTS', 'not___EXISTS'), keyspace: 'system'};
      var client = newInstance(options);
      async.timesSeries(10, function (n, next) {
        client.connect(function (err) {
          assert.ok(err);
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.ok(err.innerErrors);
          helper.assertInstanceOf(helper.values(err.innerErrors)[0], errors.AuthenticationError);
          next();
        });
      }, done);
    });
    it('should return an AuthenticationError when calling execute', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('not___EXISTS', 'not___EXISTS'), keyspace: 'system'};
      var client = newInstance(options);
      async.times(10, function (n, next) {
        client.execute('SELECT * FROM schema_keyspaces', function (err) {
          assert.ok(err);
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.ok(err.innerErrors);
          helper.assertInstanceOf(helper.values(err.innerErrors)[0], errors.AuthenticationError);
          next();
        });
      }, done);
    });
  });
  describe('#connect() with ssl', function () {
    before(helper.ccmHelper.start(1, {ssl: true}));
    after(helper.ccmHelper.remove);
    it('should connect to a ssl enabled cluster', function (done) {
      var client = newInstance({sslOptions: {}});
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 1);
        done();
      });
    });
  });
  describe('#execute()', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
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
    it('should fail to execute if the keyspace does not exists', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'NOT____EXISTS'}));
      //on all hosts
      async.times(10, function (n, next) {
        //No matter what, the keyspace does not exists
        client.execute('SELECT * FROM system.schema_keyspaces', function (err) {
          helper.assertInstanceOf(err, Error);
          next();
        });
      }, done);
    });
    it('should change the active keyspace after USE statement', function (done) {
      var client = newInstance();
      client.execute('USE system', function (err) {
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
      client.execute('USE ks_not_exist', function (err) {
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
    it('should maintain the domain in the callbacks', function (done) {
      var unexpectedErrors = [];
      var errors = [];
      var domains = [
        domain.create(),
        domain.create(),
        domain.create()
      ];
      var fatherDomain = domain.create();
      var childDomain = domain.create();
      var client = new Client(helper.baseOptions);
      async.series([
        client.connect.bind(client),
        function executeABunchOfTimes(next) {
          async.times(10, function (n, timesNext) {
            client.execute('SELECT * FROM system.local', timesNext);
          }, next);
        },
        function (next) {
          var EventEmitter = require('events').EventEmitter;
          var emitter = new EventEmitter();
          async.timesSeries(domains.length, function (n, timesNext) {
            var waiting = 1;
            var d = domains[n];
            d.add(emitter);
            d.on('error', function (err) {
              errors.push([err.toString(), n.toString()]);
              d.dispose();
            });
            d.run(function() {
              client.execute('SELECT * FROM system.local', [], {prepare: n % 2}, function (err) {
                waiting = 0;
                if (err) {
                  unexpectedErrors.push(err);
                }
                throw new Error('From domain ' + n);
              });
            });
            function wait() {
              if (waiting > 0) {
                waiting++;
                if (waiting > 100) {
                  return timesNext(new Error('Timed out'));
                }
                return setTimeout(wait, 50);
              }
              //Delay to allow throw
              setTimeout(function () {
                timesNext();
              }, 100);
            }
            wait();
          }, next);
        },
        function nestedDomain(next) {
          var waiting = true;
          fatherDomain.on('error', function (err) {
            errors.push([err.toString(), 'father']);
          });
          fatherDomain.run(function () {
            childDomain.on('error', function (err) {
              errors.push([err.toString(), 'child']);
            });
            childDomain.run(function() {
              client.execute('SELECT * FROM system.local', function (err) {
                waiting = false;
                if (err) {
                  unexpectedErrors.push(err);
                }
                throw new Error('From domain child');
              });
            });
          });
          function wait() {
            if (waiting) {
              return setTimeout(wait, 50);
            }
            //Delay to allow throw
            setTimeout(next, 100);
          }
          wait();
        },
        function assertResults(next) {
          assert.strictEqual(unexpectedErrors.length, 0, 'Unexpected errors: ' + unexpectedErrors[0]);
          assert.strictEqual(errors.length, domains.length + 1);
          errors.forEach(function (item) {
            assert.strictEqual(item[0], 'Error: From domain ' + item[1]);
          });
          next();
        }
      ], done);
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
              hosts[result.info.queriedHost] = true;
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
      var query = 'SELECT * FROM system.schema_keyspaces';
      async.series([
        function warmUpPool(seriesNext) {
          async.times(10, function (n, next) {
            client.execute(query, function (err, result) {
              assert.ifError(err);
              hosts[result.info.queriedHost] = true;
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
            if (n === 10) {
              //kill a node when there are some outstanding requests
              helper.ccmHelper.exec(['node2', 'stop', '--not-gently'], function (err) {
                killed = true;
                assert.ifError(err);
                //do a couple of more queries
                async.times(10, function (n, next2) {
                  client.execute(query, next2);
                }, next);
              });
              return;
            }
            if (killed) {
              //Don't issue more requests
              return next();
            }
            issued++;
            client.execute(query, function (err) {
              assert.ifError(err);
              counter++;
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
          assert.ok(!hosts[0].pool.shuttingDown);
          assert.ok(!hosts[1].pool.shuttingDown);
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

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
