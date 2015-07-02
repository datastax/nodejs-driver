var assert = require('assert');
var async = require('async');
var domain = require('domain');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var Host = require('../../../lib/host').Host;
var clientOptions = require('../../../lib/client-options');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');
var types = require('../../../lib/types');

var RoundRobinPolicy = require('../../../lib/policies/load-balancing.js').RoundRobinPolicy;

describe('Client', function () {
  this.timeout(120000);
  describe('#connect()', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should discover all hosts in the ring and hosts object can be serializable', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        if (err) return done(err);
        assert.strictEqual(client.hosts.length, 3);
        assert.strictEqual(client.hosts.values().length, 3);
        assert.strictEqual(client.hosts.keys().length, 3);
        assert.doesNotThrow(function () {
          //It should be serializable
          JSON.stringify(client.hosts);
        });
        client.shutdown(done);
      });
    });
    it('should retrieve the cassandra version of the hosts', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        if (err) return done(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.values().forEach(function (h) {
          assert.strictEqual(typeof h.cassandraVersion, 'string');
          assert.strictEqual(
            h.cassandraVersion.split('.').slice(0, 2).join('.'),
            helper.getCassandraVersion().split('.').slice(0, 2).join('.'));
        });
        client.shutdown(done);
      });
    });
    it('should fail if the contact points can not be resolved', function (done) {
      var client = newInstance({contactPoints: ['not-a-host']});
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        client.shutdown(function (err) {
          assert.ifError(err);
          done();
        });
      });
    });
    it('should fail if the contact points can not be reached', function (done) {
      var client = newInstance({contactPoints: ['1.1.1.1']});
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        done();
      });
    });
    it('should fail if the keyspace does not exists', function (done) {
      var client = newInstance({ keyspace: 'ks_does_not_exists'});
      client.connect(function (err) {
        helper.assertInstanceOf(err, Error);
        client.shutdown(function (err) {
          assert.ifError(err);
          done();
        });
      });
    });
    it('should select a tokenizer', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        if (err) return done(err);
        helper.assertInstanceOf(client.metadata.tokenizer, require('../../../lib/tokenizer.js').Murmur3Tokenizer);
        client.shutdown(done);
      });
    });
    it('should allow multiple parallel calls to connect', function (done) {
      var client = newInstance();
      async.times(100, function (n, next) {
        client.connect(next);
      }, function (err) {
        assert.ifError(err);
        client.shutdown(done);
      });
    });
    it('should resolve host names', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {contactPoints: ['localhost']}));
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.forEach(function (h) {
          assert.notEqual(h.address, 'localhost');
        });
        client.shutdown(done);
      });
    });
    it('should fail if the keyspace does not exists', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'not-existent-ks'}));
      async.times(10, function (n, next) {
        client.connect(function (err) {
          assert.ok(err);
          //Not very nice way to check but here it is
          //Does the message contains Keyspace
          assert.ok(err.message.toLowerCase().indexOf('keyspace') >= 0, 'Message mismatch, was: ' + err.message);
          next();
        });
      }, function (err) {
        assert.ifError(err);
        client.shutdown(done);
      });
    });
    it('should not use contactPoints that are not part of peers', function (done) {
      var contactPoints = helper.baseOptions.contactPoints.slice(0);
      contactPoints.push('host-not-existent-not-peer');
      contactPoints.push('1.1.1.1');
      var client = newInstance({contactPoints: contactPoints});
      client.connect(function (err) {
        assert.ifError(err);
        //the 3 original hosts
        assert.strictEqual(client.hosts.length, 3);
        var hosts = client.hosts.keys();
        assert.strictEqual(hosts[0], contactPoints[0] + ':9042');
        assert.notEqual(hosts[1], contactPoints[1] + ':9042');
        assert.notEqual(hosts[2], contactPoints[1] + ':9042');
        assert.notEqual(hosts[1], contactPoints[2] + ':9042');
        assert.notEqual(hosts[2], contactPoints[2] + ':9042');
        client.shutdown(done);
      });
    });
    it('should use the default pooling options according to the protocol version', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        assert.ok(client.options.pooling.coreConnectionsPerHost);
        if (client.controlConnection.protocolVersion < 3) {
          helper.assertValueEqual(client.options.pooling.coreConnectionsPerHost, clientOptions.coreConnectionsPerHostV2);
        }
        else {
          helper.assertValueEqual(client.options.pooling.coreConnectionsPerHost, clientOptions.coreConnectionsPerHostV3);
        }
        async.times(10, function (n, next) {
          client.execute('SELECT key FROM system.local', next);
        }, function (err) {
          if (err) return done(err);
          assert.strictEqual(client.hosts.values()[0].pool.connections.length, client.options.pooling.coreConnectionsPerHost[types.distance.local]);
          client.shutdown(done);
        });
      });
    });
    it('should override default pooling options when specified', function (done) {
      var client = newInstance({ pooling: {
        coreConnectionsPerHost: { '0': 4 }
      }});
      client.connect(function (err) {
        assert.ifError(err);
        assert.ok(client.options.pooling.coreConnectionsPerHost);
        var defaults = clientOptions.coreConnectionsPerHostV3;
        if (client.controlConnection.protocolVersion < 3) {
          defaults = clientOptions.coreConnectionsPerHostV2;
        }
        assert.ok(client.options.pooling.coreConnectionsPerHost[types.distance.local], 4);
        assert.ok(client.options.pooling.coreConnectionsPerHost[types.distance.remote], defaults[types.distance.remote]);
        async.times(50, function (n, next) {
          client.execute('SELECT key FROM system.local', next);
        }, function (err) {
          if (err) return done(err);
          assert.strictEqual(client.hosts.values()[0].pool.connections.length, client.options.pooling.coreConnectionsPerHost[types.distance.local]);
          client.shutdown(done);
        });
      });
    });
    it('should not fail when switching keyspace and a contact point is not valid', function (done) {
      var client = new Client({
        contactPoints: ['1.1.1.1', helper.baseOptions.contactPoints[0]],
        keyspace: 'system'
      });
      client.connect(function (err) {
        assert.ifError(err);
        client.shutdown(done);
      });
    });
    it('should open connections to all hosts when warmup is set', function (done) {
      var connectionsPerHost = {};
      connectionsPerHost[types.distance.local]  = 3;
      connectionsPerHost[types.distance.remote] = 1;
      var client = newInstance({ pooling: { warmup: true, coreConnectionsPerHost: connectionsPerHost}});
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.forEach(function (host) {
          assert.strictEqual(host.pool.connections.length, 3);
        });
        client.shutdown(done);
      });
    });
    it('should only warmup connections for hosts with local distance', function (done) {
      var lbPolicy = new RoundRobinPolicy();
      lbPolicy.getDistance = function (host) {
        //noinspection JSCheckFunctionSignatures
        var id = helper.lastOctetOf(host.address);
        if(id == '1') {
          return types.distance.local;
        } else if(id == '2') {
          return types.distance.remote;
        }
        return types.distance.ignored;
      };

      var connectionsPerHost = {};
      connectionsPerHost[types.distance.local]  = 3;
      connectionsPerHost[types.distance.remote] = 1;
      var client = newInstance({
        policies: { loadBalancing: lbPolicy },
        pooling: { warmup: true, coreConnectionsPerHost: connectionsPerHost}
      });
      client.connect(function (err) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 3);
        client.hosts.forEach(function (host) {
          var id = helper.lastOctetOf(host.address);
          if(id == '1') {
            assert.strictEqual(host.pool.connections.length, 3);
          } else {
            assert.strictEqual(host.pool.connections.length, 0);
          }
        });
        client.shutdown(done);
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
        //2 domains because there are more than 2 hosts as an uncaught error
        //will blow up the host pool, by design
        //But we need to test prepared and unprepared
        domain.create(),
        domain.create()
      ];
      var fatherDomain = domain.create();
      var childDomain = domain.create();
      var client1 = new Client(helper.baseOptions);
      var client2 = new Client(helper.baseOptions);
      async.series([
        client1.connect.bind(client1),
        client2.connect.bind(client1),
        function executeABunchOfTimes1(next) {
          async.times(10, function (n, timesNext) {
            client1.execute('SELECT * FROM system.local', timesNext);
          }, next);
        },
        function executeABunchOfTimes2(next) {
          async.times(10, function (n, timesNext) {
            client2.execute('SELECT * FROM system.local', timesNext);
          }, next);
        },
        function blowUpSingleDomain(next) {
          var EventEmitter = require('events').EventEmitter;
          async.timesSeries(domains.length, function (n, timesNext) {
            var waiting = 1;
            var d = domains[n];
            d.add(new EventEmitter());
            d.on('error', function (err) {
              errors.push([err.toString(), n.toString()]);
              setImmediate(function () {
                //OK, this line might result in an output message (!?)
                d.dispose();
              });
            });
            d.run(function() {
              client1.execute('SELECT * FROM system.local', [], {prepare: n % 2}, function (err) {
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
              client2.execute('SELECT * FROM system.local', function (err) {
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
          //assert.strictEqual(errors.length, domains.length + 1);
          errors.forEach(function (item) {
            assert.strictEqual(item[0], 'Error: From domain ' + item[1]);
          });
          next();
        }
      ], done);
    });
    it('should wait for schema agreement before calling back', function (done) {
      var queries = [
        "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
        "CREATE TABLE ks1.tbl1 (id uuid PRIMARY KEY, value text)",
        "SELECT * FROM ks1.tbl1",
        "SELECT * FROM ks1.tbl1 where id = d54cb06d-d168-45a0-b1b2-9f5c75435d3d",
        "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
        "CREATE TABLE ks2.tbl2 (id uuid PRIMARY KEY, value text)",
        "SELECT * FROM ks2.tbl2",
        "SELECT * FROM ks2.tbl2",
        "CREATE TABLE ks2.tbl3 (id uuid PRIMARY KEY, value text)",
        "SELECT * FROM ks2.tbl3",
        "SELECT * FROM ks2.tbl3",
        "CREATE TABLE ks2.tbl4 (id uuid PRIMARY KEY, value text)",
        "SELECT * FROM ks2.tbl4",
        "SELECT * FROM ks2.tbl4",
        "SELECT * FROM ks2.tbl4"
      ];
      var client = newInstance();
      //warmup first
      async.timesSeries(10, function (n, next) {
        client.execute('SELECT key FROM system.local', next);
      }, function (err) {
        assert.ifError(err);
        async.eachSeries(queries, function (query, next) {
          client.execute(query, next);
        }, done);
      });
    });
  });
  describe('failover', function () {
    beforeEach(helper.ccmHelper.start(3));
    afterEach(helper.ccmHelper.remove);
    it('should failover after a node goes down', function (done) {
      var client = newInstance();
      var hosts = {};
      var hostsDown = [];
      client.on('hostDown', function (h) {
        hostsDown.push(h);
      });
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
          helper.timesLimit(1000, 100, function (i, next) {
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
            assert.strictEqual(hostsDown.length, 1);
            assert.strictEqual(helper.lastOctetOf(hostsDown[0]), '1');
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
    it('should warn but not fail when warmup is enable and a node is down', function (done) {
      async.series([
        helper.toTask(helper.ccmHelper.exec, null, ['node2', 'stop']),
        function (next) {
          var warnings = [];
          var client = newInstance({ pooling: { warmup: true } });
          client.on('log', function (level, className, message) {
            if (level !== 'warning' || className !== 'Client') return;
            warnings.push(message);
          });
          client.connect(function (err) {
            assert.ifError(err);
            assert.strictEqual(warnings.length, 1);
            assert.ok(warnings[0].indexOf('pool') >= 0, 'warning does not contains the word pool: ' + warnings[0]);
            client.shutdown(next);
          });
        }
      ], done);
    });
    it('should connect when first contact point is down', function (done) {
      async.series([
        helper.toTask(helper.ccmHelper.exec, null, ['node1', 'stop']),
        function (next) {
          var client = newInstance({ contactPoints: ['127.0.0.1', '127.0.0.2'], pooling: { warmup: true } });
          client.connect(function (err) {
            assert.ifError(err);
            client.shutdown(next);
          });
        }
      ], done);
    });
  });
  describe('events', function () {
    //noinspection JSPotentiallyInvalidUsageOfThis
    this.timeout(600000);
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should emit hostUp hostDown', function (done) {
      var client = newInstance();
      var hostsWentUp = [];
      var hostsWentDown = [];
      async.series([
        client.connect.bind(client),
        function addListeners(next) {
          client.on('hostUp', hostsWentUp.push.bind(hostsWentUp));
          client.on('hostDown', hostsWentDown.push.bind(hostsWentDown));
          next();
        },
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        helper.toTask(helper.ccmHelper.startNode, null, 2),
        function checkResults(next) {
          assert.strictEqual(hostsWentUp.length, 1);
          assert.strictEqual(hostsWentDown.length, 1);
          helper.assertInstanceOf(hostsWentUp[0], Host);
          helper.assertInstanceOf(hostsWentDown[0], Host);
          assert.strictEqual(helper.lastOctetOf(hostsWentUp[0]),   '2');
          assert.strictEqual(helper.lastOctetOf(hostsWentDown[0]), '2');
          next();
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should emit hostAdd hostRemove', function (done) {
      var client = newInstance();
      var hostsAdded = [];
      var hostsRemoved = [];
      function trace(message) {
        return (function (next) {
          helper.trace(message);
          next();
        });
      }
      async.series([
        client.connect.bind(client),
        function addListeners(next) {
          client.on('hostAdd', hostsAdded.push.bind(hostsAdded));
          client.on('hostRemove', hostsRemoved.push.bind(hostsRemoved));
          next();
        },
        trace('Bootstrapping node 3'),
        helper.toTask(helper.ccmHelper.bootstrapNode, null, 3),
        trace('Starting newly bootstrapped node 3'),
        helper.toTask(helper.ccmHelper.startNode, null, 3),
        trace('Decommissioning node 2'),
        helper.toTask(helper.ccmHelper.decommissionNode, null, 2),
        trace('Stopping node 2'),
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        function checkResults(next) {
          helper.trace('Checking results');
          assert.strictEqual(hostsAdded.length, 1);
          assert.strictEqual(hostsRemoved.length, 1);
          helper.assertInstanceOf(hostsAdded[0], Host);
          helper.assertInstanceOf(hostsRemoved[0], Host);
          assert.strictEqual(helper.lastOctetOf(hostsAdded[0]), '3');
          assert.strictEqual(helper.lastOctetOf(hostsRemoved[0]), '2');
          next();
        },
        client.shutdown.bind(client)
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
          assert.strictEqual(hosts[0].pool.connections.length, 0);
          assert.strictEqual(hosts[1].pool.connections.length, 0);
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
