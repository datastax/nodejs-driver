"use strict";
var assert = require('assert');
var domain = require('domain');
var dns = require('dns');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var clientOptions = require('../../../lib/client-options');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');
var types = require('../../../lib/types');
var policies = require('../../../lib/policies');
var RoundRobinPolicy = require('../../../lib/policies/load-balancing.js').RoundRobinPolicy;

describe('Client', function () {
  this.timeout(120000);
  describe('#connect()', function () {
    var useLocalhost;
    before(helper.ccmHelper.start(3));
    before(function (done) {
      dns.resolve('localhost', function (err) {
        useLocalhost = !err;
        done();
      });
    });
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
      utils.times(100, function (n, next) {
        client.connect(next);
      }, function (err) {
        assert.ifError(err);
        client.shutdown(done);
      });
    });
    it('should resolve host names', function (done) {
      if (!useLocalhost) {
        return done();
      }
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
      utils.times(10, function (n, next) {
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
        utils.times(10, function (n, next) {
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
        utils.times(50, function (n, next) {
          client.execute('SELECT key FROM system.local', next);
        }, function (err) {
          if (err) {
            return done(err);
          }
          setTimeout(function () {
            //wait until all connections are made in the background
            assert.strictEqual(
              client.hosts.values()[0].pool.connections.length,
              client.options.pooling.coreConnectionsPerHost[types.distance.local]);
            client.shutdown(done);
          }, 5000);
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
      // do it multiple times
      utils.timesSeries(300, function (n, next) {
        var connectionsPerHost = {};
        connectionsPerHost[types.distance.local] = 3;
        connectionsPerHost[types.distance.remote] = 1;
        var client = newInstance({pooling: {warmup: true, coreConnectionsPerHost: connectionsPerHost}});
        client.connect(function (err) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 3);
          client.hosts.forEach(function (host) {
            assert.strictEqual(host.pool.connections.length, 3, 'For host ' + host.address);
          });
          client.shutdown(next);
        });
      }, done);
    });
    it('should only warmup connections for hosts with local distance', function (done) {
      var lbPolicy = new RoundRobinPolicy();
      lbPolicy.getDistance = function (host) {
        var id = helper.lastOctetOf(host.address);
        if(id == '1') {
          return types.distance.local;
        }
        else if(id == '2') {
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
          var id = helper.lastOctetOf(host);
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
    before(helper.ccmHelper.start(helper.isCassandraGreaterThan('2.1') ? 2 : 1, {
      yaml: ['authenticator:PasswordAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0'],
      sleep: 5000
    }));
    after(helper.ccmHelper.remove);
    var PlainTextAuthProvider = require('../../../lib/auth/plain-text-auth-provider.js');
    it('should connect using the plain text authenticator', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('cassandra', 'cassandra')};
      var client = newInstance(options);
      utils.times(100, function (n, next) {
        client.connect(next);
      }, function (err) {
        done(err);
      });
    });
    it('should connect using the plain text authenticator when calling execute', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('cassandra', 'cassandra'), keyspace: 'system'};
      var client = newInstance(options);
      utils.times(100, function (n, next) {
        client.execute('SELECT * FROM local', next);
      }, function (err) {
        done(err);
      });
    });
    it('should return an AuthenticationError', function (done) {
      var options = {authProvider: new PlainTextAuthProvider('not___EXISTS', 'not___EXISTS'), keyspace: 'system'};
      var client = newInstance(options);
      utils.timesSeries(10, function (n, next) {
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
      utils.times(10, function (n, next) {
        client.execute('SELECT * FROM local', function (err) {
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
  describe('#connect() with ipv6', function () {
    before(helper.ccmHelper.start(1, { ipFormat: '::%d' }));
    after(helper.ccmHelper.remove);
    it('should connect to ipv6 host', function (done) {
      utils.series([
        function testWithShortNotation(seriesNext) {
          testConnect('::1', seriesNext);
        },
        function testFullAddress(seriesNext) {
          testConnect('0:0:0:0:0:0:0:1', seriesNext);
        },
        function testFullAddress(seriesNext) {
          testConnect('[0:0:0:0:0:0:0:1]:9042', seriesNext);
        }
      ], done);
      function testConnect(contactPoint, testDone) {
        var client = newInstance({ contactPoints: [ contactPoint ] });
        client.connect(function (err) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 1);
          var expected = contactPoint + ':9042';
          if (contactPoint.indexOf('[') === 0) {
            expected = contactPoint.replace(/[\[\]]/g, '');
          }
          assert.strictEqual(client.hosts.values()[0].address, expected);
          utils.times(10, function (n, next) {
            client.execute(helper.queries.basic, next);
          }, testDone);
        });
      }
    });
  });
  describe('#connect() with nodes failing', function () {
    it('should connect after a failed attempt', function (done) {
      var client = newInstance();
      utils.series([
        helper.ccmHelper.removeIfAny,
        function (next) {
          client.connect(function (err) {
            helper.assertInstanceOf(err, errors.NoHostAvailableError);
            next();
          });
        },
        helper.ccmHelper.start(1),
        function (next) {
          client.connect(function (err) {
            assert.ifError(err);
            var hosts = client.hosts.values();
            assert.strictEqual(1, hosts.length);
            assert.strictEqual(typeof hosts[0].datacenter, 'string');
            assert.notEqual(hosts[0].datacenter.length, 0);
            next();
          });
        },
        client.shutdown.bind(client),
        helper.ccmHelper.remove
      ], done);
    });
    function getReceiveNotificationTest(nodeNumber) {
      return (function receiveNotificationTest(done) {
        // Should receive notification when a node gracefully closes connections
        var client = newInstance({
          pooling: {
            warmup: true,
            heartBeatInterval: 0,
            // Use just 1 connection per host for all protocol versions in this test
            coreConnectionsPerHost: clientOptions.coreConnectionsPerHostV3
          }
        });
        utils.series([
          helper.ccmHelper.removeIfAny,
          helper.ccmHelper.start(2),
          client.connect.bind(client),
          function checkInitialState(next) {
            var hosts = client.hosts.values();
            assert.ok(hosts[0].isUp());
            assert.ok(hosts[1].isUp());
            next();
          },
          function stopNode(next) {
            helper.ccmHelper.stopNode(nodeNumber, next);
          },
          helper.delay(300),
          function checkThatStateChanged(next) {
            // Only 1 node should be UP
            assert.strictEqual(client.hosts.values().filter(function (h) {
              return h.isUp();
            }).length, 1);
            next();
          },
          client.shutdown.bind(client),
          helper.ccmHelper.remove
        ], done);
      });
    }
    it('should receive socket closed event and set node as down', getReceiveNotificationTest(2));
    it('should receive socket closed event and set node as down (control connection node)', getReceiveNotificationTest(1));
  });
  describe('#execute()', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should use the keyspace provided', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {keyspace: 'system'}));
      //on all hosts
      utils.times(10, function (n, next) {
        assert.strictEqual(client.keyspace, 'system');
        //A query in the system ks
        client.execute('SELECT * FROM local', function (err, result) {
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
      utils.times(10, function (n, next) {
        //No matter what, the keyspace does not exists
        client.execute(helper.queries.basic, function (err) {
          helper.assertInstanceOf(err, Error);
          next();
        });
      }, done);
    });
    it('should change the active keyspace after USE statement', function (done) {
      var client = newInstance();
      client.execute('USE system', function (err) {
        if (err) {
          return done(err);
        }
        assert.strictEqual(client.keyspace, 'system');
        // all next queries, the instance should still "be" in the system keyspace
        utils.timesLimit(100, 50, function (n, next) {
          client.execute('SELECT * FROM local', [], next);
        }, helper.finish(client, done));
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
      utils.timesLimit(100, 50, function (n, next) {
        client.execute(helper.queries.basic, next);
      }, function (err) {
        if (err) {
          return done(err);
        }
        assert.strictEqual(client.hosts.length, 3);
        var hosts = client.hosts.slice(0);
        assert.strictEqual(hosts[0].pool.coreConnectionsLength, 3);
        assert.strictEqual(hosts[1].pool.coreConnectionsLength, 3);
        // wait for the pool to be the expected size
        helper.setIntervalUntil(function condition() {
          return (hosts[0].pool.connections.length === 3 && hosts[1].pool.connections.length === 3);
        }, 1000, 20, done);
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
      utils.series([
        client1.connect.bind(client1),
        client2.connect.bind(client1),
        function executeABunchOfTimes1(next) {
          utils.times(10, function (n, timesNext) {
            client1.execute('SELECT * FROM system.local', timesNext);
          }, next);
        },
        function executeABunchOfTimes2(next) {
          utils.times(10, function (n, timesNext) {
            client2.execute('SELECT * FROM system.local', timesNext);
          }, next);
        },
        function blowUpSingleDomain(next) {
          var EventEmitter = require('events').EventEmitter;
          utils.timesSeries(domains.length, function (n, timesNext) {
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
      utils.timesSeries(10, function (n, next) {
        client.execute('SELECT key FROM system.local', next);
      }, function (err) {
        assert.ifError(err);
        utils.eachSeries(queries, function (query, next) {
          client.execute(query, next);
        }, done);
      });
    });
    it('should handle distance changing load balancing policies', changingDistancesTest('2'));
    it('should handle distance changing load balancing policies for control connection host', changingDistancesTest('1'));
    function changingDistancesTest(address) {
      return (function doTest(done) {
        var lbp = new RoundRobinPolicy();
        var ignoredHost;
        var cc;
        lbp.getDistance = function (h) {
          return helper.lastOctetOf(h) === ignoredHost ? types.distance.ignored : types.distance.local;
        };
        var connectionsPerHost = 5;
        var client = newInstance({
          policies: { loadBalancing: lbp },
          pooling: { coreConnectionsPerHost: { '0': connectionsPerHost } }
        });
        function queryAndCheckPool(limit, assertions) {
          return (function executeSomeQueries(next) {
            var coordinators = {};
            utils.timesLimit(limit, 3, function (n, timesNext) {
              client.execute(helper.queries.basic, function (err, result) {
                if (!err) {
                  coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
                }
                timesNext(err);
              });
            }, function (err) {
              assert.ifError(err);
              assertions(Object.keys(coordinators).sort());
              next();
            });
          });
        }
        utils.series([
          client.connect.bind(client),
          queryAndCheckPool(12, function (coordinators) {
            utils.objectValues(getPoolInfo(client)).forEach(function (poolSize) {
              assert.notStrictEqual(poolSize, 0);
            });
            assert.deepEqual(coordinators, [ '1', '2', '3']);
            cc = client.controlConnection.connection;
            // Set as the ignored host for the next queries
            ignoredHost = address;
          }),
          queryAndCheckPool(500, function (coordinators) {
            // The pool for 1st and 3rd host should have the appropriate size by now.
            var expectedPoolInfo = { '1': connectionsPerHost, '2': connectionsPerHost, '3': connectionsPerHost};
            expectedPoolInfo[ignoredHost] = 0;
            assert.deepEqual(getPoolInfo(client), expectedPoolInfo );
            assert.deepEqual(coordinators, [ '1', '2', '3'].filter(function (x) { return x !== ignoredHost}));
          }),
          client.shutdown.bind(client),
          function checkPoolState(next) {
            var expectedState = { '1': 0, '2': 0, '3': 0};
            assert.deepEqual(getPoolInfo(client), expectedState);
            setTimeout(function checkPoolStateDelayed() {
              assert.deepEqual(getPoolInfo(client), expectedState);
              if (ignoredHost === '1') {
                // The control connection should have changed
                assert.ok(!cc.connected);
                assert.notStrictEqual(helper.lastOctetOf(client.controlConnection.host), '1');
              }
              else {
                assert.strictEqual(helper.lastOctetOf(client.controlConnection.host), '1');
              }
              next();
            }, 300);
          }
        ], done);
      });
    }
  });
  describe('failover', function () {
    beforeEach(helper.ccmHelper.start(3));
    afterEach(helper.ccmHelper.remove);
    it('should failover after a node goes down', function (done) {
      var client = newInstance();
      var hosts = {};
      var hostsDown = [];
      utils.series([
        client.connect.bind(client),
        function (next) {
          // wait for all initial events to ensure we don't incidentally get an 'UP' event for node 2
          // after we have killed it.
          setTimeout(next, 5000);
        },
        function warmUpPool(seriesNext) {
          client.on('hostDown', function (h) {
            hostsDown.push(h);
          });
          utils.times(100, function (n, next) {
            client.execute(helper.queries.basic, function (err, result) {
              assert.ifError(err);
              hosts[result.info.queriedHost] = true;
              next();
            });
          }, seriesNext);
        },
        function killNode(seriesNext) {
          assert.strictEqual(Object.keys(hosts).length, 3);
          setImmediate(function () {
            helper.ccmHelper.exec(['node1', 'stop', '--not-gently']);
            seriesNext();
          });
        },
        function executeWhileNodeGoingDown(seriesNext) {
          utils.timesLimit(1000, 10, function (i, next) {
            client.execute(helper.queries.basic, next);
          }, function (err) {
            assert.ifError(err);
            //delay the next queries
            setTimeout(seriesNext, 10000);
          });
        },
        function executeAfterNodeWentDown(seriesNext) {
          utils.timesSeries(20, function (i, next) {
            client.execute(helper.queries.basic, next);
          }, seriesNext);
        },
        function assertions(seriesNext) {
          //Only 2 hosts alive at the end
          assert.strictEqual(
            client.hosts.values().reduce(function (val, h) {
              return val + (h.isUp() ? 1 : 0);
            }, 0),
            2);
          assert.ok(hostsDown.length >= 1, "Expected at least 1 host down" +
            " event.");
          //Ensure each down event is for the stopped host.  We may get
          //multiple down events for the same host on a control connection.
          hostsDown.forEach(function (downHost) {
            assert.strictEqual(helper.lastOctetOf(downHost), '1');
          });
          seriesNext();
        },
        client.shutdown.bind(client)
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
      var query = helper.queries.basic;
      utils.series([
        function (next) {
          // wait for all initial events to ensure we don't incidentally get an 'UP' event for node 2
          // after we have killed it.
          setTimeout(next, 5000);
        },
        function warmUpPool(seriesNext) {
          utils.times(10, function (n, next) {
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
          utils.timesLimit(500, 20, function (n, next) {
            if (n === 10) {
              //kill a node when there are some outstanding requests
              helper.ccmHelper.exec(['node2', 'stop', '--not-gently'], function (err) {
                killed = true;
                assert.ifError(err);
                //do a couple of more queries
                utils.timesSeries(10, function (n, next2) {
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
      utils.series([
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
      utils.series([
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
    it('should reconnect in the background', function (done) {
      var client = newInstance({
        pooling: { heartBeatInterval: 0, warmup: true },
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(1000) }
      });
      utils.series([
        client.connect.bind(client),
        function (next) {
          var hosts = client.hosts.values();
          assert.strictEqual('1', helper.lastOctetOf(client.controlConnection.host));
          assert.strictEqual(3, hosts.length);
          hosts.forEach(function (h) {
            assert.ok(h.pool.connections.length > 0, 'with warmup = true, it should create the core connections');
          });
          next();
        },
        helper.toTask(helper.ccmHelper.stopNode, null, 1),
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        helper.toTask(helper.ccmHelper.stopNode, null, 3),
        function tryToQuery(next) {
          utils.timesSeries(20, function (n, timesNext) {
            client.execute(helper.queries.basic, function (err) {
              //it should fail
              helper.assertInstanceOf(err, errors.NoHostAvailableError);
              if (n % 5 === 0) {
                //wait for failed reconnection attempts to happen
                return setTimeout(timesNext, 1500);
              }
              timesNext();
            });
          }, next);
        },
        function startNode3(next) {
          helper.waitOnHost(function () {
            helper.ccmHelper.startNode(3);
          }, client, 3, 'up', helper.wait(5000, next));
        },
        function assertReconnected(next) {
          assert.strictEqual('3', helper.lastOctetOf(client.controlConnection.host));
          client.hosts.forEach(function (host) {
            assert.strictEqual(host.isUp(), helper.lastOctetOf(host) === '3');
          });
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
      utils.series([
        client.connect.bind(client),
        function makeSomeQueries(next) {
          //to ensure that the pool is all up!
          utils.times(100, function (n, timesNext) {
            client.execute(helper.queries.basic, timesNext);
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
    it('should not leak any connection when connection pool is still growing', function (done) {
      var client = newInstance({ pooling: { coreConnectionsPerHost: { '0': 4 }}});
      utils.series([
        client.connect.bind(client),
        function makeSomeQueries(next) {
          utils.times(10, function (n, timesNext) {
            client.execute(helper.queries.basic, timesNext);
          }, next);
        },
        function shutDown(next) {
          var hosts = client.hosts.values();
          assert.strictEqual(hosts.length, 2);
          assert.ok(hosts[0].pool.connections.length > 0);
          assert.ok(!hosts[0].pool.shuttingDown);
          assert.ok(!hosts[1].pool.shuttingDown);
          client.shutdown(next);
        },
        function checkPoolDelayed(next) {
          function checkNoConnections() {
            assert.deepEqual(getPoolInfo(client), { '1': 0, '2': 0 });
          }
          checkNoConnections();
          // Wait some time and check again to see if there is a new connection created in the background
          setTimeout(function checkNoConnectionsDelayed() {
            checkNoConnections();
            next();
          }, 1000);
        }
      ], done);
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}

/**
 * Returns a dictionary containing the last octet of the address as keys and the pool size as values.
 */
function getPoolInfo(client) {
  var info = {};
  client.hosts.forEach(function (h, address) {
    info[helper.lastOctetOf(address)] = h.pool.connections.length;
  });
  return info;
}
