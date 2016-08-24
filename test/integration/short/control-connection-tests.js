"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client.js');
var ControlConnection = require('../../../lib/control-connection');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var clientOptions = require('../../../lib/client-options');
var policies = require('../../../lib/policies');
var ProfileManager = require('../../../lib/execution-profile').ProfileManager;

describe('ControlConnection', function () {
  this.timeout(120000);
  describe('#init()', function () {
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should retrieve local host and peers', function (done) {
      var cc = newInstance();
      cc.init(function (err) {
        assert.ifError(err);
        assert.strictEqual(cc.hosts.length, 2);
        assert.ok(cc.protocolVersion);
        cc.hosts.forEach(function (h) {
          assert.ok(h.datacenter);
          assert.ok(h.rack);
          assert.ok(h.tokens);
        });
        done();
      });
    });
    it('should subscribe to SCHEMA_CHANGE events and refresh keyspace information', function (done) {
      var cc = newInstance({ refreshSchemaDelay: 100 });
      var otherClient = new Client(helper.baseOptions);
      utils.series([
        cc.init.bind(cc),
        helper.toTask(otherClient.execute, otherClient, "CREATE KEYSPACE sample_change_1 WITH replication = " +
          "{'class': 'SimpleStrategy', 'replication_factor' : 3}"),
        function (next) {
          helper.setIntervalUntil(function () {
            return cc.metadata.keyspaces['sample_change_1'];
          }, 200, 10, next);
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(keyspaceInfo);
          assert.ok(keyspaceInfo.strategy);
          assert.equal(keyspaceInfo.strategyOptions.replication_factor, 3);
          assert.ok(keyspaceInfo.strategy.indexOf('SimpleStrategy') > 0);
          next();
        },
        helper.toTask(otherClient.execute, otherClient, "ALTER KEYSPACE sample_change_1 WITH replication = " +
          "{'class': 'SimpleStrategy', 'replication_factor' : 2}"),
        function (next) {
          helper.setIntervalUntil(function () {
            return cc.metadata.keyspaces['sample_change_1'].strategyOptions.replication_factor == 2;
          }, 200, 10, next);
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(keyspaceInfo);
          assert.equal(keyspaceInfo.strategyOptions.replication_factor, 2);
          next();
        },
        helper.toTask(otherClient.execute, otherClient, "DROP keyspace sample_change_1"),
        function (next) {
          helper.setIntervalUntil(function () {
            return !cc.metadata.keyspaces['sample_change_1'];
          }, 200, 10, next);
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(!keyspaceInfo);
          next();
        },
        function ccShutDown(next) {
          cc.shutdown();
          next();
        },
        otherClient.shutdown.bind(otherClient)
      ], done);
    });
    it('should subscribe to STATUS_CHANGE events', function (done) {
      // Only ignored hosts are marked as DOWN when receiving the event
      // Use an specific load balancing policy, to set the node2 as ignored
      function TestLoadBalancing() {}
      util.inherits(TestLoadBalancing, policies.loadBalancing.RoundRobinPolicy);
      TestLoadBalancing.prototype.getDistance = function (h) {
        return (helper.lastOctetOf(h) === '2' ? types.distance.ignored : types.distance.local);
      };
      var cc = newInstance({ policies: { loadBalancing: new TestLoadBalancing() } });
      utils.series([
        cc.init.bind(cc),
        function (next) {
          //wait for all initial events
          setTimeout(next, 5000);
        },
        function (next) {
          helper.waitOnHost(function () {
            //noinspection JSCheckFunctionSignatures
            helper.ccmHelper.stopNode(2);
          }, cc, 2, 'down', next);
        },
        function (next) {
          var hosts = cc.hosts.slice(0);
          assert.strictEqual(hosts.length, 2);
          var countUp = hosts.reduce(function (value, host) {
            value += host.isUp() ? 1 : 0;
            return value;
          }, 0);
          assert.strictEqual(countUp, 1);
          cc.shutdown();
          next();
        }
      ], done);
    });
    it('should subscribe to TOPOLOGY_CHANGE add events and refresh ring info', function (done) {
      var options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(1000);
      var cc = newInstance(options, 1, 1);
      utils.series([
        cc.init.bind(cc),
        function (next) {
          //add a node
          helper.ccmHelper.bootstrapNode(3, next);
        },
        function (next) {
          //start the node
          helper.ccmHelper.startNode(3, helper.wait(5000, next));
        },
        function (next) {
          // While the host is started, it's not a given that it will have been connected and marked up,
          // wait for that to be the case.
          var host3 = helper.findHost(cc, 3);
          if(!host3.isUp()) {
            helper.waitOnHost(helper.noop, cc, 3, 'up', next);
          } else {
            next();
          }
        },
        function (next) {
          var hosts = cc.hosts.slice(0);
          var countUp = hosts.reduce(function (value, host) {
            value += host.isUp() ? 1 : 0;
            return value;
          }, 0);
          assert.strictEqual(countUp, 3);
          next();
        }
      ], done);
    });
    it('should subscribe to TOPOLOGY_CHANGE remove events and refresh ring info', function (done) {
      var cc = newInstance();
      utils.series([
        cc.init.bind(cc),
        function (next) {
          //decommission node
          helper.ccmHelper.exec(['node2', 'decommission'], helper.wait(5000, next));
        },
        function (next) {
          var hosts = cc.hosts.slice(0);
          assert.strictEqual(hosts.length, 1);
          next();
        }
      ], done);
    });
    it('should reconnect when host used goes down', function (done) {
      var options = clientOptions.extend(
        utils.extend({ pooling: helper.getPoolingOptions(1, 1, 500) }, helper.baseOptions));
      var cc = new ControlConnection(options, new ProfileManager(options));
      var host1;
      var host2;
      var lbp;
      utils.series([
        cc.init.bind(cc),
        function initLbp(next) {
          lbp = cc.options.policies.loadBalancing;
          lbp.init(null, cc.hosts, next);
        },
        function ensureConnected(next) {
          var hosts = cc.hosts.values();
          hosts.forEach(function (h) {
            h.setDistance(lbp.getDistance(h));
          });
          assert.strictEqual(hosts.length, 2);
          host1 = hosts[0];
          host2 = hosts[1];
          // there should be a single connection to the first host
          assert.strictEqual(host1.pool.connections.length, 1);
          assert.strictEqual(host2.pool.connections.length, 0);
          next();
        },
        helper.toTask(helper.ccmHelper.exec, null, ['node1', 'stop']),
        helper.delay(5000),
        function assertions(next) {
          assert.strictEqual(host1.pool.connections.length, 0,
            'Host1 should be DOWN and connections closed (heartbeat enabled)');
          assert.strictEqual(host1.isUp(), false);
          assert.strictEqual(host2.isUp(), true);
          assert.strictEqual(host1.pool.connections.length, 0);
          assert.strictEqual(host2.pool.connections.length, 1);
          next();
        },
        function shutdown(next) {
          cc.shutdown();
          next();
        }
      ], done);
    });
    it('should reconnect when all hosts go down and back up', function (done) {
      var options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(1000);
      var cc = newInstance(options, 1, 1);
      utils.series([
        cc.init.bind(cc),
        function initLbp(next) {
          assert.ok(cc.host);
          assert.strictEqual(helper.lastOctetOf(cc.host), '1');
          cc.options.policies.loadBalancing.init(null, cc.hosts, next);
        },
        function setHostDistance(next) {
          // the control connection host should be local or remote to trigger DOWN events
          var distance = options.policies.loadBalancing.getDistance(cc.host);
          cc.host.setDistance(distance);
          next();
        },
        function stop1(next) {
          helper.ccmHelper.stopNode(1, next);
        },
        function stop2(next) {
          helper.ccmHelper.stopNode(2, helper.wait(5000, next));
        },
        function setDownManually(next) {
          //help in case the event didn't fired by socket disconnection
          cc.hosts.forEach(function (h) {
            if (h.pool.connections.length === 1) {
              h.removeFromPool(h.pool.connections[0]);
            }
          });
          assert.strictEqual(cc.host, null);
          next();
        },
        function restart(next) {
          helper.waitOnHost(function () {
            //noinspection JSCheckFunctionSignatures
            helper.ccmHelper.startNode(2);
          }, cc, 2, 'up', helper.wait(5000, next));
        },
        function checkHostConnected(next) {
          cc.hosts.forEach(function (h) {
            if (helper.lastOctetOf(h) === '1') {
              assert.strictEqual(h.isUp(), false);
            }
            else {
              assert.strictEqual(h.isUp(), true);
            }
          });
          assert.ok(cc.host);
          assert.strictEqual(helper.lastOctetOf(cc.host), '2');
          next();
        }
      ], done);
    });
  });
  describe('#metadata', function () {
    before(helper.ccmHelper.start(3, {vnodes: true}));
    after(helper.ccmHelper.remove);
    it('should contain keyspaces information', function (done) {
      var cc = newInstance();
      cc.init(function () {
        assert.equal(cc.hosts.length, 3);
        assert.ok(cc.metadata);
        assert.strictEqual(cc.hosts.slice(0)[0]['tokens'].length, 256);
        assert.ok(cc.metadata.keyspaces);
        assert.ok(cc.metadata.keyspaces['system']);
        assert.ok(cc.metadata.keyspaces['system'].strategy);
        assert.strictEqual(typeof cc.metadata.keyspaces['system'].tokenToReplica, 'function');
        done();
      });
    });
  });
});

/** @returns {ControlConnection} */
function newInstance(options, localConnections, remoteConnections) {
  options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions, options));
  //disable the heartbeat
  options.pooling.heartBeatInterval = 0;
  options.pooling.coreConnectionsPerHost[types.distance.local] = localConnections || 2;
  options.pooling.coreConnectionsPerHost[types.distance.remote] = remoteConnections || 1;
  return new ControlConnection(options, new ProfileManager(options));
}