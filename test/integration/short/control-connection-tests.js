/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const Client = require('../../../lib/client.js');
const ControlConnection = require('../../../lib/control-connection');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const clientOptions = require('../../../lib/client-options');
const policies = require('../../../lib/policies');
const ProfileManager = require('../../../lib/execution-profile').ProfileManager;

describe('ControlConnection', function () {
  this.timeout(240000);
  describe('#init()', function () {
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should retrieve local host and peers', function (done) {
      const cc = newInstance();
      cc.init(function (err) {
        assert.ifError(err);
        assert.strictEqual(cc.hosts.length, 2);
        assert.ok(cc.protocolVersion);
        cc.hosts.forEach(function (h) {
          assert.ok(h.datacenter);
          assert.ok(h.rack);
          assert.ok(h.tokens);
          assert.ok(Array.isArray(h.workloads));
        });
        cc.shutdown();
        done();
      });
    });
    it('should subscribe to SCHEMA_CHANGE events and refresh keyspace information', function (done) {
      const cc = newInstance({ refreshSchemaDelay: 100 });
      const otherClient = new Client(helper.baseOptions);
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
          const keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
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
            return cc.metadata.keyspaces['sample_change_1'].strategyOptions.replication_factor === '2';
          }, 200, 10, next);
        },
        function (next) {
          const keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
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
          const keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
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
      const cc = newInstance({ policies: { loadBalancing: new TestLoadBalancing() } });
      utils.series([
        cc.init.bind(cc),
        helper.delay(2000 + (helper.isWin() ? 13000 : 0)),
        // Don't stop the node until we know it's up.
        helper.waitOnHostUp(cc, 2),
        // Stop the node and ensure it gets marked down.
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        helper.waitOnHostDown(cc, 2),
        function (next) {
          const hosts = cc.hosts.slice(0);
          assert.strictEqual(hosts.length, 2);
          const countUp = hosts.reduce(function (value, host) {
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
      const options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(1000);
      const cc = newInstance(options, 1, 1);
      utils.series([
        cc.init.bind(cc),
        helper.toTask(helper.ccmHelper.bootstrapNode, null, 3),
        helper.toTask(helper.ccmHelper.startNode, null, 3),
        // While the host is started, it's not a given that it will have been connected and marked up,
        // wait for that to be the case.
        helper.waitOnHostUp(cc, 3),
        function (next) {
          const hosts = cc.hosts.slice(0);
          const countUp = hosts.reduce(function (value, host) {
            value += host.isUp() ? 1 : 0;
            return value;
          }, 0);
          assert.strictEqual(countUp, 3);
          cc.shutdown();
          next();
        }
      ], done);
    });
    it('should subscribe to TOPOLOGY_CHANGE remove events and refresh ring info', function (done) {
      const cc = newInstance();
      utils.series([
        cc.init.bind(cc),
        helper.toTask(helper.ccmHelper.decommissionNode, null, 2),
        helper.waitOnHostGone(cc, 2),
        function (next) {
          const hosts = cc.hosts.slice(0);
          assert.strictEqual(hosts.length, 1);
          cc.shutdown();
          next();
        }
      ], done);
    });
    it('should reconnect when host used goes down', function (done) {
      const options = clientOptions.extend(
        utils.extend({ pooling: helper.getPoolingOptions(1, 1, 500) }, helper.baseOptions));
      const cc = new ControlConnection(options, new ProfileManager(options));
      let host1;
      let host2;
      let lbp;
      utils.series([
        cc.init.bind(cc),
        function initLbp(next) {
          lbp = cc.options.policies.loadBalancing;
          lbp.init({ log: utils.noop, options: { localDataCenter: 'dc1' }}, cc.hosts, next);
        },
        function ensureConnected(next) {
          const hosts = cc.hosts.values();
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
        helper.toTask(helper.ccmHelper.stopNode, null, 1),
        helper.waitOnHostDown(cc, 1),
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
      const options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      const reconnectionDelay = 200;
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(reconnectionDelay);
      const cc = newInstance(options, 1, 1);
      utils.series([
        cc.init.bind(cc),
        function initLbp(next) {
          assert.ok(cc.host);
          assert.strictEqual(helper.lastOctetOf(cc.host), '1');
          cc.options.policies.loadBalancing.init({ log: utils.noop, options: { localDataCenter: 'dc1' }}, cc.hosts, next);
        },
        function setHostDistance(next) {
          // the control connection host should be local or remote to trigger DOWN events
          const distance = options.policies.loadBalancing.getDistance(cc.host);
          cc.host.setDistance(distance);
          next();
        },
        // stop nodes 1 and 2 and make sure they both go down.
        helper.toTask(helper.ccmHelper.stopNode, null, 1),
        helper.waitOnHostDown(cc, 1),
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        helper.waitOnHostDown(cc, 2),
        // restart node 2 and make sure it comes up.
        helper.toTask(helper.ccmHelper.startNode, null, 2),
        helper.waitOnHostUp(cc, 2),
        // check that host 1 is down, host 2 is up and the control connection is to host 2.
        function checkHostConnected(next) {
          cc.hosts.forEach(function (h) {
            if (helper.lastOctetOf(h) === '1') {
              assert.strictEqual(h.isUp(), false);
            }
            else {
              assert.strictEqual(h.isUp(), true);
            }
          });
          // Wait until
          setTimeout(function () {
            assert.ok(cc.host);
            assert.strictEqual(helper.lastOctetOf(cc.host), '2');
            cc.shutdown();
            next();
          }, reconnectionDelay * 2);
        }
      ], done);
    });
  });
  describe('#metadata', function () {
    before(helper.ccmHelper.start(3, {vnodes: true}));
    after(helper.ccmHelper.remove);
    it('should contain keyspaces information', function (done) {
      const cc = newInstance();
      cc.init(function () {
        assert.equal(cc.hosts.length, 3);
        assert.ok(cc.metadata);
        assert.strictEqual(cc.hosts.slice(0)[0]['tokens'].length, 256);
        assert.ok(cc.metadata.keyspaces);
        assert.ok(cc.metadata.keyspaces['system']);
        assert.ok(cc.metadata.keyspaces['system'].strategy);
        assert.strictEqual(typeof cc.metadata.keyspaces['system'].tokenToReplica, 'function');
        cc.shutdown();
        done();
      });
    });
  });
});

/** @returns {ControlConnection} */
function newInstance(options, localConnections, remoteConnections) {
  options = clientOptions.extend(utils.deepExtend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions, options));
  //disable the heartbeat
  options.pooling.heartBeatInterval = 0;
  options.pooling.coreConnectionsPerHost[types.distance.local] = localConnections || 2;
  options.pooling.coreConnectionsPerHost[types.distance.remote] = remoteConnections || 1;
  return new ControlConnection(options, new ProfileManager(options));
}