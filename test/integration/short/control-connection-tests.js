/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    it('should subscribe to SCHEMA_CHANGE events and refresh keyspace information', async () => {
      const cc = newInstance({ refreshSchemaDelay: 100 });
      const otherClient = new Client(helper.baseOptions);

      helper.afterThisTest(() => otherClient.shutdown());
      disposeAfter(cc);

      await otherClient.connect();
      await cc.init();

      await otherClient.execute("CREATE KEYSPACE sample_change_1 " +
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");

      await helper.setIntervalUntilPromise(() => cc.metadata.keyspaces['sample_change_1'], 50, 100);

      let keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
      assert.ok(keyspaceInfo);
      assert.ok(keyspaceInfo.strategy);
      assert.equal(keyspaceInfo.strategyOptions.replication_factor, 3);
      assert.ok(keyspaceInfo.strategy.indexOf('SimpleStrategy') > 0);

      await otherClient.execute("ALTER KEYSPACE sample_change_1 WITH replication = " +
        "{'class': 'SimpleStrategy', 'replication_factor' : 2}");

      await helper.setIntervalUntilPromise(
        () => cc.metadata.keyspaces['sample_change_1'].strategyOptions.replication_factor === '2', 50, 100);

      keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
      assert.ok(keyspaceInfo);
      assert.equal(keyspaceInfo.strategyOptions.replication_factor, 2);

      await otherClient.execute("DROP keyspace sample_change_1");

      await helper.setIntervalUntilPromise(() => !cc.metadata.keyspaces['sample_change_1'], 50, 100);

      keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
      assert.ok(!keyspaceInfo);
    });

    it('should subscribe to STATUS_CHANGE events', async () => {
      // Only ignored hosts are marked as DOWN when receiving the event
      // Use an specific load balancing policy, to set the node2 as ignored

      class TestLoadBalancing extends policies.loadBalancing.RoundRobinPolicy {
        getDistance(h) {
          return (helper.lastOctetOf(h) === '2' ? types.distance.ignored : types.distance.local);
        }
      }

      const cc = newInstance({ policies: { loadBalancing: new TestLoadBalancing() } });
      disposeAfter(cc);

      await cc.init();

      await helper.delayAsync(2000 + (helper.isWin() ? 13000 : 0));

      // Don't stop the node until we know it's up.
      await helper.wait.forNodeUp(cc.hosts, 2);

      // Stop the node and ensure it gets marked down.
      await util.promisify(helper.ccmHelper.stopNode)(2);

      await helper.wait.forNodeDown(cc.hosts, 2, 1000);

      const hosts = cc.hosts.values();

      const countUp = hosts.reduce((value, host) => value + (host.isUp() ? 1 : 0), 0);

      assert.strictEqual(hosts.length, 2);
      assert.strictEqual(countUp, 1);
    });

    it('should subscribe to TOPOLOGY_CHANGE add events and refresh ring info', async () => {
      const options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(1000);
      options.policies.loadBalancing = new policies.loadBalancing.RoundRobinPolicy();

      const cc = newInstance(options, 1, 1);
      disposeAfter(cc);

      await cc.init();
      await new Promise(r => options.policies.loadBalancing.init(null, cc.hosts, r));

      await util.promisify(helper.ccmHelper.bootstrapNode)({nodeIndex: 3, dc: 'dc1'});
      await util.promisify(helper.ccmHelper.startNode)(3);

      // While the host is started, it's not a given that it will have been connected and marked up,
      // wait for that to be the case.
      await helper.wait.forNodeToBeAdded(cc.hosts, 3);
      await helper.wait.forNodeUp(cc.hosts, 3, 5000, 200);

      const countUp = cc.hosts.values().reduce((value, host) => value + (host.isUp() ? 1 : 0), 0);
      assert.strictEqual(countUp, 3);
    });

    it('should subscribe to TOPOLOGY_CHANGE remove events and refresh ring info', async () => {
      const cc = newInstance();
      cc.options.policies.loadBalancing = new policies.loadBalancing.RoundRobinPolicy();
      disposeAfter(cc);

      await cc.init();
      await new Promise(r => cc.options.policies.loadBalancing.init(null, cc.hosts, r));

      if (helper.isCassandraGreaterThan('4.0')) {
        // To avoid issue "Not enough live nodes to maintain replication factor"
        await cc.query("ALTER KEYSPACE system_traces" +
          " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        await cc.query("ALTER KEYSPACE system_distributed" +
          " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
      }

      await util.promisify(helper.ccmHelper.decommissionNode)(2);

      await helper.wait.forNodeToBeRemoved(cc.hosts, 2);

      assert.strictEqual(cc.hosts.length, 1);
    });

    it('should reconnect when host used goes down', async () => {
      const options = clientOptions.extend(
        utils.extend({ pooling: helper.getPoolingOptions(1, 1, 500) }, helper.baseOptions));
      const cc = new ControlConnection(options, new ProfileManager(options));
      const lbp = cc.options.policies.loadBalancing;
      disposeAfter(cc);

      await cc.init();

      await new Promise(r => lbp.init({ log: utils.noop, options: { localDataCenter: 'dc1' }}, cc.hosts, r));

      const hosts = cc.hosts.values();

      assert.strictEqual(hosts.length, 2);

      for (const h of hosts) {
        h.setDistance(lbp.getDistance(h));
        await h.warmupPool();
      }

      const host1 = hosts[0];
      const host2 = hosts[1];

      await host1.warmupPool();
      await host2.warmupPool();

      assert.strictEqual(host1.pool.connections.length, 1);
      assert.strictEqual(host2.pool.connections.length, 1);

      await util.promisify(helper.ccmHelper.stopNode)(1);
      await helper.wait.forNodeDown(cc.hosts, 1);

      assert.strictEqual(host1.pool.connections.length, 0,
        'Host1 should be DOWN and connections closed (heartbeat enabled)');
      assert.strictEqual(host1.isUp(), false);
      assert.strictEqual(host2.isUp(), true);
      assert.strictEqual(host1.pool.connections.length, 0);
      assert.strictEqual(host2.pool.connections.length, 1);
    });

    it('should reconnect when all hosts go down and back up', async () => {
      const options = clientOptions.extend(utils.extend({ pooling: { coreConnectionsPerHost: {}}}, helper.baseOptions));
      const reconnectionDelay = 200;
      options.policies.reconnection = new policies.reconnection.ConstantReconnectionPolicy(reconnectionDelay);
      const cc = newInstance(options, 1, 1);
      disposeAfter(cc);

      await cc.init();

      assert.ok(cc.host);
      assert.strictEqual(helper.lastOctetOf(cc.host), '1');
      const lbp = cc.options.policies.loadBalancing;

      await new Promise(r => lbp.init({ log: utils.noop, options: { localDataCenter: 'dc1' }}, cc.hosts, r));

      // the control connection host should be local or remote to trigger DOWN events
      for (const h of cc.hosts.values()) {
        h.setDistance(lbp.getDistance(h));
        await h.warmupPool();
      }

      // stop nodes 1 and 2 and make sure they both go down.
      await util.promisify(helper.ccmHelper.stopNode)(1);
      await helper.wait.forNodeDown(cc.hosts, 1);
      await util.promisify(helper.ccmHelper.stopNode)(2);
      await helper.wait.forNodeDown(cc.hosts, 2);

      // restart node 2 and make sure it comes up.
      await util.promisify(helper.ccmHelper.startNode)(2);
      await helper.wait.forNodeUp(cc.hosts, 2, 5000, 200);

      // check that host 1 is down, host 2 is up and the control connection is to host 2.
      cc.hosts.forEach(h => {
        if (helper.lastOctetOf(h) === '1') {
          assert.strictEqual(h.isUp(), false);
        }
        else {
          assert.strictEqual(h.isUp(), true);
        }
      });

      await helper.wait.until(() => cc.host, 5000, 200);

      assert.strictEqual(helper.lastOctetOf(cc.host), '2');
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

function disposeAfter(cc) {
  helper.afterThisTest(() => cc.shutdown());
  helper.afterThisTest(() => cc.hosts.values().forEach(h => h.shutdown()));
}