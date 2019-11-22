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

const { assert } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');

const cloudHelper = require('./cloud-helper');
const helper = require('../../../test-helper');
const policies = require('../../../../lib/policies');
const errors = require('../../../../lib/errors');
const auth = require('../../../../lib/auth');
const utils = require('../../../../lib/utils');
const types = require('../../../../lib/types');
const promiseUtils = require("../../../../lib/promise-utils");
const vdescribe = helper.vdescribe;

const port = 9042;

vdescribe('dse-6.7', 'Cloud support', function () {
  // Only run tests with few versions of DSE as SNI project has a fixed C*/DSE version

  if (helper.isWin()) {
    // Skip altogether for AppVeyor
    return;
  }

  this.timeout(300000);

  context('with a 3 node cluster', () => {

    cloudHelper.setup({
      queries: [
        "CREATE KEYSPACE ks_network_rf1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : 1}",
        "CREATE TABLE ks_network_rf1.table1 (id int primary key, name text)",
      ]
    });

    it('should resolve dns name of the proxy, connect and set defaults', () => {
      const client = cloudHelper.getClient();

      return client.connect()
        .then(() => {
          assert.strictEqual(client.hosts.length, 3);

          assert.ok(client.options.sni.addressResolver.getIp());
          assert.ok(client.options.sni.port);
          assert.strictEqual(client.metadata.isDbaas(), true);
          assert.strictEqual(client.options.queryOptions.consistency, types.consistencies.localQuorum);

          client.hosts.forEach(h => {
            assert.ok(h.isUp());
            helper.assertContains(h.address, `:${port}`);
            assert.strictEqual(h.pool.connections.length, 1);
            assert.strictEqual(h.pool.connections[0].endpointFriendlyName,
              `${client.options.sni.addressResolver.getIp()}:${client.options.sni.port} (${h.hostId})`);
          });
        })
        .then(() => client.shutdown());
    });

    it('should use all the proxy resolved addresses', () => {
      let resolvedAddress;
      const libPath = '../../../../lib';

      const ControlConnection = proxyquire(`${libPath}/control-connection`, {
        'dns':  {
          resolve4: (name, cb) => {
            resolvedAddress = name;
            // Use different loopback addresses
            cb(null, ['127.0.0.1', '127.0.0.2']);
          }
        }
      });

      const Client = proxyquire(`${libPath}/client`, { './control-connection': ControlConnection });

      const client = new Client(cloudHelper.getOptions({ cloud: { secureConnectBundle: 'certs/bundles/creds-v1.zip' } }));

      after(() => client.shutdown());

      return client.connect()
        .then(() => {
          // Validate custom resolution was used
          assert.ok(resolvedAddress);

          const hosts = client.hosts.values();
          assert.strictEqual(hosts.length, 3);

          hosts.forEach((h, index) => {
            assert.strictEqual(h.pool.connections.length, 1);
            // Validate all resolved addresses are used
            assert.strictEqual(h.pool.connections[0].endpoint.split(':')[0], `127.0.0.${(index % 2) + 1}`);
          });

          return client.shutdown();
        });
    });

    it('should match system.local information of each node', () => {
      const client = cloudHelper.getClient({ policies: new policies.loadBalancing.RoundRobinPolicy()});

      // Use round robin to make sure that the 3 host are targeted in 3 executions
      return client.connect()
        .then(() => Promise.all(new Array(3).fill(0).map(() => client.execute('SELECT * FROM system.local'))))
        .then(results => {
          const queried = new Set();
          results.forEach(rs => {
            queried.add(rs.info.queriedHost);
            const host = client.hosts.get(rs.info.queriedHost);
            const row = rs.first();

            assert.ok(host);
            assert.strictEqual(row['host_id'].toString(), host.hostId.toString());
            assert.strictEqual(host.address, `${row['rpc_address']}:${port}`);
          });

          assert.strictEqual(queried.size, 3);
        })
        .then(() => client.shutdown());
    });

    it('should set the auth provider', () => {
      const client = cloudHelper.getClient({ });

      return client.connect()
        .then(() => {
          assert.instanceOf(client.options.authProvider, auth.DsePlainTextAuthProvider);
          assert.strictEqual(client.options.authProvider.username, 'cassandra');
        })
        .then(() => client.shutdown());
    });

    it('should support leaving the auth unset', () => {
      const client = cloudHelper.getClient({ cloud: { secureConnectBundle: 'certs/bundles/creds-v1-wo-creds.zip' } });

      return client.connect()
        .catch(() => {})
        .then(() => assert.strictEqual(client.options.authProvider, null))
        .then(() => client.shutdown());
    });

    it('should support overriding the auth provider', () => {
      const authProvider = new auth.DsePlainTextAuthProvider('user1', '12345678');
      const client = cloudHelper.getClient({ authProvider });

      return client.connect()
        .catch(err => {
          assert.instanceOf(err, errors.NoHostAvailableError);
          assert.instanceOf(utils.objectValues(err.innerErrors)[0], errors.AuthenticationError);
        })
        .then(() => assert.strictEqual(client.options.authProvider, authProvider))
        .then(() => client.shutdown());
    });

    it('should callback in error when bundle file does not exist', () => {
      const client = cloudHelper.getClient({ cloud: { secureConnectBundle: 'certs/bundles/does-not-exist.zip' }});
      let error;

      return client.connect()
        .catch(err => error = err)
        .then(() => {
          assert.instanceOf(error, Error);
          assert.strictEqual(error.code, 'ENOENT');
        })
        .then(() => client.shutdown());
    });

    it('should provide token-aware load balancing by default', () => {
      const replicasByKey = [[0, 2], [1, 2], [2, 2], [3, 1], [4, 3], [5, 2]];

      const client = cloudHelper.getClient();

      return client.connect()
        .then(() => Promise.all(replicasByKey.map(item => {
          const query = 'INSERT INTO ks_network_rf1.table1 (id, name) VALUES (?, ?)';
          const params = [ item[0], `name for id ${item[0]}`];
          const replica = item[1].toString();

          return client.execute(query, params, { prepare: true })
            .then(rs => assert.strictEqual(helper.lastOctetOf(rs.info.queriedHost), replica));
        })))
        .then(() => client.shutdown());
    });

    context('with nodes going down', () => {
      beforeEach(() => cloudHelper.startAllNodes());
      after(() => cloudHelper.startAllNodes());

      it('should reconnect to the same host and refresh dns resolution', async () => {
        const client = cloudHelper.getClient({
          pooling: { heartBeatInterval: 50 },
          policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(20) }
        });

        helper.afterThisTest(() => client.shutdown());
        await client.connect();

        assert.strictEqual(client.hosts.values().find(h => !h.isUp()), undefined);

        await cloudHelper.stopNode(1);
        await helper.setIntervalUntilPromise(() => client.hosts.values().find(h => !h.isUp()), 20, 1000);

        assert.strictEqual(client.hosts.values().filter(h => h.isUp()).length, 2);

        // Patch refresh() method
        const resolver = client.options.sni.addressResolver;
        resolver.refresh = sinon.spy(resolver.refresh);

        await cloudHelper.startNode(1);
        await helper.wait.forAllNodesUp(client, 1000, 20);

        // Check that the driver was able to reconnect and refresh method was called
        assert.strictEqual(client.hosts.values().filter(h => h.isUp()).length, 3);
        assert.ok(resolver.refresh.called);
      });

      it('should continue querying', async () => {
        const client = cloudHelper.getClient({
          pooling: { heartBeatInterval: 50 },
          policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(40) },
          queryOptions: { isIdempotent: true }
        });

        let restarted = false;
        helper.afterThisTest(() => client.shutdown());

        await client.connect();

        assert.strictEqual(client.hosts.values().find(h => !h.isUp()), undefined);

        // In the background, stop and restart a node
        Promise.resolve()
          .then(() => cloudHelper.stopNode(1))
          .then(() => helper.delayAsync(200))
          .then(() => cloudHelper.startNode(1))
          .then(() => helper.delayAsync(500))
          .then(() => restarted = true);

        await repeatUntil(
          () => restarted,
          () => promiseUtils.times(1000, 32, () => client.execute(helper.queries.basic)));
      });
    });
  });

  context('without a metadata service', () => {

    it('should throw a NoHostAvailableError when there address is unreachable', () => {
      let error;

      // Use bundle with unreachable address
      const client = cloudHelper.getClient({ cloud: { secureConnectBundle: 'certs/bundles/creds-v1-unreachable.zip' } });

      return client.connect()
        .catch(err => error = err)
        .then(() => {
          assert.instanceOf(error, errors.NoHostAvailableError);
          assert.strictEqual(error.message, 'There was an error fetching the metadata information');
          assert.deepStrictEqual(Object.keys(error.innerErrors), ['192.0.2.255:30443/metadata']);
        });
    });

    it('should throw a NoHostAvailableError when there is a https connection error', () => {
      let error;

      // Use localhost
      const client = cloudHelper.getClient({ cloud: { secureConnectBundle: 'certs/bundles/creds-v1.zip' } });

      return client.connect()
        .catch(err => error = err)
        .then(() => {
          assert.instanceOf(error, errors.NoHostAvailableError);
          assert.deepStrictEqual(Object.keys(error.innerErrors), ['localhost:30443/metadata']);
        });
    });
  });
});

function repeatUntil(conditionFn, promiseFn) {
  if (conditionFn()) {
    return Promise.resolve();
  }

  return promiseFn().then(() => repeatUntil(conditionFn, promiseFn));
}