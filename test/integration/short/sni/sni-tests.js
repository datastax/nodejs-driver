/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const assert = require('assert');
const rewire = require('rewire');

const sniHelper = require('./sni-helper');
const helper = require('../../../test-helper');
const policies = require('../../../../lib/policies');

const port = 9042;

describe('SNI support', function () {
  this.timeout(120000);

  const setupInfo = sniHelper.setup();

  context('with a 3 node cluster', () => {

    it('should resolve dns name of the proxy', () => {
      const client = setupInfo.getClient();

      return client.connect()
        .then(() => {
          assert.strictEqual(client.hosts.length, 3);

          assert.ok(client.options.sni.addressResolver.getIp());
          assert.ok(client.options.sni.port);

          client.hosts.forEach(h => {
            assert.ok(h.isUp());
            helper.assertContains(h.address, `:${port}`);
            assert.strictEqual(h.pool.connections.length, 1);
            assert.strictEqual(h.pool.connections[0].endpointFriendlyName,
              `${client.options.sni.addressResolver.getIp()}:${client.options.sni.port} (${h.hostId})`);
          });
        });
    });

    it('should use the proxy address when an IP is provided', () => {
      const client = setupInfo.getClient();

      const proxyAddress = client.options.sni.address;
      assert.ok(proxyAddress);
      client.options.sni.address = `127.0.0.1${proxyAddress.substr(proxyAddress.indexOf(':'))}`;

      return client.connect()
        .then(() => client.hosts.forEach(h => assert.strictEqual(h.pool.connections.length, 1)))
        .then(() => client.shutdown());
    });

    it('should use all the proxy resolved addresses', () => {
      const libPath = '../../../../lib';
      const hostName = 'dummy-host';
      let resolvedAddress;

      const cc = rewire(`${libPath}/control-connection`);
      cc.__set__("dns", {
        resolve4: (name, cb) => {
          resolvedAddress = name;
          // Use different loopback addresses
          cb(null, ['127.0.0.1', '127.0.0.2']);
        }
      });

      const Client = rewire(`${libPath}/client`);
      Client.__set__("ControlConnection", cc);

      const client = setupInfo.getClient(null, Client);
      const proxyAddress = client.options.sni.address;
      assert.ok(proxyAddress);
      client.options.sni.address = `${hostName}${proxyAddress.substr(proxyAddress.indexOf(':'))}`;

      return client.connect()
        .then(() => {
          assert.strictEqual(resolvedAddress, hostName);
          const hosts = client.hosts.values();
          hosts.forEach((h, index) => {
            assert.strictEqual(h.pool.connections.length, 1);
            assert.strictEqual(h.pool.connections[0].endpoint.split(':')[0], `127.0.0.${(index % 2) + 1}`);
          });
        })
        .then(() => client.shutdown());
    });

    it('should match system.local information of each node', () => {
      // Use round robin to make sure that the 3 host are targeted in 3 executions
      const client = setupInfo.getClient({ policies: new policies.loadBalancing.RoundRobinPolicy() });

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
        });
    });
  });

  context('with nodes going down', () => {
    beforeEach(() => sniHelper.startAllNodes());

    it('should reconnect to the same host and refresh dns resolution', () => {
      const client = setupInfo.getClient({
        pooling: { heartBeatInterval: 50 },
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(20) }
      });

      let refreshCalled = false;

      return client.connect()
        .then(() => assert.strictEqual(client.hosts.values().find(h => !h.isUp()), undefined))
        .then(() => sniHelper.stopNode(1))
        .then(() => helper.setIntervalUntilPromise(() => client.hosts.values().find(h => !h.isUp()), 20, 1000))
        .then(() => {
          assert.strictEqual(client.hosts.values().filter(h => h.isUp()).length, 2);

          // Patch refresh() method
          const resolver = client.options.sni.addressResolver;
          const fn = resolver.refresh;
          resolver.refresh = (cb) => {
            refreshCalled = true;
            fn.call(resolver, cb);
          };
        })
        .then(() => sniHelper.startNode(1))
        .then(() => helper.setIntervalUntilPromise(() => !client.hosts.values().find(h => !h.isUp()), 20, 1000))
        .then(() => {
          assert.strictEqual(client.hosts.values().filter(h => h.isUp()).length, 3);
          assert.ok(refreshCalled);
        });
    });

    it('should continue querying', () => {
      const client = setupInfo.getClient({
        pooling: { heartBeatInterval: 50 },
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(40) },
        queryOptions: { isIdempotent: true }
      });

      let restarted = false;

      return client.connect()
        .then(() => assert.strictEqual(client.hosts.values().find(h => !h.isUp()), undefined))
        .then(() => {
          // In the background, stop and restart a node
          Promise.resolve()
            .then(() => sniHelper.stopNode(1))
            .then(() => promiseDelay(200))
            .then(() => sniHelper.startNode(1))
            .then(() => promiseDelay(500))
            .then(() => restarted = true);
        })
        .then(() => repeatUntil(
          () => restarted,
          () => promiseRepeat(1000, 32, () => client.execute(helper.queries.basic))
        ))
        .then(() => client.shutdown());
    });
  });
});

function repeatUntil(conditionFn, promiseFn) {
  if (conditionFn()) {
    return Promise.resolve();
  }

  return promiseFn().then(() => repeatUntil(conditionFn, promiseFn));
}


function promiseRepeat(times, limit, fn){
  if (times < limit) {
    limit = times;
  }

  let counter = 0;

  const promises = new Array(limit);

  function sendNext() {
    if (counter >= times) {
      return null;
    }
    return fn(counter++).then(sendNext);
  }

  for (let i = 0; i < limit; i++) {
    promises[i] = sendNext();
  }

  return Promise.all(promises).then(() => null);
}

function promiseDelay(ms) {
  return new Promise(r => setTimeout(r, ms));
}