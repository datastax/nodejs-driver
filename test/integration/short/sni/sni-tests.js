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
const errors = require('../../../../lib/errors');
const Client = require('../../../../lib/dse-client');

const port = 9042;

describe('SNI support', function () {
  this.timeout(120000);

  const setupInfo = sniHelper.setup();

  context('with a 3 node cluster', () => {

    it('should resolve dns name of the proxy', () =>
      setupInfo.getClient()
        .then(client => {
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
        }));

    it('should use all the proxy resolved addresses', () => {
      const hostName = 'dummy-host';
      let resolvedAddress;
      let client;

      return setupInfo.getClient()
        .then(tempClient => {
          const libPath = '../../../../lib';

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

          // Use a tempClient to extract all the options
          client = new Client(tempClient.options);
          const proxyAddress = client.options.sni.address;
          assert.ok(proxyAddress);
          client.options.sni.address = `${hostName}${proxyAddress.substr(proxyAddress.indexOf(':'))}`;

          return tempClient.shutdown();
        })
        .then(() => client.connect())
        .then(() => {
          assert.strictEqual(resolvedAddress, hostName);
          const hosts = client.hosts.values();
          hosts.forEach((h, index) => {
            assert.strictEqual(h.pool.connections.length, 1);
            assert.strictEqual(h.pool.connections[0].endpoint.split(':')[0], `127.0.0.${(index % 2) + 1}`);
          });

          return client.shutdown();
        });
    });

    it('should match system.local information of each node', () => {
      let client;

      // Use round robin to make sure that the 3 host are targeted in 3 executions
      return setupInfo.getClient({ policies: new policies.loadBalancing.RoundRobinPolicy() })
        .then(clientInstance => client = clientInstance)
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
      let client;
      let refreshCalled = false;

      return setupInfo
        .getClient({
          pooling: { heartBeatInterval: 50 },
          policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(20) }
        })
        .then(clientInstance => client = clientInstance)
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
      let client;
      let restarted = false;

      return setupInfo
        .getClient({
          pooling: { heartBeatInterval: 50 },
          policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(40) },
          queryOptions: { isIdempotent: true }
        })
        .then(clientInstance => client = clientInstance)
        .then(() => client.connect())
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

  context('without a metadata service', () => {

    it('should throw a NoHostAvailableError when there is a https connection error', () => {
      let error;
      // Use an unreachable address
      return Client.forClusterConfig('172.31.255.255:30443/path-to-metadata')
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, errors.NoHostAvailableError);
          assert.strictEqual(error.message, 'There was an error fetching the metadata information');
          assert.deepStrictEqual(Object.keys(error.innerErrors), ['172.31.255.255:30443/path-to-metadata']);
        });
    });

    it('should throw a NoHostAvailableError when path is invalid', () => {
      let error;
      return Client.forClusterConfig('127.0.0.1:30443/invalid')
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, errors.NoHostAvailableError);
          assert.strictEqual(error.message, 'There was an error fetching the metadata information');
          assert.deepStrictEqual(Object.keys(error.innerErrors), ['127.0.0.1:30443/invalid']);
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