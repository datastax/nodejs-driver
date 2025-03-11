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
import { assert } from "chai";
import events from "events";
import proxyquire from "proxyquire";
import util from "util";
import helper from "../test-helper";
import ControlConnection from "../../lib/control-connection";
import utils from "../../lib/utils";
import Metadata from "../../lib/metadata/index";
import types from "../../lib/types/index";
import errors from "../../lib/errors";
import policies from "../../lib/policies/index";
import clientOptions from "../../lib/client-options";
import { Host } from "../../lib/host";
import { ProfileManager } from "../../lib/execution-profile";

describe('ControlConnection', function () {
  describe('constructor', function () {
    it('should create a new metadata instance', function () {
      const cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      helper.assertInstanceOf(cc.metadata, Metadata);
    });
  });
  describe('#init()', function () {
    this.timeout(20000);

    const localhost = 'localhost';

    async function testResolution(CcMock, expectedHosts, expectedResolved, hostName) {
      if (!expectedResolved) {
        expectedResolved = expectedHosts;
      }

      const contactPointHostName = (hostName || localhost);
      const state = {};
      const cc = new CcMock(clientOptions.extend({ contactPoints: [contactPointHostName] }), null, getContext({
        failBorrow: 10, state
      }));

      let err;

      try {
        await cc.init();
      } catch (e) {
        err = e;
      }

      cc.shutdown();
      assert.instanceOf(err, errors.NoHostAvailableError);
      assert.deepStrictEqual(state.connectionAttempts.sort(), expectedHosts.sort());
      const resolvedContactPoints = cc.getResolvedContactPoints();
      assert.deepStrictEqual(resolvedContactPoints.get(contactPointHostName), expectedResolved);
    }

    // Simple utility function to return a value only if we actually get a request for the name
    // "localhost".  Allows us to make our mocks a bit more stringent.
    function ifLocalhost(name, localhostVal) {
      if (name === localhost) {
        return localhostVal;
      }
      return [];
    }

    it('should resolve IPv4 and IPv6 addresses, default host (localhost) and port', () => {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(null, ifLocalhost(name, ['127.0.0.1']));
        },
        resolve6: function (name, cb) {
          cb(null, ifLocalhost(name, ['::1']));
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      }});

      return testResolution(ControlConnectionMock,
        [ '127.0.0.1:9042', '::1:9042' ],
        [ '127.0.0.1:9042', '[::1]:9042' ]);
    });

    it('should resolve IPv4 and IPv6 addresses with non default port', () => {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(null, ifLocalhost(name, ['127.0.0.1']));
        },
        resolve6: function (name, cb) {
          cb(null, ifLocalhost(name, ['::1']));
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      }});

      return testResolution(ControlConnectionMock,
        [ '127.0.0.1:9999', '::1:9999' ],
        [ '127.0.0.1:9999', '[::1]:9999' ],
        'localhost:9999');
    });

    it('should resolve all IPv4 and IPv6 addresses provided by dns.resolve()', () => {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(null, ['1', '2']);
        },
        resolve6: function (name, cb) {
          cb(null, ['10', '20']);
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      }});

      return testResolution(ControlConnectionMock,
        [ '1:9042', '2:9042', '10:9042', '20:9042' ],
        [ '1:9042', '2:9042', '[10]:9042', '[20]:9042' ]);
    });

    it('should ignore IPv4 or IPv6 resolution errors', function () {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(null, ['1', '2']);
        },
        resolve6: function (name, cb) {
          cb(new Error('Test error'));
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      }});

      return testResolution(ControlConnectionMock, [ '1:9042', '2:9042']);
    });

    it('should use dns.lookup() as failover', () => {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(new Error('Test error'));
        },
        resolve6: function (name, cb) {
          cb(new Error('Test error'));
        },
        lookup: function (name, options, cb) {
          cb(null, [{ address: '123', family: 4 }]);
        }
      }});

      return testResolution(ControlConnectionMock, [ '123:9042' ]);
    });

    it('should use dns.lookup() when no address was resolved', () => {
      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          cb(null);
        },
        resolve6: function (name, cb) {
          cb(null, []);
        },
        lookup: function (name, options, cb) {
          cb(null, [{ address: '1234', family: 4 }]);
        }
      }});

      return testResolution(ControlConnectionMock, [ '1234:9042' ]);
    });

    it('should continue iterating through the hosts when borrowing a connection fails',async () => {
      const state = {};
      const contactPoints = [ '::1', '::2' ];
      const cc = newInstance({ contactPoints }, getContext({ state, failBorrow: [ 0 ] }));

      await cc.init();
      cc.shutdown();

      assert.ok(cc.initialized);
      assert.deepStrictEqual(state.connectionAttempts.sort(), contactPoints.map(x => `${x}:9042`));
      helper.assertMapEqual(
        cc.getResolvedContactPoints(),
        new Map([['::1', ['[::1]:9042']], ['::2', ['[::2]:9042']]]));
    });

    it('should borrow connections in random order', async () => {
      // collect unique permutations of borrow order.
      const borrowOrders = new Set();

      for (let i = 0; i < 20; i++) {
        const state = {};
        const cc = newInstance({ contactPoints: [ '::1', '::2', '::3', '::4' ] }, getContext({ state, failBorrow: 4 }));

        try {
          await cc.init();
        } catch (err) {
          cc.hosts.values().forEach(h => h.shutdown());
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          borrowOrders.add(state.connectionAttempts.join());
        } finally {
          cc.shutdown();
        }
      }

      // should have been more than 1 unique permutation
      assert.ok(borrowOrders.size > 1);
    });

    it('should callback with NoHostAvailableError when borrowing all connections fail', async () => {
      const cc = newInstance({ contactPoints: [ '::1', '::2' ] }, getContext({ failBorrow: 2 }));

      let err;

      try {
        await cc.init();
      } catch (e) {
        err = e;
      }

      cc.shutdown();
      cc.hosts.values().forEach(h => h.shutdown());
      helper.assertInstanceOf(err, errors.NoHostAvailableError);
      assert.strictEqual(Object.keys(err.innerErrors).length, 2);
      assert.ok(!cc.initialized);
    });

    it('should continue iterating through the hosts when metadata retrieval fails',async () => {
      const cc = newInstance({ contactPoints: [ '::1', '::2' ] }, getContext({
        queryResults: { '::1': 'Test error, failed query' }
      }));

      await cc.init();

      cc.shutdown();
      cc.hosts.values().forEach(h => h.shutdown());
    });

    it('should listen to socketClose and reconnect', async () => {
      const state = {};
      const peersRows = [
        {'rpc_address': types.InetAddress.fromString('::2') }
      ];

      const lbp = new policies.loadBalancing.RoundRobinPolicy();

      const cc = newInstance({ contactPoints: [ '::1', '::2' ], policies: { loadBalancing: lbp } }, getContext({
        state, queryResults: { 'peers': peersRows }
      }));

      await cc.init();

      assert.ok(state.connection);
      assert.strictEqual(state.hostsTried.length, 0);
      assert.strictEqual(state.connectionAttempts.length, 1);
      lbp.init(null, cc.hosts, utils.noop);

      state.connection.emit('socketClose');

      await helper.delayAsync();

      // Attempted reconnection and succeeded
      assert.strictEqual(state.hostsTried.length, 1);
      cc.shutdown();
      cc.hosts.values().forEach(h => h.shutdown());
    });

    it('should add an address if previous resolution failed', async () => {
      let dnsWorks = false;
      const hostname = 'my-host-name';
      const resolvedAddresses = ['1','2'];

      const ControlConnectionMock = proxyquire('../../lib/control-connection', { dns: {
        resolve4: function (name, cb) {
          if (dnsWorks) {
            cb(null, resolvedAddresses);
          }
          else {
            cb(null, []);
          }
        },
        resolve6: function (name, cb) {
          throw new Error('IPv6 resolution errors should be ignored');
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      }});
      const cc = new ControlConnectionMock(
        clientOptions.extend({ contactPoints: [hostname] }),
        null,
        getContext());

      let err = null;

      try {
        await cc.init();
      } catch (e) {
        err = e;
      }
      assert.instanceOf(err, errors.NoHostAvailableError);
      assert.deepStrictEqual(cc.getResolvedContactPoints().get(hostname), utils.emptyArray);

      // Make DNS resolution magically work and re-run initialization
      err = null;
      dnsWorks = true;

      try {
        await cc.init();
      } catch (e) {
        err = e;
      }

      assert.isNull(err);
      assert.deepStrictEqual(
        cc.getResolvedContactPoints().get(hostname),
        resolvedAddresses.map(a => a + ":9042"));
    });
  });

  describe('#getAddressForPeerHost()', function() {
    it('should handle null, 0.0.0.0 and valid addresses', async () => {
      const options = clientOptions.extend({}, helper.baseOptions);
      const cc = newInstance(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      const peer = getInet([100, 100, 100, 100]);

      assert.strictEqual(
        await cc.getAddressForPeerHost({ 'rpc_address': getInet([1, 2, 3, 4]), peer }, 9042), '1.2.3.4:9042');
      assert.strictEqual(
        await cc.getAddressForPeerHost({ 'rpc_address': getInet([0, 0, 0, 0]), peer }, 9001), '100.100.100.100:9001');

      assert.strictEqual(await cc.getAddressForPeerHost({ 'rpc_address': null, peer }, 9042), null);
    });

    it('should call the AddressTranslator', async () => {
      const options = clientOptions.extend({}, helper.baseOptions);
      let address = null;
      let port = null;
      options.policies.addressResolution = policies.defaultAddressTranslator();
      options.policies.addressResolution.translate = function (addr, p, cb) {
        address = addr;
        port = p;
        cb(addr + ':' + p);
      };

      const cc = newInstance(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;

      const row = {'rpc_address': getInet([5, 2, 3, 4]), peer: null};
      assert.strictEqual(await cc.getAddressForPeerHost(row, 9055), '5.2.3.4:9055');
      assert.strictEqual(address, '5.2.3.4');
      assert.strictEqual(port, 9055);
    });
  });

  describe('#setPeersInfo()', function () {
    it('should not add invalid addresses',async () => {
      const options = clientOptions.extend({}, helper.baseOptions);
      delete options.localDataCenter;
      const cc = newInstance(options);
      cc.host = new Host('18.18.18.18', 1, options);
      const rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1])},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1])},
        //should not be added
        {'rpc_address': null, peer: utils.allocBufferFromArray([1, 1, 1, 1])},
        //should use peer address
        {'rpc_address': getInet([0, 0, 0, 0]), peer: getInet([5, 5, 5, 5])}
      ];

      await cc.setPeersInfo(true, { rows });
      assert.strictEqual(cc.hosts.length, 3);
      assert.ok(cc.hosts.get('5.4.3.2:9042'));
      assert.ok(cc.hosts.get('9.8.7.6:9042'));
      assert.ok(cc.hosts.get('5.5.5.5:9042'));
    });

    it('should set the host datacenter and cassandra version', async () => {
      const options = utils.extend(clientOptions.extend({}, helper.baseOptions), { localDataCenter: 'dc101' });
      const cc = newInstance(options);
      const rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1]), data_center: 'dc100', release_version: '2.1.4'},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1]), data_center: 'dc101', release_version: '2.1.4'}
      ];

      await cc.setPeersInfo(true, { rows });
      assert.strictEqual(cc.hosts.length, 2);
      assert.ok(cc.hosts.get('5.4.3.2:9042'));
      assert.strictEqual(cc.hosts.get('5.4.3.2:9042').datacenter, 'dc100');
      assert.strictEqual(cc.hosts.get('5.4.3.2:9042').cassandraVersion, '2.1.4');
      assert.ok(cc.hosts.get('9.8.7.6:9042'));
      assert.strictEqual(cc.hosts.get('9.8.7.6:9042').datacenter, 'dc101');
      assert.strictEqual(cc.hosts.get('9.8.7.6:9042').cassandraVersion, '2.1.4');
    });

    it('should throw an error if configured localDataCenter is not found among hosts', async () => {
      const options = utils.extend(clientOptions.extend({}, helper.baseOptions), { localDataCenter: 'dc102' });
      const cc = newInstance(options);
      const rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1]), data_center: 'dc100', release_version: '2.1.4'},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1]), data_center: 'dc101', release_version: '2.1.4'}
      ];

      let err;
      try {
        await cc.setPeersInfo(true, { rows });
      } catch (e) {
        err = e;
      }

      assert.instanceOf(err, errors.ArgumentError);
    });

    it('should not throw an error if localDataCenter is not configured', async () => {
      const options = clientOptions.extend({}, helper.baseOptions);
      delete options.localDataCenter;
      const cc = newInstance(options);
      const rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1]), data_center: 'dc100', release_version: '2.1.4'},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1]), data_center: 'dc101', release_version: '2.1.4'}
      ];

      await cc.setPeersInfo(true, { rows });
      assert.strictEqual(cc.hosts.length, 2);
    });
  });

  describe('#refresh()', function () {
    it('should schedule reconnection when it cant borrow a connection', async () => {
      const state = {};
      const lbp = new policies.loadBalancing.RoundRobinPolicy();
      lbp.queryPlanCount = 0;
      lbp.newQueryPlan = function (ks, o, cb) {
        if (lbp.queryPlanCount++ === 0) {
          // Return an empty query plan the first time
          return cb(null, utils.arrayIterator([]));
        }
        return cb(null, [ lbp.hosts.values()[1], lbp.hosts.values()[0] ][Symbol.iterator]());
      };

      const rp = new policies.reconnection.ConstantReconnectionPolicy(40);
      rp.nextDelayCount = 0;
      rp.newSchedule = function*() {
        rp.nextDelayCount++;
        yield this.delay;
      };

      const cc = newInstance(
        { contactPoints: [ '::1' ], policies: { loadBalancing: lbp, reconnection: rp } },
        getContext({ state: state, queryResults: { 'peers': [ {'rpc_address': types.InetAddress.fromString('::2') } ] }, failBorrow: [-1,1]}));

      await cc.init();

      assert.ok(state.connection);
      assert.strictEqual(state.hostsTried.length, 0);
      assert.strictEqual(cc.hosts.length, 2);

      lbp.init(null, cc.hosts, utils.noop);
      const previousConnection = state.connection;
      state.connection.emit('socketClose');

      await helper.delayAsync(0);
      // Scheduled reconnection
      // nextDelayCount should be 2 as both the host and the control connection are reconnecting
      assert.strictEqual(rp.nextDelayCount, 2);

      await helper.delayAsync(50);

      // Reconnected
      assert.strictEqual(state.hostsTried.length, 1);
      // Changed connection
      assert.notEqual(state.connection, previousConnection);
      cc.shutdown();
      cc.hosts.values().forEach(h => h.shutdown());
    });
  });
});

/**
 * @param {Array} bytes
 * @returns {exports.InetAddress}
 */
function getInet(bytes) {
  return new types.InetAddress(utils.allocBufferFromArray(bytes));
}

/** @return {ControlConnection} */
function newInstance(options, context) {
  options = clientOptions.extend(options || {});
  return new ControlConnection(options, new ProfileManager(options), context);
}

function getFakeConnection(endpoint, queryResults) {
  queryResults = queryResults || {};
  const c = new events.EventEmitter();
  c.protocolVersion = types.protocolVersion.maxSupported;
  c.endpoint = endpoint;
  c.connected = true;
  c.requests = [];
  const queryResultKeys = Object.keys(queryResults);
  const defaultResult = { rows: [ {} ] };
  c.sendStream = function (request, options, cb) {
    c.requests.push(request);
    let result;
    for (let i = 0; i < queryResultKeys.length; i++) {
      const key = queryResultKeys[i];
      const re = new RegExp(key);
      if (re.test(request.query) || re.test(endpoint)) {
        result = queryResults[key];
        break;
      }
    }

    if (Array.isArray(result)) {
      result = { rows: result };
    }

    if (typeof result === 'string') {
      cb(new Error(result));
    } else {
      cb(null, result || defaultResult);
    }
  };
  c.close = cb => (cb ? cb() : null);
  c.closeAsync = () => Promise.resolve();
  c.send = util.promisify(c.sendStream);
  return c;
}

/**
 * Gets the ControlConnection context
 * @param {{hosts: Array|undefined, failBorrow: Array|undefined, queryResults: Object|undefined,
 *   state: Object|undefined}} [options]
 */
function getContext(options) {
  options = options || {};
  // hosts that the ControlConnection used to borrow a connection
  const state = options.state || {};
  state.connectionAttempts = [];
  state.hostsTried = [];
  let failBorrow = options.failBorrow || [];

  if (typeof failBorrow === 'number') {
    failBorrow = Array.from(new Array(failBorrow).keys());
  }

  let index = 0;

  return {
    borrowHostConnection: function (h) {
      const i = options.state.hostsTried.length;
      options.state.hostsTried.push(h);
      state.host = h;
      if (failBorrow.indexOf(i) >= 0) {
        throw new Error('Test error');
      }

      return state.connection = getFakeConnection(h.address, options.queryResults);
    },
    createConnection: function (endpoint) {
      state.connectionAttempts.push(endpoint);
      if (failBorrow.indexOf(index++) >= 0) {
        throw new Error('Fake connect error');
      }

      return state.connection = getFakeConnection(endpoint, options.queryResults);
    }
  };
}