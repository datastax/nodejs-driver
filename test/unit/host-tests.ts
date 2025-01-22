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
const util = require('util');
const events = require('events');

const hostModule = require('../../lib/host');
const Host = hostModule.Host;
const HostConnectionPool = require('../../lib/host-connection-pool');
const Metadata = require('../../lib/metadata');
const HostMap = hostModule.HostMap;
const types = require('../../lib/types');
const clientOptions = require('../../lib/client-options');
const defaultOptions = clientOptions.defaultOptions();
defaultOptions.pooling.coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV3;
const utils = require('../../lib/utils');
const policies = require('../../lib/policies');
const helper = require('../test-helper');
const reconnection = policies.reconnection;

describe('HostConnectionPool', function () {
  this.timeout(5000);
  describe('#borrowConnection()', function () {
    it('should avoid returning the previous connection', () => {
      const hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 4;
      hostPool.connections = [
        { getInFlight: () => 0, index: 0 },
        { getInFlight: () => 0, index: 1 },
        { getInFlight: () => 0, index: 2 },
        { getInFlight: () => 0, index: 3 },
      ];
      const result = new Map();

      // Avoid returning connection at index 2
      const previousConnectionIndex = 2;

      for (let i = 0; i < 8; i++) {
        const c = hostPool.borrowConnection(hostPool.connections[previousConnectionIndex]);
        result.set(c.index, (result.get(c.index) || 0) + 1);
      }

      assert.strictEqual(result.get(0), 2);
      assert.strictEqual(result.get(1), 2);
      assert.strictEqual(result.get(3), 4);
      assert.strictEqual(result.get(previousConnectionIndex), undefined);
    });
  });

  describe('#drainAndShutdown()', function () {
    it('should wait for connections to drain before shutting down', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      const c = new events.EventEmitter();
      c.getInFlight = helper.functionOf(100);
      hostPool.connections = [
        c,
        { close: helper.noop, getInFlight: helper.functionOf(0) }
      ];
      hostPool.drainAndShutdown();
      let drained, closed;
      hostPool.once('close', function () {
        assert.ok(drained);
        assert.ok(closed);
        done();
      });
      c.close = function () {
        closed = true;
      };
      setImmediate(function () {
        drained = true;
        c.emit('drain');
      });
    });

    it('should timeout when draining connections takes longer than expected', function (done) {
      const hostPool = newHostConnectionPoolInstance({ socketOptions: { readTimeout: 20 } });
      const c = new events.EventEmitter();
      c.getInFlight = helper.functionOf(100);
      hostPool.connections = [ c ];
      hostPool.drainAndShutdown();
      let closed;
      hostPool.once('close', function () {
        assert.ok(closed);
      });
      c.close = function () {
        closed = true;
      };
      setTimeout(function () {
        // Its not closed immediately
        assert.ok(!closed);
      }, 10);
      setTimeout(function () {
        // Drain was never emitted but the pool is closed
        assert.ok(closed);
        assert.strictEqual(hostPool.connections.length, 0);
        done();
      }, 140);
    });
  });

  describe('#_attemptNewConnection()', function () {
    it('should create and attempt to open a connection', async () => {
      const hostPool = newHostConnectionPoolInstance();
      const c = sinon.spy({
        openAsync: () => Promise.resolve()
      });

      hostPool._createConnection = function () {
        return c;
      };

      await hostPool._attemptNewConnection();

      assert.strictEqual(1, c.openAsync.callCount);
    });

    it('should callback in error when open fails', async () => {
      const hostPool = newHostConnectionPoolInstance();
      const c = sinon.spy({
        openAsync: () => Promise.reject(new Error('Test dummy error')),
        closeAsync: () => Promise.resolve()
      });

      hostPool._createConnection = function () {
        return c;
      };

      await helper.assertThrowsAsync(hostPool._attemptNewConnection());
      assert.strictEqual(c.openAsync.callCount, 1);
      assert.strictEqual(c.closeAsync.callCount, 1);
    });

    it('should create a single connection with multiple calls in parallel', async () => {
      const hostPool = newHostConnectionPoolInstance();
      const c = sinon.spy({
        openAsync: () => Promise.resolve()
      });

      hostPool._createConnection = sinon.spy(() => c);

      await Promise.all(Array(10).fill(0).map(() => hostPool._attemptNewConnection()));

      assert.strictEqual(c.openAsync.callCount, 1);
      assert.strictEqual(hostPool._createConnection.callCount, 1);
    });
  });

  describe('minInFlight()', function () {
    it('should round robin between connections with the same amount of in-flight requests', function () {
      /** @type {Array.<Connection>} */
      const connections = [];
      for (let i = 0; i < 3; i++) {
        connections.push({ getInFlight: helper.functionOf(0), index: i});
      }

      const initial = HostConnectionPool.minInFlight(connections, 32, null).index;

      for (let i = 1; i < 10; i++) {
        assert.strictEqual(
          (initial + i) % connections.length,
          HostConnectionPool.minInFlight(connections, 32, null).index);
      }
    });

    it('should skip the previous connection', function () {
      /** @type {Array.<Connection>} */
      const connections = [];
      for (let i = 0; i < 5; i++) {
        connections.push({ getInFlight: helper.functionOf(32), index: i});
      }

      const previousConnectionIndex = 2;
      const previousConnection = connections[previousConnectionIndex];

      const initial = HostConnectionPool.minInFlight(connections, 32, null).index;

      // Assert that minInFlight() skips the previous connection
      for (let i = 1; i < 10; i++) {
        let expectedIndex = (initial + i) % connections.length;
        if (expectedIndex === previousConnectionIndex) {
          expectedIndex++;
        }

        assert.strictEqual(
          expectedIndex,
          HostConnectionPool.minInFlight(connections, 32, previousConnection).index);
      }
    });

    it('should skip the previous connection when there are two', function () {
      /** @type {Array.<Connection>} */
      const connections = [];
      for (let i = 0; i < 2; i++) {
        connections.push({ getInFlight: helper.functionOf(32), index: i});
      }

      const previousConnection = connections[1];

      // Assert that minInFlight() skips the previous connection, multiple times
      for (let i = 0; i < 10; i++) {
        assert.strictEqual(0, HostConnectionPool.minInFlight(connections, 32, previousConnection).index);
      }
    });

    it('should not skip the previous connection when there is a single connection in the pool', function () {
      const connections = [ { getInFlight: helper.functionOf(32), index: 0} ];

      for (let i = 0; i < 10; i++) {
        assert.strictEqual(connections[0], HostConnectionPool.minInFlight(connections, 32, connections[0]));
      }
    });
  });
});

describe('Host', function () {

  describe('#setUp()', function () {
    it('should reset the reconnection schedule when bring it up', function () {
      const maxDelay = 1000;
      const options = utils.extend({
        policies: {
          reconnection: new reconnection.ExponentialReconnectionPolicy(50, maxDelay, false)
        }}, defaultOptions);
      const host = newHostInstance(options);
      const create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        const c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      const initialSchedule = options.policies.reconnection.newSchedule();
      host.reconnectionSchedule = initialSchedule;
      host.setDownAt = 1;
      host.setUp();
      assert.notStrictEqual(host.reconnectionSchedule, initialSchedule);
    });
  });

  describe('#setDown()', function () {
    it('should emit event when called', function (done) {
      const host = newHostInstance(defaultOptions);
      host.on('down', done);
      host.setDown();
      host.shutdown(false);
    });
  });

  describe('#getActiveConnection()', function () {
    it('should return null if a the pool is initialized', function () {
      const h = newHostInstance(defaultOptions);
      assert.strictEqual(h.getActiveConnection(), null);
    });
  });

  describe('#setDistance()', function () {
    it('should call checkIsUp() when the new distance is local and was down', function () {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.ignored;
      host.setDownAt = 1;
      let checkIsUpCalled = 0;
      host.checkIsUp = function () { checkIsUpCalled++; };
      host.setDistance(types.distance.local);
      assert.strictEqual(checkIsUpCalled, 1);
      host.shutdown(false);
    });
    it('should call drainAndShutdown() and emit when the new distance is ignored', function () {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      let drainAndShutdownCalled = 0;
      let ignoreEventCalled = 0;
      host.pool.drainAndShutdown = function () { drainAndShutdownCalled++; };
      host.once('ignore', function () {
        ignoreEventCalled++;
      });
      host.setDistance(types.distance.ignored);
      assert.strictEqual(drainAndShutdownCalled, 1);
      assert.strictEqual(ignoreEventCalled, 1);
      assert.strictEqual(host.pool.coreConnectionsLength, 0);
    });
    it('should not call drainAndShutdown() when the new distance is ignored and was previously ignored', function () {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.ignored;
      let drainAndShutdownCalled = 0;
      let ignoreEventCalled = 0;
      host.pool.drainAndShutdown = function () { drainAndShutdownCalled++; };
      host.once('ignore', function () {
        ignoreEventCalled++;
      });
      host.setDistance(types.distance.ignored);
      assert.strictEqual(drainAndShutdownCalled, 0);
      assert.strictEqual(ignoreEventCalled, 0);
    });
  });

  describe('#removeFromPool()', function () {
    it('should remove the connection in a new array instance', function () {
      const host = newHostInstance(defaultOptions);
      const initialConnections = [ newConnectionMock(), newConnectionMock() ];
      host.pool.connections = initialConnections;
      host.removeFromPool(initialConnections[0]);
      assert.deepEqual(host.pool.connections, [ initialConnections[1] ]);
      assert.notStrictEqual(host.pool.connections, initialConnections);
      host.shutdown(false);
    });

    it('should issue a new connection attempt when pool size is smaller than config', function () {
      const host = newHostInstance(defaultOptions);
      const initialConnections = [ newConnectionMock(), newConnectionMock() ];
      host.pool.connections = initialConnections;
      host.pool.coreConnectionsLength = 10;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.removeFromPool(initialConnections[1]);
      assert.deepEqual(host.pool.connections, [ initialConnections[0] ]);
      assert.ok(host.pool.hasScheduledNewConnection());
      assert.ok(host.isUp());
      host.shutdown(false);
    });
    it('should set the host down when no connections', function () {
      const host = newHostInstance(defaultOptions);
      const initialConnections = [ newConnectionMock()];
      host.pool.connections = initialConnections;
      host._distance = types.distance.local;
      assert.ok(!host.pool.hasScheduledNewConnection());
      assert.ok(host.isUp());
      host.removeFromPool(initialConnections[0]);
      assert.deepEqual(host.pool.connections, []);
      assert.ok(host.pool.hasScheduledNewConnection());
      assert.ok(!host.isUp());
      host.shutdown(false);
    });
    it('should not set the host down when it is ignored', function () {
      const host = newHostInstance(defaultOptions);
      const initialConnections = [ newConnectionMock()];
      host.pool.connections = initialConnections;
      host._distance = types.distance.ignored;
      assert.ok(host.isUp());
      host.removeFromPool(initialConnections[0]);
      assert.deepEqual(host.pool.connections, []);
      assert.ok(host.isUp());
      host.shutdown(false);
    });
  });

  describe('#checkHealth()', function () {
    it('should remove connection from Array and invoke close', function (done) {
      const host = newHostInstance(defaultOptions);
      let closeInvoked = 0;
      const c = {
        timedOutOperations: 1000,
        close: function () {
          closeInvoked++;
        }
      };
      const initialConnections = [ newConnectionMock(), newConnectionMock(), c];
      host.pool.connections = initialConnections;
      host.checkHealth(c);
      setImmediate(function () {
        assert.strictEqual(1, closeInvoked);
        assert.deepEqual(host.pool.connections, initialConnections.slice(0, 2));
        // different references
        assert.notStrictEqual(initialConnections, host.pool.connections);
        host.shutdown(false);
        done();
      });
    });
    it('should remove set host down when no more connections available', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.connections = [ newConnectionMock() ];
      assert.ok(host.isUp());
      host.checkHealth(host.pool.connections[0]);
      setImmediate(function () {
        assert.strictEqual(host.pool.connections.length, 0);
        assert.ok(!host.isUp());
        host.shutdown(false);
        done();
      });
    });
  });
  describe('#checkIsUp()', function () {
    it('should schedule a connection attempt', function () {
      const host = newHostInstance(defaultOptions);
      host.setDownAt = 1;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.ok(host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
    it('should reset the reconnection schedule and set the delay to 0', function () {
      const host = newHostInstance(defaultOptions);
      host.setDownAt = 1;
      host.reconnectionDelay = 1;
      const reconnectionSchedule = host.reconnectionSchedule;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.notStrictEqual(host.reconnectionSchedule, reconnectionSchedule);
      assert.strictEqual(host.reconnectionDelay, 0);
      assert.ok(host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
    it('should not issue a connection attempt if host is UP', function () {
      const host = newHostInstance(defaultOptions);
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
  });

  describe('#warmupPool()', function () {
    it('should create the exact amount of connections after borrowing when opening is instant', async () => {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = () => newConnectionMock({ openAsync: () => {} });

      await host.warmupPool();

      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
      await helper.delayAsync(100);
      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
    });

    it('should create the exact amount of connections after borrowing when opening takes some time', async () => {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = () => newConnectionMock({ openAsync: () => helper.delayAsync(20) });

      await host.warmupPool();

      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
      await helper.delayAsync(200);
      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
    });

    it('should create the exact amount of connections when opening is instant', async () => {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = () => newConnectionMock({ openAsync: () => {} });

      await host.warmupPool();

      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
      await helper.delayAsync(100);
      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
    });

    it('should create the exact amount of connections when opening takes some time', async () => {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = () => newConnectionMock({ openAsync: () => helper.delayAsync(20) });

      await host.warmupPool();

      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
      await helper.delayAsync(200);
      assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
    });
  });
});

describe('HostMap', function () {
  describe('#values()', function () {
    it('should return a frozen array', function () {
      const map = new HostMap();
      map.set('h1', 'h1');
      const values = map.values();
      assert.strictEqual(values.length, 1);
      assert.ok(Object.isFrozen(values));
    });

    it('should return the same instance as long as the value does not change', function () {
      const map = new HostMap();
      map.set('h1', 'h1');
      const values1 = map.values();
      const values2 = map.values();
      assert.strictEqual(values1, values2);
      map.set('h2', 'h2');
      const values3 = map.values();
      assert.strictEqual(values3.length, 2);
      assert.notEqual(values3, values1);
    });
  });

  describe('#set()', function () {
    it('should modify the cached values', function () {
      const map = new HostMap();
      map.set('h1', 'v1');
      const values = map.values();
      assert.strictEqual(util.inspect(values), util.inspect(['v1']));
      map.set('h1', 'v1a');
      assert.strictEqual(util.inspect(map.values()), util.inspect(['v1a']));
      assert.strictEqual(map.get('h1'), 'v1a');
      assert.notStrictEqual(map.values(), values);
    });
  });
});

/**
 * @returns {HostConnectionPool}
 */
function newHostConnectionPoolInstance(options) {
  options = utils.extend({ logEmitter: function () {} }, defaultOptions, options);
  return new HostConnectionPool(newHostInstance(options), 2);
}

/**
 * @param {Object} options
 * @returns {Host}
 */
function newHostInstance(options) {
  options = utils.extend({logEmitter: function () {}}, options);
  return new Host('0.0.0.1:9042', 2, options, new Metadata(options, null));
}

/**
 * @returns {Connection}
 */
function newConnectionMock(properties) {
  return utils.extend({
    close: helper.noop,
    closeAsync: () => Promise.resolve(),
    getInFlight: helper.functionOf(0)
  }, properties);
}