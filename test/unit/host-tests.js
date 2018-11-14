/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
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
const utils = require('../../lib/utils.js');
const policies = require('../../lib/policies');
const helper = require('../test-helper');
const reconnection = policies.reconnection;

describe('HostConnectionPool', function () {
  this.timeout(5000);
  describe('#create()', function () {
    it('should create the pool once', function (done) {
      const hostPool = newHostConnectionPoolInstance( { pooling: { warmup: true }} );
      hostPool._createConnection = function () {
        return { open: function (cb) {
          setTimeout(cb, 30);
        }};
      };
      hostPool.coreConnectionsLength = 10;
      utils.times(5, function (n, next) {
        //even though it is called multiple times in parallel
        //it should only create a pool with 10 connections
        hostPool.create(true, function (err) {
          assert.equal(err, null);
          assert.strictEqual(hostPool.connections.length, 10);
          next();
        });
      }, done);
    });
    it('should never callback with unopened connections', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 10;
      hostPool._createConnection = function () {
        return {
          open: function (cb) {
            this.connected = true;
            setTimeout(cb, 30);
          }
        };
      };
      utils.times(5, function(n, next) {
        setTimeout(function () {
          hostPool.create(false, function (err) {
            assert.ifError(err);
            const closedConnections = hostPool.connections.filter(function (x) {return !x.connected;}).length;
            if (closedConnections)
            {
              return next(new Error('All connections should be opened: ' + closedConnections + ' closed'));
            }
            next();
          });
        }, n);
      }, function (err) {
        assert.ifError(err);
        done();
      });
    });
    it('should never callback with unopened connections when resizing', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 1;
      hostPool._createConnection = function () {
        return {
          open: function (cb) {
            this.connected = true;
            setTimeout(cb, 50);
          }
        };
      };
      let counter = 0;
      utils.timesLimit(10, 4, function(n, next) {
        counter++;
        hostPool.create(false, function (err) {
          setImmediate(function () {
            assert.ifError(err);
            const closedConnections = hostPool.connections.filter(function (x) {return !x.connected;}).length;
            if (closedConnections) {
              return next(new Error('All connections should be opened: ' + closedConnections + ' closed'));
            }
            if (counter > 5) {
              hostPool.coreConnectionsLength = 15;
            }
            next();
          });
        });
      }, done);
    });
    it('should remove connections and callback in error if state changed to closing', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      hostPool._createConnection = function () {
        return { open: helper.callbackNoop, close: helper.noop };
      };
      process.nextTick(function () {
        // Set the state to shutdown
        hostPool.shutdown(helper.noop);
      });
      hostPool.create(false, function (err) {
        helper.assertInstanceOf(err, Error);
        assert.strictEqual(err.message, 'Pool is being closed');
        assert.strictEqual(0, hostPool.connections.length);
        done();
      });
    });
  });
  describe('#createAndBorrowConnection()', function () {
    it('should get an open connection', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 10;
      hostPool._createConnection = function () {
        return {
          open: function (cb) {
            this.connected = true;
            setTimeout(cb, 30);
          },
          getInFlight: helper.functionOf(0)
        };
      };
      hostPool.createAndBorrowConnection(null, function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        done();
      });
    });
    it('should balance between connections avoiding busy ones', function (done) {
      const maxRequestsPerConnection = clientOptions.maxRequestsPerConnectionV3;
      const hostPool = newHostConnectionPoolInstance({ pooling: { maxRequestsPerConnection }});
      hostPool.coreConnectionsLength = 4;
      hostPool.connections = [
        { getInFlight: helper.functionOf(maxRequestsPerConnection) },
        { getInFlight: helper.functionOf(maxRequestsPerConnection) },
        { getInFlight: helper.functionOf(0) },
        { getInFlight: helper.functionOf(0) },
      ];
      const result = [];
      utils.times(8, (n, next) => {
        hostPool.createAndBorrowConnection(null, (err, c) => {
          result.push(c);
          next(err);
        });
      }, err => {
        if (err) {
          return done(err);
        }
        // Second and third connections should be selected
        const expectedConnections = hostPool.connections.slice(2);
        assert.strictEqual(8, result.filter(c => expectedConnections.indexOf(c) >= 0).length);
        done();
      });
    });
  });
  describe('#borrowConnection()', function () {
    it('should avoid returning the previous connection', done => {
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

      utils.times(8, (n, next) => {
        hostPool.borrowConnection(null, hostPool.connections[previousConnectionIndex], (err, c) => {
          result.set(c.index, (result.get(c.index) || 0) + 1);
          next(err);
        });
      }, err => {
        if (err) {
          return done(err);
        }
        assert.strictEqual(result.get(0), 2);
        assert.strictEqual(result.get(1), 2);
        assert.strictEqual(result.get(3), 4);
        assert.strictEqual(result.get(previousConnectionIndex), undefined);
        done();
      });
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
    it('should wait for creation before setting state to init', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      hostPool._createConnection = function () {
        return { open: helper.callbackNoop, close: helper.noop };
      };
      let created;
      let sync = true;
      hostPool.create(false, function (err) {
        assert.ok(err);
        created = sync !== true;
      });
      sync = false;
      hostPool.drainAndShutdown();
      hostPool.once('close', function () {
        assert.ok(created);
        done();
      });
    });
  });
  describe('#_attemptNewConnection()', function () {
    it('should create and attempt to open a connection', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      let openCalled = 0;
      const c = {
        open: function (cb) {
          openCalled++;
          setImmediate(cb);
        }
      };
      hostPool._createConnection = function () {
        return c;
      };
      hostPool._attemptNewConnection(utils.noop);
      setTimeout(function () {
        assert.strictEqual(1, openCalled);
        done();
      }, 50);
    });
    it('should callback in error when open fails', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      let openCalled = 0;
      let closeCalled = 0;
      const c = {
        open: function (cb) {
          openCalled++;
          setImmediate(function () {
            cb(new Error('test open err'));
          });
        },
        close: function () {
          closeCalled++;
        }
      };
      hostPool._createConnection = function () {
        return c;
      };
      hostPool._attemptNewConnection(function (err) {
        helper.assertInstanceOf(err, Error);
        assert.strictEqual(openCalled, 1);
        assert.strictEqual(closeCalled, 1);
        done();
      });
    });
    it('should callback when open succeeds', function (done) {
      const hostPool = newHostConnectionPoolInstance();
      const c = {
        open: function (cb) {
          setImmediate(cb);
        }
      };
      hostPool._createConnection = function () {
        return c;
      };
      hostPool._attemptNewConnection(function (err) {
        assert.ifError(err);
        done();
      });
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
  describe('constructor', function () {
    it('should listen for pool idleRequestError event', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      //should be marked as down
      host.on('down', done);
      const create = host.pool._createConnection.bind(host.pool);
      const c = create();
      host.pool._createConnection = function () {
        c.open = helper.callbackNoop;
        return c;
      };
      host.borrowConnection(null, null, function () {
        host.pool.connections[0].emit('idleRequestError', new Error('Test error'), c);
      });
    });
  });
  describe('#borrowConnection()', function () {
    const options = {
      pooling: {
        coreConnectionsPerHost: {}
      },
      policies: {
        reconnection: new reconnection.ConstantReconnectionPolicy(1)
      }
    };
    it('should get an open connection', function (done) {
      const host = newHostInstance(defaultOptions);
      const create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        const c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.borrowConnection(null, null, function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        //Only 1 connection should be created as the distance has not been set
        assert.equal(host.pool.connections.length, 1);
        host.shutdown(false);
        done();
      });
    });
    it('should trigger the creation of a pool of size determined by the distance', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.local] = 5;
      const host = newHostInstance(options);
      const create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        const c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      // setup the distance and expected connections for the host
      host.setDistance(types.distance.local);
      host.borrowConnection(null, null, function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        // initially just 1 connection
        assert.equal(host.pool.connections.length, 1);
        setTimeout(function () {
          // all connections should be created by now
          assert.equal(host.pool.connections.length, 5);
          host.shutdown(false);
          done();
        }, 100);
      });
    });
    it('should resize the pool after distance is set', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.local] = 3;
      const host = newHostInstance(options);
      const create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        const c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      utils.series([
        function (next) {
          host.borrowConnection(null, null, function (err, c) {
            assert.equal(err, null);
            assert.notEqual(c, null);
            //Just 1 connection at the beginning
            assert.equal(host.pool.connections.length, 1);
            next();
          });
        },
        function (next) {
          host.setDistance(types.distance.local);
          host.borrowConnection(null, null, function (err) {
            assert.ifError(err);
            //Pool resizing happen in the background
            setTimeout(next, 100);
          });
        },
        function (next) {
          //Check multiple times in parallel
          utils.times(10, function (n, timesNext) {
            host.borrowConnection(null, null, function (err, c) {
              assert.equal(err, null);
              assert.notEqual(c, null);
              //The right size afterwards
              assert.equal(host.pool.connections.length, 3);
              timesNext();
            });
          }, next);
        }
      ], err => {
        host.shutdown(false);
        done(err);
      });
    });
  });
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
    it('should create the exact amount of connections after borrowing when opening is instant', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = () => newConnectionMock({ open: helper.callbackNoop });
      host.borrowConnection(null, null, function (err) {
        assert.ifError(err);
        host.warmupPool(function (err) {
          assert.ifError(err);
          assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
          setTimeout(function () {
            assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
            done();
          }, 100);
        });
      });
    });
    it('should create the exact amount of connections after borrowing when opening takes some time', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = () => newConnectionMock({ open: (cb) => setTimeout(cb, 20)});
      host.borrowConnection(null, null, function (err) {
        assert.ifError(err);
        host.warmupPool(function (err) {
          assert.ifError(err);
          assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
          setTimeout(function () {
            assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
            done();
          }, 200);
        });
      });
    });
    it('should create the exact amount of connections when opening is instant', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = () => newConnectionMock({ open: helper.callbackNoop });
      host.warmupPool(function (err) {
        assert.ifError(err);
        assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
        setTimeout(function () {
          assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
          done();
        }, 100);
      });
    });
    it('should create the exact amount of connections when opening takes some time', function (done) {
      const host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = () => newConnectionMock({ open: (cb) => setTimeout(cb, 20)});
      host.warmupPool(function (err) {
        assert.ifError(err);
        assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
        setTimeout(function () {
          assert.strictEqual(host.pool.coreConnectionsLength, host.pool.connections.length);
          done();
        }, 200);
      });
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
    getInFlight: helper.functionOf(0)
  }, properties);
}