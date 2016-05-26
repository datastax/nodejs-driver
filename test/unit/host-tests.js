"use strict";
var assert = require('assert');
var util = require('util');
var events = require('events');

var hostModule = require('../../lib/host');
var Host = hostModule.Host;
var HostConnectionPool = require('../../lib/host-connection-pool');
var HostMap = hostModule.HostMap;
var types = require('../../lib/types');
var clientOptions = require('../../lib/client-options');
var defaultOptions = clientOptions.defaultOptions();
defaultOptions.pooling.coreConnectionsPerHost = clientOptions.coreConnectionsPerHostV3;
var utils = require('../../lib/utils.js');
var policies = require('../../lib/policies');
var helper = require('../test-helper');
var reconnection = policies.reconnection;

describe('HostConnectionPool', function () {
  this.timeout(5000);
  describe('#create()', function () {
    it('should create the pool once', function (done) {
      var hostPool = newHostConnectionPoolInstance( { pooling: { warmup: true }} );
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
      var hostPool = newHostConnectionPoolInstance();
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
            var closedConnections = hostPool.connections.filter(function (x) {return !x.connected}).length;
            if (closedConnections)
            {
              return next(new Error('All connections should be opened: ' + closedConnections + ' closed'))
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
      var hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 1;
      hostPool._createConnection = function () {
        return {
          open: function (cb) {
            this.connected = true;
            setTimeout(cb, 50);
          }
        };
      };
      var counter = 0;
      utils.timesLimit(10, 4, function(n, next) {
        counter++;
        hostPool.create(false, function (err) {
          setImmediate(function () {
            assert.ifError(err);
            var closedConnections = hostPool.connections.filter(function (x) {return !x.connected}).length;
            if (closedConnections) {
              return next(new Error('All connections should be opened: ' + closedConnections + ' closed'))
            }
            if (counter > 5) {
              hostPool.coreConnectionsLength = 15;
            }
            next();
          })
        });
      }, done);
    });
  });
  describe('#borrowConnection()', function () {
    it('should get an open connection', function (done) {
      var hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 10;
      hostPool._createConnection = function () {
        return {
          open: function (cb) {
            this.connected = true;
            setTimeout(cb, 30);
          },
          getInFlight: function () { return 0; }
        };
      };
      hostPool.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        done();
      });
    });
  });
  describe('#_attemptNewConnection()', function () {
    it('should create and attempt to open a connection', function (done) {
      var hostPool = newHostConnectionPoolInstance();
      var openCalled = 0;
      var c = {
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
      var hostPool = newHostConnectionPoolInstance();
      var openCalled = 0;
      var closeCalled = 0;
      var c = {
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
      var hostPool = newHostConnectionPoolInstance();
      var openCalled = 0;
      var c = {
        open: function (cb) {
          openCalled++;
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
    it('should round robin with between connections with the same amount of in-flight requests', function () {
      /** @type {Array.<Connection>} */
      var connections = [];
      for (var i = 0; i < 3; i++) {
        //noinspection JSCheckFunctionSignatures
        connections.push({ getInFlight: function () { return 0; }, index: i});
      }
      var initial = HostConnectionPool.minInFlight(connections).index;
      for (i = 1; i < 10; i++) {
        assert.strictEqual((initial + i) % connections.length, HostConnectionPool.minInFlight(connections).index);
      }
    });
  });
});
describe('Host', function () {
  describe('constructor', function () {
    it('should listen for pool idleRequestError event', function (done) {
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      //should be marked as down
      host.on('down', done);
      var create = host.pool._createConnection.bind(host.pool);
      var c = create();
      host.pool._createConnection = function () {
        c.open = helper.callbackNoop;
        return c;
      };
      host.borrowConnection(function () {
        host.pool.connections[0].emit('idleRequestError', new Error('Test error'), c);
      });
    });
  });
  describe('#borrowConnection()', function () {
    var options = {
      pooling: {
        coreConnectionsPerHost: {}
      },
      policies: {
        reconnection: new reconnection.ConstantReconnectionPolicy(1)
      }
    };
    it('should get an open connection', function (done) {
      var host = newHostInstance(defaultOptions);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        //Only 1 connection should be created as the distance has not been set
        assert.equal(host.pool.connections.length, 1);
        done();
      });
    });
    it('should trigger the creation of a pool of size determined by the distance', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.local] = 5;
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      // setup the distance and expected connections for the host
      host.setDistance(types.distance.local);
      host.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        // initially just 1 connection
        assert.equal(host.pool.connections.length, 1);
        setTimeout(function () {
          // all connections should be created by now
          assert.equal(host.pool.connections.length, 5);
          done();
        }, 100);
      });
    });
    it('should resize the pool after distance is set', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.local] = 3;
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      utils.series([
        function (next) {
          host.borrowConnection(function (err, c) {
            assert.equal(err, null);
            assert.notEqual(c, null);
            //Just 1 connection at the beginning
            assert.equal(host.pool.connections.length, 1);
            next();
          });
        },
        function (next) {
          host.setDistance(types.distance.local);
          host.borrowConnection(function (err) {
            assert.ifError(err);
            //Pool resizing happen in the background
            setTimeout(next, 100);
          });
        },
        function (next) {
          //Check multiple times in parallel
          utils.times(10, function (n, timesNext) {
            host.borrowConnection(function (err, c) {
              assert.equal(err, null);
              assert.notEqual(c, null);
              //The right size afterwards
              assert.equal(host.pool.connections.length, 3);
              timesNext();
            });
          }, next);
        }
      ], done);
    });
  });
  describe('#setUp()', function () {
    it('should reset the reconnection schedule when bring it up', function () {
      var maxDelay = 1000;
      var options = utils.extend({
        policies: {
          reconnection: new reconnection.ExponentialReconnectionPolicy(50, maxDelay, false)
        }}, defaultOptions);
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      var initialSchedule = options.policies.reconnection.newSchedule();
      host.reconnectionSchedule = initialSchedule;
      host.setDownAt = 1;
      host.setUp();
      assert.notStrictEqual(host.reconnectionSchedule, initialSchedule);
    });
  });
  describe('#setDown()', function () {
    it('should emit event when called', function (done) {
      var host = newHostInstance(defaultOptions);
      host.on('down', done);
      host.setDown();
      host.shutdown(false);
    });
  });
  describe('#getActiveConnection()', function () {
    it('should return null if a the pool is initialized', function () {
      var h = newHostInstance(defaultOptions);
      assert.strictEqual(h.getActiveConnection(), null);
    });
  });
  describe('#getDistance()', function () {
    it('should call checkIsUp() when the new distance is local and was down', function () {
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.ignored;
      host.setDownAt = 1;
      var checkIsUpCalled = 0;
      host.checkIsUp = function () { checkIsUpCalled++; };
      host.setDistance(types.distance.local);
      assert.strictEqual(checkIsUpCalled, 1);
    });
    it('should call drainAndShutdown() when the new distance is ignored', function () {
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      var drainAndShutdownCalled = 0;
      host.pool.drainAndShutdown = function () { drainAndShutdownCalled++; };
      host.setDistance(types.distance.ignored);
      assert.strictEqual(drainAndShutdownCalled, 1);
      assert.strictEqual(host.pool.coreConnectionsLength, 0);
    });
    it('should not call drainAndShutdown() when the new distance is ignored and was previously ignored', function () {
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.ignored;
      var drainAndShutdownCalled = 0;
      host.pool.drainAndShutdown = function () { drainAndShutdownCalled++; };
      host.setDistance(types.distance.ignored);
      assert.strictEqual(drainAndShutdownCalled, 0);
    });
  });
  describe('#removeFromPool()', function () {
    it('should remove the connection in a new array instance', function () {
      var host = newHostInstance(defaultOptions);
      var initialConnections = [ newConnectionMock(), newConnectionMock() ];
      host.pool.connections = initialConnections;
      host.removeFromPool(initialConnections[0]);
      assert.deepEqual(host.pool.connections, [ initialConnections[1] ]);
      assert.notStrictEqual(host.pool.connections, initialConnections);
      host.shutdown(false);
    });
    it('should issue a new connection attempt when pool size is smaller than config', function () {
      var host = newHostInstance(defaultOptions);
      var initialConnections = [ newConnectionMock(), newConnectionMock() ];
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
      var host = newHostInstance(defaultOptions);
      var initialConnections = [ newConnectionMock()];
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
      var host = newHostInstance(defaultOptions);
      var initialConnections = [ newConnectionMock()];
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
      var host = newHostInstance(defaultOptions);
      var closeInvoked = 0;
      var c = {
        timedOutHandlers: 1000,
        close: function () {
          closeInvoked++;
        }
      };
      var initialConnections = [ newConnectionMock(), newConnectionMock(), c];
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
      var host = newHostInstance(defaultOptions);
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
      var host = newHostInstance(defaultOptions);
      host.setDownAt = 1;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.ok(host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
    it('should reset the reconnection schedule and set the delay to 0', function () {
      var host = newHostInstance(defaultOptions);
      host.setDownAt = 1;
      host.reconnectionDelay = 1;
      var reconnectionSchedule = host.reconnectionSchedule;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.notStrictEqual(host.reconnectionSchedule, reconnectionSchedule);
      assert.strictEqual(host.reconnectionDelay, 0);
      assert.ok(host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
    it('should not issue a connection attempt if host is UP', function () {
      var host = newHostInstance(defaultOptions);
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.checkIsUp();
      assert.ok(!host.pool.hasScheduledNewConnection());
    });
    it('should schedule new connection attempt after previous shutdown finished', function () {
      var host = newHostInstance(defaultOptions);
      host.setDownAt = 1;
      assert.ok(!host.pool.hasScheduledNewConnection());
      host.pool.shuttingDown = true;
      host.checkIsUp();
      assert.ok(!host.pool.hasScheduledNewConnection());
      // emit the pool has been shutdown
      host.pool.shuttingDown = false;
      host.pool.emit('shutdown');
      assert.ok(host.pool.hasScheduledNewConnection());
      host.shutdown(false);
    });
  });
  describe('#warmupPool()', function () {
    it('should create the exact amount of connections after borrowing when opening is instant', function (done) {
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = function () {
        return { open: helper.callbackNoop };
      };
      host.borrowConnection(function (err) {
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
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = function () {
        return ({ open: function open(cb) {
          setTimeout(cb, 20);
        }});
      };
      host.borrowConnection(function (err) {
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
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 4;
      host.pool._createConnection = function () {
        return { open: helper.callbackNoop };
      };
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
      var host = newHostInstance(defaultOptions);
      host._distance = types.distance.local;
      host.pool.coreConnectionsLength = 3;
      host.pool._createConnection = function () {
        return ({ open: function open(cb) {
          setTimeout(cb, 20);
        }});
      };
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
      var map = new HostMap();
      //noinspection JSCheckFunctionSignatures
      map.set('h1', 'h1');
      var values = map.values();
      assert.strictEqual(values.length, 1);
      assert.ok(Object.isFrozen(values));
    });
    it('should return the same instance as long as the value does not change', function () {
      var map = new HostMap();
      //noinspection JSCheckFunctionSignatures
      map.set('h1', 'h1');
      var values1 = map.values();
      var values2 = map.values();
      assert.strictEqual(values1, values2);
      //noinspection JSCheckFunctionSignatures
      map.set('h2', 'h2');
      var values3 = map.values();
      assert.strictEqual(values3.length, 2);
      assert.notEqual(values3, values1);
    });
  });
  describe('#set()', function () {
    it('should modify the cached values', function () {
      var map = new HostMap();
      //noinspection JSCheckFunctionSignatures
      map.set('h1', 'v1');
      var values = map.values();
      assert.strictEqual(util.inspect(values), util.inspect(['v1']));
      //noinspection JSCheckFunctionSignatures
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
  return new Host('0.0.0.1:9042', 2, options);
}

/**
 * @returns {Connection}
 */
function newConnectionMock() {
  //noinspection JSValidateTypes
  return ({
    close: helper.noop
  });
}