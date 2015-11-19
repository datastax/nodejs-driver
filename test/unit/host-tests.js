"use strict";
var assert = require('assert');
var async = require('async');
var util = require('util');
var events = require('events');

var hostModule = require('../../lib/host');
var Host = hostModule.Host;
var HostConnectionPool = hostModule.HostConnectionPool;
var HostMap = hostModule.HostMap;
var types = require('../../lib/types');
var defaultOptions = require('../../lib/client-options').defaultOptions();
var utils = require('../../lib/utils.js');
var reconnection = require('../../lib/policies/reconnection');
var helper = require('../test-helper');

describe('HostConnectionPool', function () {
  this.timeout(5000);
  describe('#_maybeCreatePool()', function () {
    it('should create the pool once', function (done) {
      var hostPool = newHostConnectionPoolInstance();
      hostPool._createConnection = function () {
        return { open: function (cb) {
          setTimeout(cb, 30);
        }};
      };
      hostPool.coreConnectionsLength = 10;
      async.times(5, function (n, next) {
        //even though it is called multiple times in parallel
        //it should only create a pool with 10 connections
        hostPool._maybeCreatePool(function (err) {
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
      async.times(5, function(n, next) {
        setTimeout(function () {
          hostPool._maybeCreatePool(function (err) {
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
      async.timesLimit(10, 4, function(n, next) {
        counter++;
        hostPool._maybeCreatePool(function (err) {
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
  describe('#checkHealth()', function () {
    it('should remove remove connection from Array and invoke close', function (done) {
      var hostPool = newHostConnectionPoolInstance();
      var closeInvoked = 0;
      var c = {
        timedOutHandlers: 1000,
        close: function () {
          closeInvoked++;
        }
      };
      hostPool.connections = ['a', 'b', c];
      hostPool.checkHealth(c);
      setImmediate(function () {
        assert.strictEqual(1, closeInvoked);
        assert.deepEqual(hostPool.connections, ['a', 'b']);
        done();
      });
    });
  });
  describe('#_attemptReconnection()', function () {
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
      hostPool._attemptReconnection();
      setTimeout(function () {
        assert.strictEqual(1, openCalled);
        done();
      }, 50);
    });
    describe('when open fails', function () {
      it('should call Host#setDown() and Connection#close()', function (done) {
        var hostPool = newHostConnectionPoolInstance();
        var openCalled = 0;
        var closeCalled = 0;
        var setDownCalled = 0;
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
        hostPool.host.setDown = function () {
          setDownCalled++;
        };
        hostPool._createConnection = function () {
          return c;
        };
        hostPool._attemptReconnection();
        setTimeout(function () {
          assert.strictEqual(1, openCalled);
          assert.strictEqual(1, closeCalled);
          assert.strictEqual(1, setDownCalled);
          done();
        }, 50);
      });
    });
    describe('when open succeeds', function () {
      it('should call Host#setUp()', function (done) {
        var hostPool = newHostConnectionPoolInstance();
        var openCalled = 0;
        var setUpCalled = 0;
        var c = {
          open: function (cb) {
            openCalled++;
            setImmediate(cb);
          }
        };
        hostPool.host.setUp = function () {
          setUpCalled++;
        };
        hostPool._createConnection = function () {
          return c;
        };
        hostPool._attemptReconnection();
        setTimeout(function () {
          assert.strictEqual(1, openCalled);
          assert.strictEqual(1, setUpCalled);
          assert.strictEqual(1, hostPool.connections.length);
          assert.strictEqual(c, hostPool.connections[0]);
          done();
        }, 50);
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
      host.pool.scheduleReconnection = helper.noop;
      //should be marked as down
      host.on('down', done);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.borrowConnection(function () {
        host.pool.connections[0].emit('idleRequestError');
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
      var host = new Host('0.0.0.1:9042', 2, options);
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
    it('should create a pool of size determined by the relative distance local', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.local] = 5;
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.setDistance(types.distance.local);
      host.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        assert.equal(host.pool.connections.length, 5);
        done();
      });
    });
    it('should create a pool of size determined by the relative distance remote', function (done) {
      options.pooling.coreConnectionsPerHost[types.distance.remote] = 2;
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.setDistance(types.distance.remote);
      host.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        assert.equal(host.pool.connections.length, 2);
        done();
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
      async.series([
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
            //Pool resizing can happen in the background
            setImmediate(next);
          });
        },
        function (next) {
          //Check multiple times in parallel
          async.times(10, function (n, timesNext) {
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
    //Use a test policy that starts at zero to be easier to track down
    var maxDelay = 1000;

    var options = {
      policies: {
        reconnection: new reconnection.ExponentialReconnectionPolicy(100, maxDelay, true)
      }};
    it('should reset the reconnection schedule when bring it up', function () {
      var host = newHostInstance(options);
      var create = host.pool._createConnection.bind(host.pool);
      host.pool._createConnection = function () {
        var c = create();
        c.open = helper.callbackNoop;
        return c;
      };
      host.setDown();
      //start at zero
      assert.strictEqual(host.reconnectionDelay, 0);
      //Force to be considered as up
      host.unhealthyAt = 1;
      assert.ok(host.canBeConsideredAsUp());
      host.setDown();
      host.unhealthyAt = 1;
      host.setDown();
      host.unhealthyAt = 1;
      host.setDown();
      assert.ok(host.reconnectionDelay > 0);
      host.unhealthyAt = 1;
      host.setDown();
      host.unhealthyAt = 1;
      host.setDown();
      //hitting max
      assert.strictEqual(host.reconnectionDelay, maxDelay);
      host.unhealthyAt = 1;
      host.setDown();
      assert.strictEqual(host.reconnectionDelay, maxDelay);

      //BRING IT UP!
      host.setUp();
      //Oh no, DOWN again :)
      host.setDown();
      //restart at zero
      assert.strictEqual(host.reconnectionDelay, 0);
      host.shutdown(helper.noop);
    });
  });
  describe('#setDown()', function () {
    var options = {
      policies: {
        reconnection: new reconnection.ConstantReconnectionPolicy(100)
      }};
    it('should emit event when called', function (done) {
      var host = newHostInstance(options);
      host.on('down', done);
      host.setDown();
      host.shutdown(helper.noop);
    });
  });
  describe('#getActiveConnection()', function () {
    it('should return null if a the pool is initialized', function () {
      var options = {
        policies: {
          reconnection: new reconnection.ConstantReconnectionPolicy(100)
        }};
      var h = newHostInstance(options);
      assert.strictEqual(h.getActiveConnection(), null);
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
 * @returns {Host}
 */
function newHostInstance(options) {
  options = utils.extend({logEmitter: function () {}}, options);
  return new Host('0.0.0.1:9042', 2, options);
}