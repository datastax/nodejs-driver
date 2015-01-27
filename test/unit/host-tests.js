var assert = require('assert');
var async = require('async');
var util = require('util');
var events = require('events');
var rewire = require('rewire');

var hostModule = rewire('../../lib/host.js');
var Host = hostModule.Host;
var HostConnectionPool = hostModule.HostConnectionPool;
var types = require('../../lib/types');
var defaultOptions = require('../../lib/client-options').defaultOptions();
var utils = require('../../lib/utils.js');
var reconnection = require('../../lib/policies/reconnection.js');
var helper = require('../test-helper');
//Delay before connection.open callbacks
var openDelay = 10;

before(function () {
  //inject a mock Connection class
  var connectionMock = events.EventEmitter;
  connectionMock.prototype.open = function noop (cb) {
    var self = this;
    setTimeout(function () {
      self.connected = true;
      cb();
    }, openDelay);
  };
  hostModule.__set__("Connection", connectionMock);
});

describe('HostConnectionPool', function () {
  describe('#_maybeCreatePool()', function () {
    afterEach(function () {
      openDelay = 10;
    });
    it('should create the pool once', function (done) {
      var hostPool = newHostConnectionPoolInstance();
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
      openDelay = 800;
      var hostPool = newHostConnectionPoolInstance();
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
  });
  describe('#borrowConnection()', function () {
    it('should get an open connection', function (done) {
      var hostPool = newHostConnectionPoolInstance();
      hostPool.coreConnectionsLength = 10;
      hostPool.borrowConnection(function (err, c) {
        assert.equal(err, null);
        assert.notEqual(c, null);
        //its a connection or is a mock
        assert.ok(c.open instanceof Function);
        done();
      });
    });
  });
});
describe('Host', function () {
  describe('constructor', function () {
    it('should listen for pool idleRequestError event', function (done) {
      var host = newHostInstance(defaultOptions);
      host.pool.forceShutdown = helper.noop;
      //should be marked as down
      host.on('down', done);
      host.borrowConnection(function () {
        host.pool.connections[0].emit('idleRequestError');
      });
    });
  });
  describe('#borrowConnection()', function () {
    var options = {
      pooling: {
        coreConnectionsPerHost: {},
        maxConnectionsPerHost: {}
      },
      policies: {
        reconnection: new reconnection.ConstantReconnectionPolicy(1)
      }
    };
    it('should get an open connection', function (done) {
      var host = new Host('0.0.0.1', 2, options);
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
      options.pooling.maxConnectionsPerHost[types.distance.local] = 10;
      var host = newHostInstance(options);
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
      options.pooling.maxConnectionsPerHost[types.distance.remote] = 4;
      var host = newHostInstance(options);
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
      options.pooling.maxConnectionsPerHost[types.distance.local] = 4;
      var host = newHostInstance(options);
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
    });
  });
});

/**
 * @returns {HostConnectionPool}
 */
function newHostConnectionPoolInstance() {
  return new HostConnectionPool('0.0.0.1', 2, {logEmitter: function (){}});
}

/**
 * @returns {Host}
 */
function newHostInstance(options) {
  options = utils.extend({logEmitter: function () {}}, options);
  return new Host('0.0.0.1', 2, options);
}