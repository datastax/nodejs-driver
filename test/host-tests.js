var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var hostModule = rewire('../lib/host.js');
var Host = hostModule.Host;
var HostConnectionPool = hostModule.HostConnectionPool;
var types = require('../lib/types.js');

describe('HostConnectionPool', function () {
  before(function () {
    //inject a mock Connection class
    var connectionMock = function () {};
    connectionMock.prototype.open = function noop (cb) {cb()};
    hostModule.__set__("Connection", connectionMock);
  });

  describe('#maybeCreatePool()', function () {
    it('should create the pool once', function (done) {
      var host = new Host('127.0.0.1');
      var options = { poolOptions: { coreConnections: {} }};
      options.poolOptions.coreConnections[types.distance.local] = 10;
      var hostPool = new HostConnectionPool(host, types.distance.local, 2, options);
      async.times(5, function (n, next) {
        //even though it is called multiple times in parallel
        //it should only create a pool with 10 connections
        hostPool.maybeCreatePool(function (err) {
          assert.equal(err, null);
          assert.strictEqual(hostPool.connections.length, 10);
          next();
        });
      }, done);
    });
  });

  describe('#borrowConnection()', function () {
    it('should get an open connection', function (done) {
      var host = new Host('127.0.0.1');
      var options = { poolOptions: { coreConnections: {} }};
      options.poolOptions.coreConnections[types.distance.local] = 10;
      var hostPool = new HostConnectionPool(host, types.distance.local, 2, options);
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