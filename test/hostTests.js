var assert = require('assert');
var async = require('async');
var rewire = require('rewire');
var hostModule = rewire('../lib/host.js');
var Host = hostModule.Host;
var HostConnectionPool = hostModule.HostConnectionPool;
var types = require('../lib/types.js');

describe('HostConnectionPool', function () {
  describe('#maybeCreatePool()', function () {
    before(function () {
      //inject a mock Connection class
      var connectionMock = function () {};
      connectionMock.prototype.open = function noop (cb) {cb()};
      hostModule.__set__("Connection", connectionMock);
    });

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
          assert.equal(hostPool.connections.length, 10);
          next();
        });
      }, done);
    });
  });
});