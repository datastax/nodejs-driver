"use strict";
var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var Connection = require('../../../lib/connection');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var errors = require('../../../lib/errors');

describe('client read timeouts', function () {
  this.timeout(120000);
  beforeEach(helper.ccmHelper.start(2));
  afterEach(helper.ccmHelper.remove);
  describe('when socketOptions.readTimeout is not set', function () {
    it('should do nothing else than waiting', function (done) {
      //set readTimeout to 0
      var client = newInstance({ socketOptions: { readTimeout: 0 } });
      var coordinators = {};
      async.series([
        client.connect.bind(client),
        function warmup(next) {
          async.timesSeries(10, function (n, timesNext) {
            client.execute('SELECT key FROM system.local', function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              timesNext();
            });
          }, next);
        },
        helper.toTask(helper.ccmHelper.pauseNode, null, 2),
        function checkTimeouts(next) {
          assert.strictEqual(Object.keys(coordinators).length, 2);
          assert.strictEqual(coordinators['1'], true);
          assert.strictEqual(coordinators['2'], true);
          coordinators = {};
          //execute 2 queries without waiting for the response
          for (var i = 0; i < 2; i++) {
            client.execute('SELECT key FROM system.local', function (err, result) {
              assert.ifError(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
            });
          }
          //wait for the node that is healthy to respond
          setTimeout(next, 2000);
        },
        function checkWhenPaused(next) {
          //the other callback is still waiting
          assert.strictEqual(Object.keys(coordinators).length, 1);
          assert.strictEqual(coordinators['1'], true);
          next();
        },
        helper.toTask(helper.ccmHelper.resumeNode, null, 2),
        function waitForResponse(next) {
          // Wait for 2 seconds after resume for node to respond.
          setTimeout(next, 2000);
        },
        function checkAfterResuming(next) {
          assert.strictEqual(Object.keys(coordinators).length, 2);
          assert.strictEqual(coordinators['1'], true);
          assert.strictEqual(coordinators['2'], true);
          next();
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('when socketOptions.readTimeout is set', function () {
    it('should move to next host by default for simple queries', getMoveNextHostTest(false, false));
    it('should move to next host for prepared queries executions', getMoveNextHostTest(true, true));
    it('should move to next host for prepared requests', getMoveNextHostTest(true, false));
    it('should move to next host for the initial prepare', getMoveNextHostTest(true, false));
    it('should callback in error when retryOnTimeout is false', function (done) {
      var client = newInstance({ socketOptions: { readTimeout: 3000 } });
      var coordinators = {};
      var errorsReceived = [];
      async.series([
        client.connect.bind(client),
        function warmup(next) {
          async.timesSeries(10, function (n, timesNext) {
            client.execute('SELECT key FROM system.local', function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              timesNext();
            });
          }, next);
        },
        helper.toTask(helper.ccmHelper.pauseNode, null, 2),
        function checkTimeouts(next) {
          assert.strictEqual(Object.keys(coordinators).length, 2);
          assert.strictEqual(coordinators['1'], true);
          assert.strictEqual(coordinators['2'], true);
          coordinators = {};
          async.times(10, function (n, timesNext) {
            client.execute('SELECT key FROM system.local', [], { retryOnTimeout: false }, function (err, result) {
              if (err) {
                errorsReceived.push(err);
              }
              else {
                coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              }
              timesNext();
            });
          }, function (err) {
            if (err) return next(err);
            assert.strictEqual(Object.keys(coordinators).length, 1);
            assert.strictEqual(coordinators['1'], true);
            //half of the executions failed
            assert.strictEqual(errorsReceived.length, 5);
            assert.ok(errorsReceived.reduce(function (previous, current) {
              return previous && (current instanceof errors.OperationTimedOutError);
            }, true));
            next();
          });
        },
        helper.toTask(helper.ccmHelper.resumeNode, null, 2),
        client.shutdown.bind(client)
      ], done);
    });
    it('defunct the connection when the threshold passed', function (done) {
      var client = newInstance({
        socketOptions: {
          readTimeout: 3000,
          defunctReadTimeoutThreshold: 16
        },
        //1 connection per host to simply it
        poolingOptions: {
          coreConnectionsPerHost: {
            '0': 1,
            '1': 1,
            '2': 0
          }
        }
      });
      var coordinators = {};
      var connection;
      async.series([
        client.connect.bind(client),
        function warmup(next) {
          async.times(10, function (n, timesNext) {
            client.execute('SELECT key FROM system.local', function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              timesNext();
            });
          }, next);
        },
        function identifyConnection(next) {
          connection = client.hosts.values()
            .filter(function (h) {
              return helper.lastOctetOf(h) === '2';
            })[0]
            .pool
            .connections[0];
          helper.assertInstanceOf(connection, Connection);
          assert.strictEqual(connection.netClient.writable, true);
          next();
        },
        helper.toTask(helper.ccmHelper.pauseNode, null, 2),
        function checkTimeouts(next) {
          assert.strictEqual(Object.keys(coordinators).length, 2);
          assert.strictEqual(coordinators['1'], true);
          assert.strictEqual(coordinators['2'], true);
          coordinators = {};
          async.times(34, function (n, timesNext) {
            client.execute('SELECT key FROM system.local', function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              timesNext();
            });
          }, function (err) {
            if (err) return next(err);
            assert.strictEqual(Object.keys(coordinators).length, 1);
            assert.strictEqual(coordinators['1'], true);
            //we check using an internal property of socket which is lame
            //and might break
            assert.strictEqual(connection.netClient.writable, false);
            next();
          });
        },
        helper.toTask(helper.ccmHelper.resumeNode, null, 2),
        client.shutdown.bind(client)
      ], done);
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}

function getMoveNextHostTest(prepare, prepareWarmup) {
  return (function (done) {
    var client = newInstance({ socketOptions: { readTimeout: 3000 } });
    var coordinators = {};
    async.series([
      client.connect.bind(client),
      function warmup(next) {
        async.timesSeries(10, function (n, timesNext) {
          client.execute('SELECT key FROM system.local', [], { prepare: prepareWarmup }, function (err, result) {
            if (err) return timesNext(err);
            coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
            timesNext();
          });
        }, next);
      },
      helper.toTask(helper.ccmHelper.pauseNode, null, 2),
      function checkTimeouts(next) {
        assert.strictEqual(Object.keys(coordinators).length, 2);
        assert.strictEqual(coordinators['1'], true);
        assert.strictEqual(coordinators['2'], true);
        coordinators = {};
        async.times(10, function (n, timesNext) {
          client.execute('SELECT key FROM system.local', [], { prepare: prepare }, function (err, result) {
            if (err) return timesNext(err);
            coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
            timesNext();
          });
        }, function (err) {
          if (err) return next(err);
          assert.strictEqual(Object.keys(coordinators).length, 1);
          assert.strictEqual(coordinators['1'], true);
          next();
        });
      },
      helper.toTask(helper.ccmHelper.resumeNode, null, 2),
      client.shutdown.bind(client)
    ], done);
  });
}