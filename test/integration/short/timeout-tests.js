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
    it('should do nothing else than waiting', getTimeoutErrorExpectedTest(false, false));
    it('should use readTimeout when defined', getMoveNextHostTest(false, false, 0, { readTimeout: 3000 }));
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
          defunctReadTimeoutThreshold: 32
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
          async.times(500, function (n, timesNext) {
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
    it('should move to next host for eachRow() executions', function (done) {
      var client = newInstance({ socketOptions: { readTimeout: 3000 } });
      var coordinators = {};
      async.series([
        client.connect.bind(client),
        function warmup(next) {
          async.timesSeries(10, function (n, timesNext) {
            var counter = 0;
            client.eachRow('SELECT key FROM system.local', [], function () {
              counter++;
            }, function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              assert.strictEqual(result.rowLength, counter);
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
            var counter = 0;
            client.eachRow('SELECT key FROM system.local', [], function () {
              counter++;
            }, function (err, result) {
              if (err) return timesNext(err);
              coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
              assert.strictEqual(result.rowLength, counter);
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
  });
  describe('when queryOptions.readTimeout is set', function () {
    it('should be used instead of socketOptions.readTimeout for simple queries',
      getMoveNextHostTest(false, false, 1 << 24, { readTimeout: 3000 }));
    it('should be used instead of socketOptions.readTimeout for prepared queries executions',
      getMoveNextHostTest(true, true, 1 << 24, { readTimeout: 3000 }));
    it('should suppress socketOptions.readTimeout when set to 0 for simple queries',
      getTimeoutErrorExpectedTest(false, false, 3000, { readTimeout: 0}));
    it('should suppress socketOptions.readTimeout when set to 0 for prepared queries executions',
      getTimeoutErrorExpectedTest(true, true, 3000, { readTimeout: 0}));
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}

/**
 * @param {Boolean} prepare
 * @param {Boolean} prepareWarmup
 * @param {Number} [readTimeout]
 * @param {{readTimeout: number}} [queryOptions]
 * @returns {Function}
 */
function getMoveNextHostTest(prepare, prepareWarmup, readTimeout, queryOptions) {
  if (typeof readTimeout === 'undefined') {
    readTimeout = 3000;
  }
  var testAbortTimeout = readTimeout;
  if (queryOptions && queryOptions.readTimeout) {
    testAbortTimeout = queryOptions.readTimeout;
  }
  testAbortTimeout *= 4;
  return (function moveNextHostTest(done) {
    var client = newInstance({ socketOptions: { readTimeout: readTimeout } });
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
        var testTimeout = setTimeout(function () {
          throw new Error('It should have been executed in the next (not paused) host.');
        }, testAbortTimeout);
        async.times(10, function (n, timesNext) {
          queryOptions = utils.extend({ }, queryOptions, { prepare: prepare});
          client.execute('SELECT key FROM system.local', [], queryOptions, function (err, result) {
            if (err) return timesNext(err);
            coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
            timesNext();
          });
        }, function timeFinished(err) {
          clearTimeout(testTimeout);
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

function getTimeoutErrorExpectedTest(prepare, prepareWarmup, readTimeout, queryOptions) {
  if (typeof readTimeout === 'undefined') {
    readTimeout = 0;
  }

  return (function timeoutErrorExpectedTest(done) {
    var client = newInstance({ socketOptions: { readTimeout: readTimeout } });
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
        //execute 2 queries without waiting for the response
        for (var i = 0; i < 2; i++) {
          queryOptions = utils.extend({ }, queryOptions, { prepare: prepare });
          client.execute('SELECT key FROM system.local', [], queryOptions, function (err, result) {
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
}