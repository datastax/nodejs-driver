"use strict";
var assert = require('assert');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var Connection = require('../../../lib/connection');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var errors = require('../../../lib/errors');
var ExecutionProfile = require('../../../lib/execution-profile').ExecutionProfile;

describe('client read timeouts', function () {
  this.timeout(120000);
  beforeEach(helper.ccmHelper.start(2));
  afterEach(helper.ccmHelper.remove);
  describe('when socketOptions.readTimeout is not set', function () {
    it('should do nothing else than waiting', getTimeoutErrorExpectedTest(false, false));
    it('should use readTimeout when defined', getMoveNextHostTest(false, false, 0, { readTimeout: 3123 }));
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
      utils.series([
        client.connect.bind(client),
        function warmup(next) {
          utils.timesSeries(10, function (n, timesNext) {
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
          utils.times(10, function (n, timesNext) {
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
      utils.series([
        client.connect.bind(client),
        function warmup(next) {
          utils.times(10, function (n, timesNext) {
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
          utils.times(500, function (n, timesNext) {
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
      utils.series([
        client.connect.bind(client),
        function warmup(next) {
          utils.timesSeries(10, function (n, timesNext) {
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
          utils.times(10, function (n, timesNext) {
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
    function profiles() {
      return {
        'aProfile': new ExecutionProfile({readTimeout: 8675})
      };
    }
    it('should be used instead of socketOptions.readTimeout and profile.readTimeout for simple queries',
      getMoveNextHostTest(false, false, 3123, 1 << 24, { executionProfile: 'aProfile', readTimeout: 3123 }, profiles()));
    it('should be used instead of socketOptions.readTimeout and profile.readTimeout for prepared queries executions',
      getMoveNextHostTest(true, true, 3123, 1 << 24, { executionProfile: 'aProfile', readTimeout: 3123 }, profiles()));
    it('should suppress socketOptions.readTimeout and profile.readTimeout when set to 0 for simple queries',
      getTimeoutErrorExpectedTest(false, false, 3000, { executionProfile: 'aProfile', readTimeout: 0}, profiles()));
    it('should suppress socketOptions.readTimeout and profile.readTimeout  when set to 0 for prepared queries executions',
      getTimeoutErrorExpectedTest(true, true, 3000, { executionProfile: 'aProfile', readTimeout: 0}, profiles()));
  });
  describe('when executionProfile.readTimeout is set', function() {
    function timeoutProfiles() {
      return {
        'indefiniteTimeout' : new ExecutionProfile({readTimeout: 0}),
        'definedTimeout' : new ExecutionProfile({readTimeout: 3123})
      };
    }
    it('should be used instead of socketOptions.readTimeout for simple queries',
      getMoveNextHostTest(false, false, 3123, 1 << 24, { executionProfile: 'definedTimeout' }, timeoutProfiles()));
    it('should be used instead of socketOptions.readTimeout for prepared queries executions',
      getMoveNextHostTest(true, true, 3123, 1 << 24, { executionProfile: 'definedTimeout' }, timeoutProfiles()));
    it('should suppress socketOptions.readTimeout when set to 0 for simple queries',
      getTimeoutErrorExpectedTest(false, false, 3000, { executionProfile: 'indefiniteTimeout'}, timeoutProfiles()));
    it('should suppress socketOptions.readTimeout when set to 0 for prepared queries executions',
      getTimeoutErrorExpectedTest(true, true, 3000, { executionProfile: 'indefiniteTimeout'}, timeoutProfiles()));
  })
});


/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}

/**
 * @param {Boolean} prepare
 * @param {Boolean} prepareWarmup
 * @param {Number} expectedTimeoutMillis
 * @param {Number} [readTimeout]
 * @param {QueryOptions} [queryOptions]
 * @param {Object.<string, ExecutionProfile>} [profiles]
 * @returns {Function}
 */
function getMoveNextHostTest(prepare, prepareWarmup, expectedTimeoutMillis, readTimeout, queryOptions, profiles) {
  if (typeof readTimeout === 'undefined') {
    readTimeout = 3000;
  }
  profiles = profiles || {};
  return (function moveNextHostTest(done) {
    var client = newInstance({ profiles: profiles, socketOptions: { readTimeout: readTimeout } });
    var timeoutLogs = [];
    client.on('log', function (level, constructorName, info) {
      if (level !== 'warning' || info.indexOf('timeout') === -1) {
        return;
      }
      timeoutLogs.push(info);
    });
    var coordinators = {};
    utils.series([
      client.connect.bind(client),
      function warmup(next) {
        utils.timesSeries(10, function (n, timesNext) {
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
        var testAbortTimeout = setTimeout(function () {
          throw new Error('It should have been executed in the next (not paused) host.');
        }, expectedTimeoutMillis * 4);
        utils.times(10, function (n, timesNext) {
          queryOptions = utils.extend({ }, queryOptions, { prepare: prepare});
          client.execute('SELECT key FROM system.local', [], queryOptions, function (err, result) {
            if (err) return timesNext(err);
            coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
            timesNext();
          });
        }, function timeFinished(err) {
          clearTimeout(testAbortTimeout);
          if (err) return next(err);
          assert.strictEqual(Object.keys(coordinators).length, 1);
          assert.strictEqual(coordinators['1'], true);
          assert.ok(timeoutLogs.length);
          //check that the logs messages contains the actual millis value
          assert.ok(timeoutLogs.reduce(function (val, current) {
            return val || current.indexOf(expectedTimeoutMillis.toString()) >= 0;
          }, false), 'Timeout millis not found');
          next();
        });
      },
      helper.toTask(helper.ccmHelper.resumeNode, null, 2),
      client.shutdown.bind(client)
    ], done);
  });
}

function getTimeoutErrorExpectedTest(prepare, prepareWarmup, readTimeout, queryOptions, profiles) {
  if (typeof readTimeout === 'undefined') {
    readTimeout = 0;
  }

  profiles = profiles || {};

  return (function timeoutErrorExpectedTest(done) {
    var client = newInstance({ profiles: profiles, socketOptions: { readTimeout: readTimeout } });
    var coordinators = {};
    utils.series([
      client.connect.bind(client),
      function warmup(next) {
        utils.timesSeries(10, function (n, timesNext) {
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