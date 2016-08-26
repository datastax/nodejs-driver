"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var Connection = require('../../../lib/connection');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var errors = require('../../../lib/errors');
var ExecutionProfile = require('../../../lib/execution-profile').ExecutionProfile;
var loadBalancing = require('../../../lib/policies').loadBalancing;
var vdescribe = helper.vdescribe;

describe('client read timeouts', function () {
  this.timeout(120000);
  before(helper.ccmHelper.start(2));
  after(helper.ccmHelper.remove);
  afterEach(function (done) {
    // Tests will pause any of the nodes and should resume it in the general case, but if for whatever reason they fail
    // resuming the nodes should be safe.
    utils.series([
      helper.toTask(helper.ccmHelper.resumeNode, null, 1),
      helper.toTask(helper.ccmHelper.resumeNode, null, 2)
    ], done);
  });
  describe('when socketOptions.readTimeout is not set', function () {
    it('should do nothing else than waiting', getTimeoutErrorNotExpectedTest(false, false));
    it('should use readTimeout when defined', getMoveNextHostTest(false, false, 3123, 0, { readTimeout: 3123 }));
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
      return [
        new ExecutionProfile('aProfile', {readTimeout: 8675})
      ];
    }
    it('should be used instead of socketOptions.readTimeout and profile.readTimeout for simple queries',
      getMoveNextHostTest(false, false, 3123, 1 << 24, { executionProfile: 'aProfile', readTimeout: 3123 }, profiles()));
    it('should be used instead of socketOptions.readTimeout and profile.readTimeout for prepared queries executions',
      getMoveNextHostTest(true, true, 3123, 1 << 24, { executionProfile: 'aProfile', readTimeout: 3123 }, profiles()));
    it('should suppress socketOptions.readTimeout and profile.readTimeout when set to 0 for simple queries',
      getTimeoutErrorNotExpectedTest(false, false, 1000, { executionProfile: 'aProfile', readTimeout: 0}, profiles()));
    it('should suppress socketOptions.readTimeout and profile.readTimeout when set to 0 for prepared queries executions',
      getTimeoutErrorNotExpectedTest(true, true, 1000, { executionProfile: 'aProfile', readTimeout: 0}, profiles()));
  });
  describe('when executionProfile.readTimeout is set', function() {
    function timeoutProfiles() {
      return [
        new ExecutionProfile('indefiniteTimeout', {readTimeout: 0}),
        new ExecutionProfile('definedTimeout', {readTimeout: 3123})
      ];
    }
    it('should be used instead of socketOptions.readTimeout for simple queries',
      getMoveNextHostTest(false, false, 3123, 1 << 24, { executionProfile: 'definedTimeout' }, timeoutProfiles()));
    it('should be used instead of socketOptions.readTimeout for prepared queries executions',
      getMoveNextHostTest(true, true, 3123, 1 << 24, { executionProfile: 'definedTimeout' }, timeoutProfiles()));
    it('should suppress socketOptions.readTimeout when set to 0 for simple queries',
      getTimeoutErrorNotExpectedTest(false, false, 1000, { executionProfile: 'indefiniteTimeout'}, timeoutProfiles()));
    it('should suppress socketOptions.readTimeout when set to 0 for prepared queries executions',
      getTimeoutErrorNotExpectedTest(true, true, 1000, { executionProfile: 'indefiniteTimeout'}, timeoutProfiles()));
  });
  vdescribe('2.0', 'with prepared batches', function () {
    it('should retry when preparing multiple queries', function (done) {
      var client = newInstance({
        // Use a lbp that always yields the hosts in the same order
        policies: { loadBalancing: new FixedOrderLoadBalancingPolicy() },
        pooling: { warmup: true, coreConnectionsPerHost: { '0': 1 }},
        socketOptions: { readTimeout: 1000 },
        queryOptions: { consistency: types.consistencies.one }
      });
      var ksName = 'ks_batch_test1';
      var tableName = ksName + '.table1';
      utils.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, helper.createKeyspaceCql(ksName, 2)),
        helper.toTask(client.execute, client, helper.createTableCql(tableName)),
        helper.toTask(helper.ccmHelper.pauseNode, null, 1),
        function checkPreparing(next) {
          var queries = [{
            query: util.format("INSERT INTO %s (id, text_sample) VALUES (?, ?)", tableName),
            params: [types.Uuid.random(), 'one']
          }, {
            query: util.format("INSERT INTO %s (id, int_sample) VALUES (?, ?)", tableName),
            params: [types.Uuid.random(), 2]
          }];
          // It should be retried on the next node
          client.batch(queries, { prepare: true, logged: false }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(helper.lastOctetOf(result.info.queriedHost), '2');
            next();
          });
        },
        helper.toTask(helper.ccmHelper.resumeNode, null, 1),
        client.shutdown.bind(client)
      ], done);
    });
    it('should produce a NoHostAvailableError when prepare tried and timed out on all hosts', function (done) {
      var client = newInstance({
        // Use a lbp that always yields the hosts in the same order
        policies: { loadBalancing: new FixedOrderLoadBalancingPolicy() },
        pooling: { warmup: true, coreConnectionsPerHost: { '0': 1 }},
        socketOptions: { readTimeout: 1000 },
        queryOptions: { consistency: types.consistencies.one }
      });
      var ksName = 'ks_batch_test2';
      var tableName = ksName + '.table1';
      utils.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, helper.createKeyspaceCql(ksName, 2)),
        helper.toTask(client.execute, client, helper.createTableCql(tableName)),
        helper.toTask(helper.ccmHelper.pauseNode, null, 1),
        helper.toTask(helper.ccmHelper.pauseNode, null, 2),
        function checkPreparing(next) {
          var queries = [{
            query: util.format("INSERT INTO %s (id, text_sample) VALUES (?, ?)", tableName),
            params: [types.Uuid.random(), 'one']
          }, {
            query: util.format("INSERT INTO %s (id, int_sample) VALUES (?, ?)", tableName),
            params: [types.Uuid.random(), 2]
          }];
          // It should be tried on all nodes and produce a NoHostAvailableError.
          client.batch(queries, { prepare: true, logged: false }, function (err) {
            helper.assertInstanceOf(err, errors.NoHostAvailableError);
            //noinspection JSUnresolvedVariable
            var numErrors = Object.keys(err.innerErrors).length;
            assert.strictEqual(numErrors, 2);
            next();
          });
        },
        helper.toTask(helper.ccmHelper.resumeNode, null, 1),
        helper.toTask(helper.ccmHelper.resumeNode, null, 2),
        client.shutdown.bind(client)
      ], done);
    });
  });
});


/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}

/**
 * @param {Boolean} prepare
 * @param {Boolean} prepareWarmup
 * @param {Number} [expectedTimeoutMillis]
 * @param {Number} [readTimeout]
 * @param {QueryOptions} [queryOptions]
 * @param {Array.<ExecutionProfile>} [profiles]
 * @returns {Function}
 */
function getMoveNextHostTest(prepare, prepareWarmup, expectedTimeoutMillis, readTimeout, queryOptions, profiles) {
  if (!expectedTimeoutMillis) {
    expectedTimeoutMillis = 3000;
  }
  if (!readTimeout) {
    readTimeout = 3000;
  }
  profiles = profiles || [];
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

function getTimeoutErrorNotExpectedTest(prepare, prepareWarmup, readTimeout, queryOptions, profiles) {
  if (typeof readTimeout === 'undefined') {
    readTimeout = 0;
  }

  profiles = profiles || [];

  return (function timeoutErrorNotExpectedTest(done) {
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
        // Should get responses from each coordinator since no requests should have timed out.
        assert.strictEqual(Object.keys(coordinators).length, 2);
        assert.strictEqual(coordinators['1'], true);
        assert.strictEqual(coordinators['2'], true);
        next();
      },
      client.shutdown.bind(client)
    ], done);
  });
}

/**
 * Represents a LoadBalancingPolicy that always yields the hosts in the same order, only suitable for testing.
 * @constructor
 */
function FixedOrderLoadBalancingPolicy() {
}

util.inherits(FixedOrderLoadBalancingPolicy, loadBalancing.RoundRobinPolicy);

FixedOrderLoadBalancingPolicy.prototype.newQueryPlan = function (ks, q, callback) {
  callback(null, utils.arrayIterator(this.hosts.values()));
};