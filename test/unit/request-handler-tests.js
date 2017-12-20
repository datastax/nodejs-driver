'use strict';
const assert = require('assert');
const util = require('util');

const RequestHandler = require('../../lib/request-handler');
const requests = require('../../lib/requests');
const helper = require('../test-helper');
const errors = require('../../lib/errors');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const retry = require('../../lib/policies/retry');
const speculativeExecution = require('../../lib/policies/speculative-execution');
const ProfileManager = require('../../lib/execution-profile').ProfileManager;
const OperationState = require('../../lib/operation-state');
const defaultOptions = require('../../lib/client-options').defaultOptions;

describe('RequestHandler', function () {
  var queryRequest = new requests.QueryRequest('QUERY1');
  describe('#send()', function () {
    it('should return a ResultSet', function (done) {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ]);
      var handler = newInstance(queryRequest, null, lbp);
      handler.send(function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        done();
      });
    });
    it('should callback with error when error can not be retried', function (done) {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new Error('Test Error'));
        }
        cb(null, {});
      });
      var handler = newInstance(queryRequest, null, lbp, new TestRetryPolicy());
      handler.send(function (err) {
        helper.assertInstanceOf(err, Error);
        var hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
        done();
      });
    });
    it('should use the retry policy defined in the queryOptions', function (done) {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.ResponseError(types.responseErrorCodes.writeTimeout, 'Test error'));
        }
        cb(null, {});
      });
      var retryPolicy = new TestRetryPolicy();
      var handler = newInstance(queryRequest, null, lbp, retryPolicy);
      handler.send(function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        var hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 1);
        assert.strictEqual(retryPolicy.writeTimeoutErrors.length, 1);
        done();
      });
    });
    it('should callback with OperationTimedOutError when the retry policy decides', function (done) {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.OperationTimedOutError('Test error'));
        }
        cb(null, {});
      });
      var retryPolicy = new TestRetryPolicy(false);
      var handler = newInstance(queryRequest, null, lbp, retryPolicy);
      handler.send(function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        var hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
        assert.strictEqual(retryPolicy.requestErrors.length, 1);
        done();
      });
    });
    context('when an UNPREPARED response is obtained', function () {
      it('should send a prepare request on the same connection', function (done) {
        var queryId = utils.allocBufferFromString('123');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendCallback(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            var err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        var hosts = lbp.getFixedQueryPlan();
        const client = newClient({
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id };
          }
        }, lbp);
        var request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        var handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 2);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          done();
        });
      });
      it('should move to next host when PREPARE response is an error', function (done) {
        var queryId = utils.allocBufferFromString('123');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function prepareCallback(q, h, cb) {
          if (h.address === '0') {
            return cb(new Error('Test error'));
          }
          cb();
        }, function sendFake(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            var err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        var hosts = lbp.getFixedQueryPlan();
        const client = newClient({
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id };
          }
        }, lbp);
        var request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        var handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].prepareCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 2);
          done();
        });
      });
    });
    context('with speculative executions', function () {
      it('should use the query plan to use next hosts as coordinators', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {}, {}], undefined, function sendStreamCb(r, h, cb) {
          var op = new OperationState(r, null, cb);
          if (h.address !== '2') {
            setTimeout(function () {
              op.setResult(null, {});
            }, 60);
            return op;
          }
          op.setResult(null, {});
          return op;
        });
        const client = newClient(null, lbp);
        client.options.policies.speculativeExecution =
          new speculativeExecution.ConstantSpeculativeExecutionPolicy(20, 2);
        var handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err, result) {
          assert.ifError(err);
          helper.assertInstanceOf(result, types.ResultSet);
          // Used the third host to get the response
          assert.strictEqual(result.info.queriedHost, '2');
          assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1', '2' ]);
          var hosts = lbp.getFixedQueryPlan();
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 1);
          assert.strictEqual(hosts[2].sendStreamCalled, 1);
          done();
        });
      });
      it('should use the query plan to use next hosts as coordinators with zero delay', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
          var op = new OperationState(r, null, cb);
          if (h.address !== '1') {
            setTimeout(function () {
              op.setResult(null, {});
            }, 40);
            return op;
          }
          op.setResult(null, {});
          return op;
        });
        const client = newClient(null, lbp);
        client.options.policies.speculativeExecution =
          new speculativeExecution.ConstantSpeculativeExecutionPolicy(0, 2);
        var handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err, result) {
          assert.ifError(err);
          helper.assertInstanceOf(result, types.ResultSet);
          // Used the second host to get the response
          assert.strictEqual(result.info.queriedHost, '1');
          assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1' ]);
          var hosts = lbp.getFixedQueryPlan();
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 1);
          done();
        });
      });
      it('should callback in error when any of execution responses is an error that cant be retried', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {}, {}], undefined, function sendStreamCb(r, h, cb) {
          var op = new OperationState(r, null, cb);
          if (h.address !== '0') {
            setTimeout(function () {
              op.setResult(null, {});
            }, 60);
            return op;
          }
          // The first request is going to be completed with an error
          setTimeout(function () {
            op.setResult(new Error('Test error'));
          }, 60);
          return op;
        });
        const client = newClient(null, lbp);
        client.options.policies.speculativeExecution =
          new speculativeExecution.ConstantSpeculativeExecutionPolicy(20, 2);
        var handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err) {
          helper.assertInstanceOf(err, Error);
          var hosts = lbp.getFixedQueryPlan();
          // 3 hosts were queried but the first responded with an error
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 1);
          assert.strictEqual(hosts[2].sendStreamCalled, 1);
          done();
        });
      });
    });
  });
});

/**
 * @param {Request} request
 * @param {Client} client
 * @param {LoadBalancingPolicy} lbp
 * @param {RetryPolicy} [retry]
 * @param {Boolean} [isIdempotent]
 * @returns {RequestHandler}
 */
function newInstance(request, client, lbp, retry, isIdempotent) {
  client = client || newClient(null, lbp);
  const options = { executionProfile: { loadBalancing: lbp }, retry: retry, isIdempotent: isIdempotent };
  return new RequestHandler(request, options, client);
}

function newClient(metadata, lbp) {
  const options = defaultOptions();
  options.logEmitter = utils.noop;
  options.policies.loadBalancing = lbp || options.policies.loadBalancing;
  return {
    profileManager: new ProfileManager(options),
    options: options,
    metadata: metadata
  };
}

/** @extends RetryPolicy */
function TestRetryPolicy(retryOnRequestError, retryOnUnavailable, retryOnReadTimeout, retryOnWriteTimeout) {
  this._retryOnRequestError = ifUndefined(retryOnRequestError, true);
  this._retryOnUnavailable = ifUndefined(retryOnUnavailable, true);
  this._retryOnReadTimeout = ifUndefined(retryOnReadTimeout, true);
  this._retryOnWriteTimeout = ifUndefined(retryOnWriteTimeout, true);
  this.requestErrors = [];
  this.unavailableErrors = [];
  this.writeTimeoutErrors = [];
  this.readTimeoutErrors = [];
}

util.inherits(TestRetryPolicy, retry.RetryPolicy);

TestRetryPolicy.prototype.onRequestError = function () {
  this.requestErrors.push(Array.prototype.slice.call(arguments));
  return this._retryOnRequestError ? this.retryResult(undefined, false) : this.rethrowResult();
};

TestRetryPolicy.prototype.onUnavailable = function () {
  this.unavailableErrors.push(Array.prototype.slice.call(arguments));
  return this._retryOnUnavailable ? this.retryResult(undefined, false) : this.rethrowResult();
};

TestRetryPolicy.prototype.onReadTimeout = function () {
  this.readTimeoutErrors.push(Array.prototype.slice.call(arguments));
  return this._retryOnReadTimeout ? this.retryResult(undefined, false) : this.rethrowResult();
};

TestRetryPolicy.prototype.onWriteTimeout = function () {
  this.writeTimeoutErrors.push(Array.prototype.slice.call(arguments));
  return this._retryOnWriteTimeout ? this.retryResult(undefined, false) : this.rethrowResult();
};

function ifUndefined(value, valueIfUndefined) {
  return value === undefined ? valueIfUndefined : value;
}