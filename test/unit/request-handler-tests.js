/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
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
  const queryRequest = new requests.QueryRequest('QUERY1');
  describe('#send()', function () {
    it('should return a ResultSet', function (done) {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ]);
      const handler = newInstance(queryRequest, null, lbp);
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
      const handler = newInstance(queryRequest, null, lbp, new TestRetryPolicy());
      handler.send(function (err) {
        helper.assertInstanceOf(err, Error);
        const hosts = lbp.getFixedQueryPlan();
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
      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy);
      handler.send(function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 1);
        assert.strictEqual(retryPolicy.writeTimeoutErrors.length, 1);
        done();
      });
    });
    it('should use the provided host if specified in the queryOptions', function (done) {
      // get a fake host that always responds with a readTimeout
      const host = helper.getHostsMock([ {} ], undefined, (r, h, cb) => {
        cb(new errors.ResponseError(types.responseErrorCodes.readTimeout, 'Test error'));
      })[0];

      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        cb(null, {});
      });
      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, null, host);
      handler.send(function (err, result) {
        // expect an error that includes read timeout for that host.
        assert.ok(err);
        assert.deepEqual(Object.keys(err.innerErrors), [host.address]);
        assert.strictEqual(err.innerErrors[host.address].code, types.responseErrorCodes.readTimeout);
        // should have skipped lbp entirely.
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 0);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
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
      const retryPolicy = new TestRetryPolicy(false);
      const handler = newInstance(queryRequest, null, lbp, retryPolicy);
      handler.send(function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
        assert.strictEqual(retryPolicy.requestErrors.length, 1);
        done();
      });
    });
    context('when an UNPREPARED response is obtained', function () {
      it('should send a prepare request on the same connection and update the cache', function (done) {
        const queryId = utils.allocBufferFromString('123');
        const resultId = utils.allocBufferFromString('8675');
        const metadata = { resultId: resultId };
        let executeRequest;
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function prepareCallback(q, h, cb) {
          // mock prepare returning metadata different than what is already cached.
          cb(null, { meta: metadata });
        }, function sendCallback(r, h, cb) {
          // capture final execute request to ensure new metadata was propagated.
          executeRequest = r;
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            const err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        const hosts = lbp.getFixedQueryPlan();
        const preparedCacheData = { query: 'QUERY1', meta: {}};
        const client = newClient({
          getPreparedById: function (id) {
            preparedCacheData.id = id;
            return preparedCacheData;
          }
        }, lbp);
        const request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        const handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 2);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          // metadata should be updated when reprepared.
          const info = client.metadata.getPreparedById(1);
          assert.deepEqual(info.meta, metadata);
          // metadata should have been propagated to subsequent execute request.
          assert.deepEqual(executeRequest.meta, metadata);
          done();
        });
      });
      it('should allow prepared statement keyspace different than connection keyspace', function (done) {
        const queryId = utils.allocBufferFromString('123');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendCallback(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            const err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        const hosts = lbp.getFixedQueryPlan();
        const client = newClient({
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id, keyspace: 'ks1'};
          }
        }, lbp);
        const request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        const handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          // should have been initial request, unprepared sent back, and error raised before preparing.
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 2);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          done();
        });
      });
      it('should throw an error if prepared statement was on different keyspace than connection with older protocol version', function (done) {
        const queryId = utils.allocBufferFromString('123');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendCallback(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            const err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        }, types.protocolVersion.dseV1);
        const hosts = lbp.getFixedQueryPlan();
        const client = newClient({
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id, keyspace: 'ks1'};
          }
        }, lbp);
        const request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        const handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          helper.assertInstanceOf(err, Error);
          helper.assertContains(err.message, 'Query was prepared on keyspace ks1');
          // should have been initial request, unprepared sent back, and error raised before preparing.
          assert.strictEqual(hosts[0].prepareCalled, 0);
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          done();
        });
      });
      it('should move to next host when PREPARE response is an error', function (done) {
        const queryId = utils.allocBufferFromString('123');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function prepareCallback(q, h, cb) {
          if (h.address === '0') {
            return cb(new Error('Test error'));
          }
          cb(null, { });
        }, function sendFake(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            const err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        const hosts = lbp.getFixedQueryPlan();
        const client = newClient({
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id };
          }
        }, lbp);
        const request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        const handler = newInstance(request, client, lbp);
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
      it('should update prepared cache when rows response received with new result id', function (done) {
        const queryId = utils.allocBufferFromString('123');
        const resultId = utils.allocBufferFromString('8675');
        const newResultId = utils.allocBufferFromString('309');
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendCallback(r, h, cb) {
          // mock a result having meta with a newResultId
          cb(null, { meta: { newResultId: newResultId } });
        });
        const hosts = lbp.getFixedQueryPlan();
        const preparedCacheData = { query: 'QUERY1', meta: { resultId: resultId }};
        const client = newClient({
          getPreparedById: function (id) {
            preparedCacheData.id = id;
            return preparedCacheData;
          }
        }, lbp);
        const request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        const handler = newInstance(request, client, lbp);
        handler.send(function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 0);
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          // metadata should be updated by newResultId detected in result.
          const info = client.metadata.getPreparedById(1);
          assert.deepEqual(info.meta.resultId, newResultId);
          done();
        });
      });
    });
    context('with speculative executions', function () {
      it('should use the query plan to use next hosts as coordinators', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {}, {}], undefined, function sendStreamCb(r, h, cb) {
          const op = new OperationState(r, null, cb);
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
        const handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err, result) {
          assert.ifError(err);
          helper.assertInstanceOf(result, types.ResultSet);
          // Used the third host to get the response
          assert.strictEqual(result.info.queriedHost, '2');
          assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1', '2' ]);
          const hosts = lbp.getFixedQueryPlan();
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 1);
          assert.strictEqual(hosts[2].sendStreamCalled, 1);
          done();
        });
      });
      it('should use the query plan to use next hosts as coordinators with zero delay', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
          const op = new OperationState(r, null, cb);
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
        const handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err, result) {
          assert.ifError(err);
          helper.assertInstanceOf(result, types.ResultSet);
          // Used the second host to get the response
          assert.strictEqual(result.info.queriedHost, '1');
          assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1' ]);
          const hosts = lbp.getFixedQueryPlan();
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 1);
          done();
        });
      });
      it('should callback in error when any of execution responses is an error that cant be retried', function (done) {
        const lbp = helper.getLoadBalancingPolicyFake([ {}, {}, {}], undefined, function sendStreamCb(r, h, cb) {
          const op = new OperationState(r, null, cb);
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
        const handler = newInstance(queryRequest, client, lbp, null, true);
        handler.send(function (err) {
          helper.assertInstanceOf(err, Error);
          const hosts = lbp.getFixedQueryPlan();
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
function newInstance(request, client, lbp, retry, isIdempotent, host) {
  client = client || newClient(null, lbp);
  const options = { executionProfile: { loadBalancing: lbp }, retry: retry, isIdempotent: isIdempotent, host: host };
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
