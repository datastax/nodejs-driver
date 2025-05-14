/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from "assert";
import util from "util";
import RequestHandler from "../../lib/request-handler";
import requests from "../../lib/requests";
import helper from "../test-helper";
import errors from "../../lib/errors";
import types from "../../lib/types/index";
import utils from "../../lib/utils";
import retry from "../../lib/policies/retry";
import speculativeExecution from "../../lib/policies/speculative-execution";
import execProfileModule from "../../lib/execution-profile";
import OperationState from "../../lib/operation-state";
import * as execOptionsModule from "../../lib/execution-options";
import ClientMetrics from "../../lib/metrics/client-metrics";
import { defaultOptions } from "../../lib/client-options";

const ProfileManager = execProfileModule.ProfileManager;
const ExecutionProfile = execProfileModule.ExecutionProfile;
const DefaultExecutionOptions = execOptionsModule.DefaultExecutionOptions;
const ExecutionOptions = execOptionsModule.ExecutionOptions;
describe('RequestHandler', function () {
  const queryRequest = new requests.QueryRequest('QUERY1');
  describe('#send()', function () {
    it('should return a ResultSet', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ]);
      const handler = newInstance(queryRequest, null, lbp);
      const result = await handler.send();
      helper.assertInstanceOf(result, types.ResultSet);
    });

    it('should callback with error when error can not be retried', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new Error('Test Error'));
        }
        cb(null, {});
      });

      const handler = newInstance(queryRequest, null, lbp, new TestRetryPolicy());
      let err;

      try {
        await handler.send();
      } catch (e) {
        err = e;
      }

      helper.assertInstanceOf(err, Error);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 0);

    });

    it('should use the retry policy defined in the queryOptions', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.ResponseError(types.responseErrorCodes.writeTimeout, 'Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, true);
      const result = await handler.send();

      helper.assertInstanceOf(result, types.ResultSet);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 1);
      assert.strictEqual(retryPolicy.writeTimeoutErrors.length, 1);
    });

    it('should use the provided host if specified in the queryOptions', async () => {
      // get a fake host that always responds with a readTimeout
      const host = helper.getHostsMock([ {} ], undefined, (r, h, cb) => {
        cb(new errors.ResponseError(types.responseErrorCodes.readTimeout, 'Test error'));
      })[0];

      helper.afterThisTest(() => host.shutdown());

      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, null, host);

      const err = await helper.assertThrowsAsync(handler.send());

      // expect an error that includes read timeout for that host.
      assert.deepEqual(Object.keys(err.innerErrors), [host.address]);
      assert.strictEqual(err.innerErrors[host.address].code, types.responseErrorCodes.readTimeout);
      // should have skipped lbp entirely.
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 0);
      assert.strictEqual(hosts[1].sendStreamCalled, 0);
    });

    it('should callback with OperationTimedOutError when the retry policy decides', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.OperationTimedOutError('Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy(false);
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, true);

      await helper.assertThrowsAsync(handler.send(), errors.OperationTimedOutError);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 0);
      assert.strictEqual(retryPolicy.requestErrors.length, 1);
    });

    it('should not use the retry policy if query is non-idempotent on writeTimeout', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.ResponseError(types.responseErrorCodes.writeTimeout, 'Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, false);

      const err = await helper.assertThrowsAsync(handler.send());
      helper.assertInstanceOf(err, errors.ResponseError);
      assert.strictEqual(err.code, types.responseErrorCodes.writeTimeout);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 0);
      assert.strictEqual(retryPolicy.writeTimeoutErrors.length, 0);
    });

    it('should not use the retry policy if query is non-idempotent on OperationTimedOutError', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.OperationTimedOutError('Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy(false);
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, false);

      await helper.assertThrowsAsync(handler.send(), errors.OperationTimedOutError);

      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 0);
      assert.strictEqual(retryPolicy.requestErrors.length, 0);
    });

    it('should use the retry policy even if query is non-idempotent on readTimeout', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.ResponseError(types.responseErrorCodes.readTimeout, 'Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, false);
      const result = await handler.send();

      helper.assertInstanceOf(result, types.ResultSet);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 1);
      assert.strictEqual(retryPolicy.readTimeoutErrors.length, 1);
    });

    it('should use the retry policy even if query is non-idempotent on unavailable', async () => {
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendStreamCb(r, h, cb) {
        if (h.address === '0') {
          return cb(new errors.ResponseError(types.responseErrorCodes.unavailableException, 'Test error'));
        }
        cb(null, {});
      });

      const retryPolicy = new TestRetryPolicy();
      const handler = newInstance(queryRequest, null, lbp, retryPolicy, false);
      const result = await handler.send();

      helper.assertInstanceOf(result, types.ResultSet);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].sendStreamCalled, 1);
      assert.strictEqual(hosts[1].sendStreamCalled, 1);
      assert.strictEqual(retryPolicy.unavailableErrors.length, 1);
    });

    context('when an UNPREPARED response is obtained', function () {
      it('should send a prepare request on the same connection and update the cache', async () => {
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

        const request = new requests.ExecuteRequest('QUERY1', queryId, []);
        const handler = newInstance(request, client, lbp);

        await handler.send();

        assert.strictEqual(hosts[0].prepareCalled, 1);
        assert.strictEqual(hosts[0].sendStreamCalled, 2);
        assert.strictEqual(hosts[1].prepareCalled, 0);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
        // metadata should be updated when reprepared.
        const info = client.metadata.getPreparedById(1);
        assert.deepEqual(info.meta, metadata);
        // metadata should have been propagated to subsequent execute request.
        assert.deepEqual(executeRequest.meta, metadata);
      });

      it('should allow prepared statement keyspace different than connection keyspace', async () => {
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

        const request = new requests.ExecuteRequest('QUERY1', queryId, [], ExecutionOptions.empty());
        const handler = newInstance(request, client, lbp);
        await handler.send();

        // should have been initial request, unprepared sent back, and error raised before preparing.
        assert.strictEqual(hosts[0].prepareCalled, 1);
        assert.strictEqual(hosts[0].sendStreamCalled, 2);
        assert.strictEqual(hosts[1].prepareCalled, 0);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
      });

      it('should throw an error if prepared statement was on different keyspace than connection with older protocol version', async () => {
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

        const request = new requests.ExecuteRequest('QUERY1', queryId, []);
        const handler = newInstance(request, client, lbp);
        const err = await helper.assertThrowsAsync(handler.send());

        helper.assertContains(err.message, 'Query was prepared on keyspace ks1');
        // should have been initial request, unprepared sent back, and error raised before preparing.
        assert.strictEqual(hosts[0].prepareCalled, 0);
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].prepareCalled, 0);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
      });

      it('should move to next host when PREPARE response is an error', async () => {
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

        const request = new requests.ExecuteRequest('QUERY1', queryId, [], ExecutionOptions.empty());
        const handler = newInstance(request, client, lbp);
        await handler.send();

        assert.strictEqual(hosts[0].prepareCalled, 1);
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].prepareCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 2);
      });

      it('should update prepared cache when rows response received with new result id', async () => {
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

        const request = new requests.ExecuteRequest('QUERY1', queryId, []);
        const handler = newInstance(request, client, lbp);

        await handler.send();

        assert.strictEqual(hosts[0].prepareCalled, 0);
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].prepareCalled, 0);
        assert.strictEqual(hosts[1].sendStreamCalled, 0);
        // metadata should be updated by newResultId detected in result.
        const info = client.metadata.getPreparedById(1);
        assert.deepEqual(info.meta.resultId, newResultId);
      });
    });

    context('with speculative executions', function () {
      it('should use the query plan to use next hosts as coordinators', async () => {
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
        const result = await handler.send();

        helper.assertInstanceOf(result, types.ResultSet);
        // Used the third host to get the response
        assert.strictEqual(result.info.queriedHost, '2');
        assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1', '2' ]);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 1);
        assert.strictEqual(hosts[2].sendStreamCalled, 1);
      });

      it('should use the query plan to use next hosts as coordinators with zero delay', async () => {
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
        const result = await handler.send();
        helper.assertInstanceOf(result, types.ResultSet);
        // Used the second host to get the response
        assert.strictEqual(result.info.queriedHost, '1');
        assert.deepEqual(Object.keys(result.info.triedHosts), [ '0', '1' ]);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 1);
      });

      it('should callback in error when any of execution responses is an error that cant be retried', async () => {
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
        await helper.assertThrowsAsync(handler.send());
        const hosts = lbp.getFixedQueryPlan();
        // 3 hosts were queried but the first responded with an error
        assert.strictEqual(hosts[0].sendStreamCalled, 1);
        assert.strictEqual(hosts[1].sendStreamCalled, 1);
        assert.strictEqual(hosts[2].sendStreamCalled, 1);
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
 * @param {Host} [host]
 * @returns {RequestHandler}
 */
function newInstance(request, client, lbp, retry, isIdempotent, host) {
  client = client || newClient(null, lbp);
  const options = {
    executionProfile: new ExecutionProfile('abc', { loadBalancing: lbp }), retry: retry, isIdempotent: isIdempotent, host: host
  };
  const execOptions = new DefaultExecutionOptions(options, client);

  return new RequestHandler(request, execOptions, client);
}

function newClient(metadata, lbp) {
  const options = defaultOptions();
  options.logEmitter = utils.noop;
  options.policies.loadBalancing = lbp || options.policies.loadBalancing;
  return {
    profileManager: new ProfileManager(options),
    options: options,
    metadata: metadata,
    metrics: new ClientMetrics()
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