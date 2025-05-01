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
import Client from "../../../lib/client";
import metrics from "../../../lib/metrics/index";
import errors from "../../../lib/errors";
import simulacron from "../simulacron";
import helper from "../../test-helper";
import policies from "../../../lib/policies/index";


const RetryPolicy = policies.retry.RetryPolicy;

const queries = {
  syntaxError: { value: 'SELECT * FROM system.syntax_error', failure: 'syntax_error' }
};

const delay = 400;

const queriesFailingOnFirstNode = {
  delayed: { value: 'INSERT INTO delayed (id) VALUES (?)', failure: { result: 'success', delay_in_ms: delay } },
  overloaded: { value: 'INSERT INTO overloaded (id) VALUES (?)', failure: 'overloaded' },
  unavailable: { value: 'INSERT INTO unavailable (id) VALUES (?)', failure: {
    result: 'unavailable', alive: 4, required: 5, consistency_level: 'LOCAL_QUORUM'
  }},
  readTimeout: { value: 'INSERT INTO read_timeout (id) VALUES (?)', failure: {
    result: 'read_timeout', received: 1, block_for: 2, consistency_level: 'TWO', data_present: false
  }},
  writeTimeout: { value: 'INSERT INTO write_timeout (id) VALUES (?)', failure: {
    result: 'write_timeout', received: 1, block_for: 3, consistency_level: 'QUORUM', write_type: 'SIMPLE'
  }}
};

describe('Client', function () {
  this.timeout(5000);
  const setupInfo = simulacron.setup([ 3 ], { initClient: false });
  primeQueries(setupInfo);

  // Use different metrics implementations
  [ getClientMetrics, getDefaultMetrics ].forEach(factory => {

    context(`with ${factory().constructor.name}`, () => {

      let client;

      beforeEach(() => {
        client = new Client({
          contactPoints: [ simulacron.startingIp ],
          policies: {
            retry: new TestRetryPolicy(),
            loadBalancing: new helper.OrderedLoadBalancingPolicy(),
            speculativeExecution: new policies.speculativeExecution.ConstantSpeculativeExecutionPolicy(delay / 2, 1)
          },
          metrics: factory()
        });

        return client.connect();
      });

      afterEach(() => client.shutdown());

      it('should not retry non-idempotent queries', () => {
        let catchCalled;

        return client.execute(queriesFailingOnFirstNode.overloaded.value, [], { isIdempotent: false })
          .catch(err => {
            catchCalled = true;
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, undefined);
            assert.strictEqual(client.metrics.response.length, 1);
            const latency = client.metrics.response[0];
            // Latency is an Array tuple composed of [seconds, nanoseconds]
            assert.strictEqual(latency.length, 2);
            assert.ok(Number.isInteger(latency[0]));
            assert.ok(Number.isInteger(latency[1]));
          })
          .then(() => assert.strictEqual(catchCalled, true));
      });

      it('should not retry syntax errors', () => {
        let catchCalled;

        return client.execute(queries.syntaxError.value, [], { isIdempotent: true })
          .catch(err => {
            catchCalled = true;
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, undefined);
            assert.strictEqual(client.options.policies.retry.called, 0);
          })
          .then(() => assert.strictEqual(catchCalled, true));
      });

      it('should not retry when a speculative execution previously completed', () =>
        client.execute(queriesFailingOnFirstNode.delayed.value, [], {isIdempotent: true})
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(rs.info.speculativeExecutions, 1);

            assert.strictEqual(client.metrics.clientTimeoutError, undefined);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.metrics.speculativeExecution, 1);
            assert.strictEqual(client.metrics.clientTimeoutRetry, undefined);
            assert.strictEqual(client.metrics.response.length, 1);
          })
          .then(() => new Promise(r => setTimeout(r, delay + 20)))
          .then(() => {
            // First request should be finished by now
            // It was not retried
            assert.strictEqual(client.metrics.clientTimeoutRetry, undefined);
            assert.strictEqual(client.metrics.successfulResponse, 1);
          }));

      it('should use the retry policy on overloaded errors', () =>
        client.execute(queriesFailingOnFirstNode.overloaded.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            // The retry policy was invoked
            assert.strictEqual(client.options.policies.retry.requestError, 1);
          }));

      it('should use the retry policy for unavailable', () =>
        client.execute(queriesFailingOnFirstNode.unavailable.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.unavailableError, 1);
            assert.strictEqual(client.metrics.unavailableRetry, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.options.policies.retry.called, 1);
            assert.strictEqual(client.options.policies.retry.unavailable, 1);
            assert.strictEqual(client.metrics.response.length, 2);
          }));

      it('should use the retry policy for server read timeout', () =>
        client.execute(queriesFailingOnFirstNode.readTimeout.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.readTimeoutError, 1);
            assert.strictEqual(client.metrics.readTimeoutRetry, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.options.policies.retry.called, 1);
            assert.strictEqual(client.options.policies.retry.readTimeout, 1);
            assert.strictEqual(client.metrics.response.length, 2);
          }));

      it('should use the retry policy for server write timeout', () =>
        client.execute(queriesFailingOnFirstNode.writeTimeout.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.writeTimeoutError, 1);
            assert.strictEqual(client.metrics.writeTimeoutRetry, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.options.policies.retry.called, 1);
            assert.strictEqual(client.options.policies.retry.writeTimeout, 1);
            assert.strictEqual(client.metrics.response.length, 2);
          }));

      it('should use the retry policy for client timeout', () => {
        // Throw the error as fast as possible
        client.options.socketOptions.readTimeout = 50;
        // Disable speculative executions
        client.options.policies.speculativeExecution = new policies.speculativeExecution.NoSpeculativeExecutionPolicy();

        return client.execute(queriesFailingOnFirstNode.delayed.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.clientTimeoutError, 1);
            assert.strictEqual(client.metrics.clientTimeoutRetry, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.options.policies.retry.called, 1);
            assert.strictEqual(client.options.policies.retry.requestError, 1);
            assert.strictEqual(client.metrics.response.length, 1);
          });
      });

      it('should rethrow when defined by the retry policy', () => {
        // Rethrow all errors
        client.options.policies.retry.rethrowAll();

        let catchCalled;

        return client.execute(queriesFailingOnFirstNode.overloaded.value, [], { isIdempotent: true })
          .catch(err => {
            catchCalled = true;
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, undefined);
            assert.strictEqual(client.metrics.ignoredError, undefined);
            assert.strictEqual(client.options.policies.retry.requestError, 1);
            assert.strictEqual(client.metrics.response.length, 1);
          })
          .then(() => assert.strictEqual(catchCalled, true));
      });

      it('should return an empty result set when ignored by the retry policy', () => {
        // Ignore all errors
        client.options.policies.retry.ignoreAll();

        return client.execute(queriesFailingOnFirstNode.overloaded.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.first(), null);
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(0).address);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, undefined);
            assert.strictEqual(client.metrics.ignoredError, 1);
            assert.strictEqual(client.options.policies.retry.requestError, 1);
            assert.strictEqual(client.metrics.response.length, 1);
          });
      });
    });
  });
});

function getClientMetrics() {
  const m = new metrics.ClientMetrics();
  m.response = [];

  // Custom implementation of each interface method
  m.onClientTimeoutError = () => m.clientTimeoutError = (m.clientTimeoutError || 0) + 1;
  m.onConnectionError = () => m.connectionError = (m.connectionError || 0) + 1;
  m.onOtherError = () => m.otherError = (m.otherError || 0) + 1;
  m.onReadTimeoutError = () => m.readTimeoutError = (m.readTimeoutError || 0) + 1;
  m.onUnavailableError = () => m.unavailableError = (m.unavailableError || 0) + 1;
  m.onWriteTimeoutError = () => m.writeTimeoutError = (m.writeTimeoutError || 0) + 1;

  m.onClientTimeoutRetry = () => m.clientTimeoutRetry = (m.clientTimeoutRetry || 0) + 1;
  m.onOtherRetry = () => m.otherRetry = (m.otherRetry || 0) + 1;
  m.onReadTimeoutRetry = () => m.readTimeoutRetry = (m.readTimeoutRetry || 0) + 1;
  m.onUnavailableRetry = () => m.unavailableRetry = (m.unavailableRetry || 0) + 1;
  m.onWriteTimeoutRetry = () => m.writeTimeoutRetry = (m.writeTimeoutRetry || 0) + 1;

  m.onIgnoreError = () => m.ignoredError = (m.ignoredError || 0) + 1;
  m.onSpeculativeExecution = () => m.speculativeExecution = (m.speculativeExecution || 0) + 1;
  m.onSuccessfulResponse = () => m.successfulResponse = (m.successfulResponse || 0) + 1;
  m.onResponse = latency => m.response.push(latency);

  return m;
}

function getDefaultMetrics() {
  const m = new metrics.DefaultMetrics();
  m.response = [];

  // Listen to each event and track them in properties
  m.errors.clientTimeout.on('increment', () => m.clientTimeoutError = (m.clientTimeoutError || 0) + 1);
  m.errors.other.on('increment', () => m.otherError = (m.otherError || 0) + 1);
  m.errors.readTimeout.on('increment', () => m.readTimeoutError = (m.readTimeoutError || 0) + 1);
  m.errors.unavailable.on('increment', () => m.unavailableError = (m.unavailableError || 0) + 1);
  m.errors.writeTimeout.on('increment', () => m.writeTimeoutError = (m.writeTimeoutError || 0) + 1);

  m.retries.clientTimeout.on('increment', () => m.clientTimeoutRetry = (m.clientTimeoutRetry || 0) + 1);
  m.retries.other.on('increment', () => m.otherRetry = (m.otherRetry || 0) + 1);
  m.retries.readTimeout.on('increment', () => m.readTimeoutRetry = (m.readTimeoutRetry || 0) + 1);
  m.retries.unavailable.on('increment', () => m.unavailableRetry = (m.unavailableRetry || 0) + 1);
  m.retries.writeTimeout.on('increment', () => m.writeTimeoutRetry = (m.writeTimeoutRetry || 0) + 1);

  m.ignoredErrors.on('increment', () => m.ignoredError = (m.ignoredError || 0) + 1);
  m.speculativeExecutions.on('increment', () => m.speculativeExecution = (m.speculativeExecution || 0) + 1);
  m.responses.on('increment', latency => m.response.push(latency));
  m.responses.success.on('increment', () => m.successfulResponse = (m.successfulResponse || 0) + 1);

  return m;
}

/** A retry policy that tracks calls to each method.*/
class TestRetryPolicy extends RetryPolicy {
  constructor() {
    super();

    this.unavailable = this.readTimeout = this.writeTimeout = this.requestError = this.called = 0;

    this._result = {
      decision: RetryPolicy.retryDecision.retry,
      useCurrentHost: false
    };
  }

  ignoreAll() {
    this._result = { decision: RetryPolicy.retryDecision.ignore };
  }

  rethrowAll() {
    this._result = { decision: RetryPolicy.retryDecision.rethrow };
  }

  _getResult() {
    this.called++;
    return this._result;
  }

  onUnavailable() {
    this.unavailable++;
    return this._getResult();
  }

  onReadTimeout() {
    this.readTimeout++;
    return this._getResult();
  }

  onWriteTimeout() {
    this.writeTimeout++;
    return this._getResult();
  }

  onRequestError() {
    this.requestError++;
    return this._getResult();
  }
}

function primeQueries(setupInfo) {
  Object.keys(queries).forEach(key => {
    const item = queries[key];

    beforeEach(done => setupInfo.cluster.prime({
      when: { query: item.value },
      then: {
        result: item.failure, message: `Sample error message for ${item.failure}`,
      }
    }, done));
  });

  Object.keys(queriesFailingOnFirstNode).forEach(key => {
    const item = queriesFailingOnFirstNode[key];

    beforeEach(done => setupInfo.cluster.prime({
      when: { query: item.value },
      then: { result: 'success', delay_in_ms: 0 }
    }, done));

    const failureResult = typeof item.failure === 'string'
      ? { result: item.failure, message: `Sample error (first node) message for ${item.failure}`}
      : item.failure;

    beforeEach(done => setupInfo.cluster.node(0).prime({
      when: { query: item.value },
      then: failureResult
    }, done));
  });
}