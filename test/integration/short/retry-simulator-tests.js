'use strict';

const assert = require('assert');
const Client = require('../../../lib/Client');
const metrics = require('../../../lib/metrics');
const errors = require('../../../lib/errors');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const policies = require('../../../lib/policies');
const RetryPolicy = policies.retry.RetryPolicy;

const queries = {
  syntaxError: { value: 'SELECT * FROM system.syntax_error', failure: 'syntax_error' }
};

const delay = 500;

const queriesFailingOnFirstNode = {
  delayed: { value: 'INSERT INTO delayed (id) VALUES (?)', failure: { result: 'success', delay_in_ms: delay } },
  overloaded: { value: 'INSERT INTO overloaded (id) VALUES (?)', failure: 'overloaded' }
};

describe('Client', function () {
  this.timeout(5000);
  const setupInfo = simulacron.setup([ 3 ], { initClient: false });

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


  [ getClientMetrics, getDefaultMetrics ].forEach(factory => {

    context(`with ${factory().constructor.name}`, () => {

      let client;

      beforeEach(() => {
        client = new Client({
          contactPoints: [ simulacron.startingIp ],
          policies: {
            retry: new AlwaysRetryPolicy(),
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
          })
          .then(() => assert.strictEqual(catchCalled, true));
      });

      it('should not retry when a speculative execution previously completed', () =>
        client.execute(queriesFailingOnFirstNode.delayed.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(rs.info.speculativeExecutions, 1);

            assert.strictEqual(client.metrics.clientTimeoutError, undefined);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.metrics.clientTimeoutRetry, undefined);
          })
          .then(() => new Promise(r => setTimeout(r, delay + 20)))
          .then(() => {
            // First request should be finished by now
            // It was not retried
            assert.strictEqual(client.metrics.clientTimeoutRetry, undefined);
            assert.strictEqual(client.metrics.successfulResponse, 1);
          }));

      it('should retry overloaded errors', () =>
        client.execute(queriesFailingOnFirstNode.overloaded.value, [], { isIdempotent: true })
          .then(rs => {
            assert.strictEqual(rs.info.queriedHost, setupInfo.cluster.node(1).address);
            assert.strictEqual(client.metrics.otherError, 1);
            assert.strictEqual(client.metrics.successfulResponse, 1);
            assert.strictEqual(client.options.policies.retry.requestError, 1);
          }));
    });
  });
});

function getClientMetrics() {
  const m = new metrics.ClientMetrics();

  m.onAuthenticationError = () => m.onAuthenticationError = (m.onAuthenticationError || 0) + 1;
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

  return m;
}

function getDefaultMetrics() {
  const m = new metrics.DefaultMetrics();

  m.onAuthenticationError = () => m.onAuthenticationError = (m.onAuthenticationError || 0) + 1;
  m.onClientTimeoutError = () => m.clientTimeoutError = (m.clientTimeoutError || 0) + 1;
  m.onConnectionError = () => m.connectionError = (m.connectionError || 0) + 1;

  m.errors.other.on('increment', () => m.otherError = (m.otherError || 0) + 1);
  m.errors.readTimeout.on('increment', () => m.readTimeoutError = (m.readTimeoutError || 0) + 1);
  m.errors.unavailable.on('increment', () => m.unavailableError = (m.unavailableError || 0) + 1);
  m.errors.writeTimeout.on('increment', () => m.writeTimeoutError = (m.writeTimeoutError || 0) + 1);

  m.onClientTimeoutRetry = () => m.clientTimeoutRetry = (m.clientTimeoutRetry || 0) + 1;
  m.onOtherRetry = () => m.otherRetry = (m.otherRetry || 0) + 1;
  m.onReadTimeoutRetry = () => m.readTimeoutRetry = (m.readTimeoutRetry || 0) + 1;
  m.onUnavailableRetry = () => m.unavailableRetry = (m.unavailableRetry || 0) + 1;
  m.onWriteTimeoutRetry = () => m.writeTimeoutRetry = (m.writeTimeoutRetry || 0) + 1;

  m.onIgnoreError = () => m.ignoredError = (m.ignoredError || 0) + 1;
  m.onSpeculativeExecution = () => m.speculativeExecution = (m.speculativeExecution || 0) + 1;

  m.successfulResponses.on('increment', () => m.successfulResponse = (m.successfulResponse || 0) + 1);
  //m.onSuccessfulResponse = () => m.successfulResponse = (m.successfulResponse || 0) + 1;

  return m;
}

class AlwaysRetryPolicy extends RetryPolicy {
  constructor() {
    super();
    this.unavailable = this.readTimeout = this.readTimeout = this.requestError = 0;
    this._retryResult = {
      decision: RetryPolicy.retryDecision.retry,
      useCurrentHost: false
    };
  }

  onUnavailable() {
    this.unavailable++;
    return this._retryResult;
  }

  onReadTimeout() {
    this.readTimeout++;
    return this._retryResult;
  }

  onWriteTimeout() {
    this.readTimeout++;
    return this._retryResult;
  }

  onRequestError() {
    this.requestError++;
    return this._retryResult;
  }
}