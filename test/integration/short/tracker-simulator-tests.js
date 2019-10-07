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

'use strict';

const assert = require('chai').assert;
const sinon = require('sinon');

const tracker = require('../../../lib/tracker');
const errors = require('../../../lib/errors');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const Client = require('../../../lib/client');
const Host = require('../../../lib/host').Host;
const simulacron = require('../simulacron');

const queryDelayed = 'INSERT INTO delayed (id) VALUES (?)';

describe('tracker', function () {
  this.timeout(5000);
  const setupInfo = simulacron.setup('1', { initClient: false });

  beforeEach(done => setupInfo.cluster.prime({
    when: { query: 'SELECT * FROM system.failing' },
    then: {
      result: 'syntax_error',
      message: 'Invalid Syntax!',
    }
  }, done));

  beforeEach(done => setupInfo.cluster.prime({
    when: { query: queryDelayed },
    then: { result: 'success', delay_in_ms: 200 }
  }, done));

  describe('RequestTracker', function () {
    context('when using execute()', () => {
      [ true, false ].forEach(prepare => {
        const requestType = prepare ? 'EXECUTE' : 'QUERY';

        it('should be called when a valid response is obtained for ' + requestType + ' request', () => {
          const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
          const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
          const query = 'SELECT * FROM system.local';
          const parameters = [ 'local' ];
          return client.connect()
            .then(() => client.execute(query, parameters, { prepare }))
            .then(() => verifyResponse(requestTracker, query, parameters))
            .then(() => client.shutdown());
        });
      });

      it('should be called when a error response is obtained', () => {
        const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
        const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
        const query = 'SELECT * FROM system.failing';
        const parameters = [ 'abc' ];
        let err;
        return client.connect()
          .then(() => client.execute(query, parameters))
          .catch(e => {
            verifyError(requestTracker, query, parameters);
            err = e;
          })
          .then(() => assert.instanceOf(err, errors.ResponseError))
          .then(() => client.shutdown());
      });
    });

    context('when using batch()', () => {
      [ true, false ].forEach(prepare => {
        const requestType = prepare ? 'bound statements' : 'queries';

        it('should be called when a valid response is obtained for BATCH request containing ' + requestType, () => {
          const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
          const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
          const queries = [
            { query: 'SELECT * FROM system.local WHERE key = ?', params: [] }
          ];

          return client.connect()
            .then(() => client.batch(queries, { prepare }))
            .then(() => verifyResponse(requestTracker, queries))
            .then(() => client.shutdown());
        });
      });

      it('should be called when a error response is obtained', () => {
        const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
        const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
        const query = 'SELECT INVALID';
        const parameters = [ 'abc' ];
        return client.connect()
          .then(() => client.execute(query, parameters))
          .catch(() => verifyError(requestTracker, query, parameters))
          .then(() => client.shutdown());
      });
    });

    context('when using eachRow()', () => {
      [ true, false ].forEach(prepare => {
        const requestType = prepare ? 'EXECUTE' : 'QUERY';

        it('should be called when a valid response is obtained for ' + requestType + ' request', () => {
          const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
          const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
          const query = 'SELECT * FROM system.local';
          const parameters = ['local'];
          return client.connect()
            .then(() => new Promise((resolve, reject) => {
              client.eachRow(query, parameters, { prepare }, () => {}, (err, rs) => (err ? reject(err) : resolve(rs)));
            }))
            .then(() => verifyResponse(requestTracker, query, parameters))
            .then(() => client.shutdown());
        });
      });
    });

    it('should be called when client is being shutdown', () => {
      const requestTracker = sinon.createStubInstance(tracker.RequestTracker);
      const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker });
      return client.connect()
        .then(() => assert.strictEqual(requestTracker.shutdown.callCount, 0))
        .then(() => client.shutdown())
        .then(() => assert.strictEqual(requestTracker.shutdown.callCount, 1));
    });
  });

  describe('RequestLogger', function () {
    const logger = new tracker.RequestLogger({ slowThreshold: 50, requestSizeThreshold: 1000 });
    const client = new Client({ contactPoints: [ simulacron.startingIp ], localDataCenter: 'dc1', requestTracker: logger });

    before(() => client.connect());
    after(() => client.shutdown());

    const slowMessages = [];
    const largeMessages = [];

    beforeEach(() => {
      slowMessages.length = 0;
      largeMessages.length = 0;
    });

    logger.emitter.on('slow', m => slowMessages.push(m));
    logger.emitter.on('large', m => largeMessages.push(m));

    context('when using execute()', () => {
      it('should log slow queries', () => {
        const id = types.Uuid.random();

        return client.execute(queryDelayed, [ id ], { prepare: true })
          .then(() => {
            assert.strictEqual(slowMessages.length, 1);
            assert.strictEqual(largeMessages.length, 0);
            assert.match(slowMessages[0], /Slow request, took/);
            assert.include(slowMessages[0], `${queryDelayed} [${id}]`);
          });
      });

      it('should log large queries', () => {
        const query = 'INSERT INTO table1 (id) VALUES (?)';
        const params = [ utils.stringRepeat('a', 2000) ];

        return client.execute(query, params, { prepare: true })
          .then(() => {
            assert.strictEqual(slowMessages.length, 0);
            assert.strictEqual(largeMessages.length, 1);
            assert.include(largeMessages[0], 'Request exceeded length');
            assert.include(largeMessages[0], query);
          });
      });
    });

    context('when using batch()', () => {
      [true, false].forEach(logged => {
        it('should log large ' + (logged ? 'logged' : 'unlogged') + ' batches', () => {
          const query = 'INSERT INTO table1 (id) VALUES (?)';
          const queries = [{ query, params: [ utils.stringRepeat('a', 2000) ] }];

          return client.batch(queries, { prepare: true, logged })
            .then(() => {
              assert.strictEqual(slowMessages.length, 0);
              assert.strictEqual(largeMessages.length, 1);
              assert.match(largeMessages[0], /Request exceeded length/);
              if (logged) {
                assert.include(largeMessages[0], ': LOGGED BATCH w/ 1 queries (' + query);
              }
              else {
                assert.include(largeMessages[0], ': BATCH w/ 1 queries (' + query);
              }
            });
        });
      });
    });
  });
});

function verifyResponse(tracker, query, parameters) {
  assert.isTrue(tracker.onSuccess.calledOnce);
  assert.isTrue(tracker.onError.notCalled);

  const r = tracker.onSuccess.getCall(0).args;

  verifyCommon(r, query, parameters);

  const responseLength = r[5];
  assert.typeOf(responseLength, 'number');
  assert.isAbove(responseLength, 0);
}

function verifyError(tracker, query, parameters) {
  assert.isTrue(tracker.onSuccess.notCalled);
  assert.isTrue(tracker.onError.calledOnce);

  const errorInfo = tracker.onError.getCall(0).args;
  verifyCommon(errorInfo, query, parameters);
}

function verifyCommon(info, query, parameters) {
  assert.instanceOf(info[0], Host);
  if (Array.isArray(query)) {
    assert.deepEqual(info[1].map(x => ({ query: x.query, params: x.params })), query);
  } else {
    assert.deepEqual(info[1], query);
  }
  assert.strictEqual(info[2], parameters);
  assert.ok(info[3]); // options

  const requestLength = info[4];
  const latency = info[info.length - 1];
  assert.typeOf(requestLength, 'number');

  // latency is an Array with 2 integers: seconds, nanos
  assert.isArray(latency);
  assert.strictEqual(latency.length, 2);
  assert.strictEqual(latency[0], 0);
  assert.isAbove(latency[1], 0);
}