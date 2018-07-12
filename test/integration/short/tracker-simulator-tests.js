'use strict';

const assert = require('assert');
const helper = require('../../test-helper');
const tracker = require('../../../lib/tracker');
const errors = require('../../../lib/errors');
const Client = require('../../../lib/client');
const Host = require('../../../lib/host').Host;
const simulacron = require('../simulacron');

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

  describe('RequestTracker', function () {
    context('when using execute()', () => {
      [ true, false ].forEach(prepare => {
        const requestType = prepare ? 'EXECUTE' : 'QUERY';

        it('should be called when a valid response is obtained for ' + requestType + ' request', () => {
          const requestTracker = new TestTracker();
          const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
          const query = 'SELECT * FROM system.local';
          const parameters = [ 'local' ];
          return client.connect()
            .then(() => client.execute(query, parameters, { prepare }))
            .then(() => verifyResponse(requestTracker, query, parameters))
            .then(() => client.shutdown());
        });
      });

      it('should be called when a error response is obtained', () => {
        const requestTracker = new TestTracker();
        const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
        const query = 'SELECT * FROM system.failing';
        const parameters = [ 'abc' ];
        let err;
        return client.connect()
          .then(() => client.execute(query, parameters))
          .catch(e => {
            verifyError(requestTracker, query, parameters);
            err = e;
          })
          .then(() => helper.assertInstanceOf(err, errors.ResponseError))
          .then(() => client.shutdown());
      });
    });

    context('when using batch()', () => {
      [ true, false ].forEach(prepare => {
        const requestType = prepare ? 'bound statements' : 'queries';

        it('should be called when a valid response is obtained for BATCH request containing ' + requestType, () => {
          const requestTracker = new TestTracker();
          const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
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
        const requestTracker = new TestTracker();
        const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
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
          const requestTracker = new TestTracker();
          const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
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
      const requestTracker = new TestTracker();
      const client = new Client({ contactPoints: [ simulacron.startingIp ], requestTracker });
      return client.connect()
        .then(() => assert.strictEqual(requestTracker.shutdownCalled, 0))
        .then(() => client.shutdown())
        .then(() => assert.strictEqual(requestTracker.shutdownCalled, 1));
    });
  });

  describe('RequestLogger', function () {

  });
});

function verifyResponse(tracker, query, parameters) {
  assert.strictEqual(tracker.responses.length, 1);
  assert.strictEqual(tracker.errors.length, 0);
  const r = tracker.responses[0];

  verifyCommon(r, query, parameters);

  const responseLength = r[5];
  assert.strictEqual(typeof responseLength, 'number');
  assert.ok(responseLength > 0);
}

function verifyError(tracker, query, parameters) {
  assert.strictEqual(tracker.responses.length, 0);
  assert.strictEqual(tracker.errors.length, 1);
  const errorInfo = tracker.errors[0];
  verifyCommon(errorInfo, query, parameters);
}

function verifyCommon(info, query, parameters) {
  helper.assertInstanceOf(info[0], Host);
  if (Array.isArray(query)) {
    assert.deepEqual(info[1].map(x => ({ query: x.query, params: x.params })), query);
  } else {
    assert.deepEqual(info[1], query);
  }
  assert.strictEqual(info[2], parameters);
  assert.ok(info[3]); // options

  const requestLength = info[4];
  const latency = info[info.length - 1];
  assert.strictEqual(typeof requestLength, 'number');

  // latency is an Array with 2 integers: seconds, nanos
  assert.ok(Array.isArray(latency));
  assert.strictEqual(latency.length, 2);
  assert.strictEqual(latency[0], 0);
  assert.ok(latency[1] > 0);
}

class TestTracker extends tracker.RequestTracker {
  constructor() {
    super();
    this.responses = [];
    this.errors = [];
    this.shutdownCalled = 0;
  }

  onSuccess() {
    this.responses.push(Array.prototype.slice.call(arguments));
  }

  onError() {
    this.errors.push(Array.prototype.slice.call(arguments));
  }

  shutdown() {
    this.shutdownCalled++;
  }
}