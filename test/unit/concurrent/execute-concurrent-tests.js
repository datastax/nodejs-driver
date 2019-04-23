/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const assert = require('assert');
const Readable = require('stream').Readable;
const types = require('../../../lib/types');
const utils = require('../../../lib/utils');
const helper = require('../../test-helper');
const executeConcurrent = require('../../../lib/concurrent').executeConcurrent;

describe('executeConcurrent(client, query, parameters)', function () {
  this.timeout(10000);

  it('should validate parameters', () => {
    assert.throws(() => executeConcurrent(null), TypeError, /Client instance is not defined/);

    assert.throws(() => executeConcurrent({}, null), TypeError,
      /A string query or query and parameters array should be provided/);

    assert.throws(() => executeConcurrent({}, {}), TypeError,
      /A string query or query and parameters array should be provided/);

    assert.throws(() => executeConcurrent({}, 'SELECT ...'), TypeError,
      /parameters should be an Array or a Stream instance/);

    assert.throws(() => executeConcurrent({}, 'SELECT ...', {}), TypeError,
      /parameters should be an Array or a Stream instance/);
  });

  it('should support a higher concurrency level than the number of items', () => {
    const testContext = getTestContext();
    const parameters = Array.from(Array(100).keys()).map(x => [ x, 0 ]);
    const concurrencyLevel = Number.MAX_SAFE_INTEGER;

    return executeConcurrent(testContext.client, 'Q1', parameters, { concurrencyLevel, collectResults: true })
      .then(result => {
        assert.ok(result);
        assert.deepStrictEqual(result.errors, []);
        assert.deepStrictEqual(result.resultItems.map(x => x.rows[0]['col1']), parameters);
        assert.strictEqual(testContext.maxInFlight, parameters.length);
        assert.strictEqual(result.totalExecuted, parameters.length);
      });
  });

  it('should execute per each parameter set', () => {
    const testContext = getTestContext();
    const parameters = Array.from(Array(10).keys()).map(x => [ x, 0 ]);
    const concurrencyLevel = 5;

    return executeConcurrent(testContext.client, 'Q1', parameters, { concurrencyLevel, collectResults: true })
      .then(result => {
        assert.ok(result);
        assert.deepStrictEqual(result.errors, []);
        assert.deepStrictEqual(result.resultItems.map(x => x.rows[0]['col1']), parameters);
        assert.strictEqual(testContext.maxInFlight, concurrencyLevel);
      });
  });

  it('should execute per each parameter set when total items divided by concurrency level is not an integer', () => {
    const testContext = getTestContext();
    const parameters = Array.from(Array(11).keys()).map(x => [ x, 0 ]);
    const concurrencyLevel = 3;

    return executeConcurrent(testContext.client, 'Q1', parameters, { concurrencyLevel, collectResults: true })
      .then(result => {
        assert.ok(result);
        assert.deepStrictEqual(result.errors, []);
        assert.deepStrictEqual(result.resultItems.map(x => x.rows[0]['col1']), parameters);
        assert.strictEqual(testContext.maxInFlight, concurrencyLevel);
      });
  });

  it('should support empty parameters array', () => {
    const testContext = getTestContext();

    return executeConcurrent(testContext.client, 'Q1', [])
      .then(result => assert.strictEqual(result.totalExecuted, 0));
  });

  it('should set the execution profile', () => {
    const testContext = getTestContext();
    const options = { executionProfile: 'ep1' };

    return executeConcurrent(testContext.client, 'INSERT ...', [ [1], [2] ], options)
      .then(result => {
        assert.strictEqual(result.totalExecuted, 2);
        assert.strictEqual(testContext.executions.length, 2);
        testContext.executions.forEach(item => {
          assert.strictEqual(item.options.executionProfile, options.executionProfile);
        });
      });
  });

  context('when collectResults is true', () => {

    it('should set the resultItems', () => {
      const testContext = getTestContext();
      const parameters = Array.from(Array(200).keys()).map(x => [ x, x ]);
      const options = { collectResults: true, concurrencyLevel: 8 };

      return executeConcurrent(testContext.client, 'Q1', parameters, options)
        .then(result => {
          assert.strictEqual(testContext.maxInFlight, options.concurrencyLevel);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, parameters.length);
          assert.strictEqual(result.errors.length, 0);
          assert.strictEqual(result.resultItems.length, parameters.length);

          result.resultItems.forEach((rs, index) => {
            helper.assertInstanceOf(rs, types.ResultSet);
            assert.ok(Array.isArray(rs.rows));
            assert.deepStrictEqual(rs.rows[0]['col1'], [ index, index ]);
          });
        });
    });
  });

  context('when collectResults is false', () => {
    it('should throw when accessing resultItems property', () => {
      const testContext = getTestContext();
      const parameters = Array.from(Array(10).keys()).map(x => [ x, 0 ]);

      return executeConcurrent(testContext.client, 'Q1', parameters)
        .then(result => assert.throws(() => result.resultItems,
          /Property resultItems can not be accessed when collectResults is set to false/));
    });
  });

  context('when raiseOnFirstError is true', () => {
    it('should throw when first error is encountered', () => {
      const testContext = getTestContext([ 53, 80 ]);
      const concurrencyLevel = 10;
      const parameters = Array.from(Array(100).keys()).map(x => [ x, 0 ]);

      let error;

      return executeConcurrent(testContext.client, 'Q1', parameters, { raiseOnFirstError: true, concurrencyLevel })
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, Error);
          assert.strictEqual(error.message, 'Test error 53');
          return new Promise(r => setTimeout(r, 100));
        })
        // It should not continue executing
        .then(() => assert.ok(testContext.index < 70));
    });
  });

  context('when raiseOnFirstError is false', () => {
    it('should continue executing when first error is encountered', () => {
      const errorIndexes = [ 23, 81 ];
      const testContext = getTestContext(errorIndexes);
      const parameters = Array.from(Array(100).keys()).map(x => [ x, 0 ]);

      let error;

      return executeConcurrent(testContext.client, 'Q1', parameters, { raiseOnFirstError: false })
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, parameters.length);
          assert.strictEqual(result.errors.length, 2);
          assert.deepStrictEqual(result.errors.map(e => e.message), errorIndexes.map(i => `Test error ${i}`) );
        });
    });

    it('should set the errors in the resultItems when collectResults is set to true', () => {
      const errorIndexes = [ 13, 70 ];
      const testContext = getTestContext(errorIndexes);
      const parameters = Array.from(Array(100).keys()).map(x => [ x, 0 ]);
      const options = { raiseOnFirstError: false, collectResults: true, concurrencyLevel: 8 };

      let error;

      return executeConcurrent(testContext.client, 'Q1', parameters, options)
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, parameters.length);
          assert.strictEqual(result.errors.length, 2);
          assert.deepStrictEqual(result.errors.map(e => e.message), errorIndexes.map(i => `Test error ${i}`) );
          assert.strictEqual(result.resultItems.length, parameters.length);

          result.resultItems.forEach((rs, index) => {
            if (errorIndexes.indexOf(index) === -1) {
              helper.assertInstanceOf(rs, types.ResultSet);
              assert.ok(Array.isArray(rs.rows));
            } else {
              helper.assertInstanceOf(rs, Error);
              helper.assertContains(rs.message, 'Test error');
            }
          });
        });
    });

    it('should stop collecting errors when maxErrors is exceeded', () => {
      const errorIndexes = Array.from(Array(100).keys()).slice(31, 46);
      const testContext = getTestContext(errorIndexes);
      const parameters = Array.from(Array(100).keys()).map(x => [ x, x ]);
      const options = { raiseOnFirstError: false, maxErrors: 10 };

      let error;

      return executeConcurrent(testContext.client, 'Q1', parameters, options)
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, parameters.length);
          assert.strictEqual(result.errors.length, options.maxErrors);
          assert.deepStrictEqual(
            result.errors.map(e => e.message),
            errorIndexes.slice(0, 10).map(i => `Test error ${i}`) );
        });
    });
  });
});

describe('executeConcurrent(client, query, stream)', () => {
  it('should support an empty stream', () => {
    const testContext = getTestContext();
    const stream = new TestStream([]);

    return executeConcurrent(testContext.client, 'INSERT...', stream)
      .then(result => {
        assert.strictEqual(result.totalExecuted, 0);
        assert.strictEqual(testContext.index, 0);
      });
  });

  it('should throw when the stream emits an error', () => {
    const testContext = getTestContext();
    const testError = new Error('Test error');
    const stream = new TestStream([[[1, 2], [3, 4]], [[5, 6], testError, [9, 10]]]);
    let error;

    return executeConcurrent(testContext.client, 'INSERT...', stream)
      .catch(err => error = err)
      .then(() => {
        assert.strictEqual(error, testError);
      });
  });

  [
    { params: [['a']], description: 'when there is an invalid item on the first position' },
    { params: [[[1, 2], 'a']], description: 'when there is an invalid item on the second position' },
    { params: [[[1, 2], [2, 3]], ['a']], description: 'when there is an invalid item pushed later in time' }
  ].forEach(item => {
    it(`should throw when items are not Array instances ${item.description}`, () => {
      const testContext = getTestContext();
      const stream = new TestStream(item.params);
      let error;

      return executeConcurrent(testContext.client, 'INSERT...', stream)
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, TypeError);
          assert.strictEqual(error.message, 'Stream should be in objectMode and should emit Array instances');
        });
    });
  });

  it('should execute one query per item in the stream', () => {
    const testContext = getTestContext();
    const stream = new TestStream([[[1, 2], [3, 4]], [[5, 6], [7, 8], [9, 10]]], { endAsync: true });

    return executeConcurrent(testContext.client, 'INSERT...', stream)
      .then(result => {
        assert.strictEqual(result.totalExecuted, 5);
        assert.strictEqual(testContext.index, 5);
      });
  });

  it('should support ending right after the last item is read', () => {
    const testContext = getTestContext();
    const stream = new TestStream([[['a', 'b'], ['c', 'd']], [['e', 'f']]], { endAsync: false });

    return executeConcurrent(testContext.client, 'INSERT...', stream)
      .then(result => {
        assert.strictEqual(result.totalExecuted, 3);
        assert.strictEqual(testContext.index, 3);
      });
  });

  it('should stop reading according to the concurrency level', () => {
    const testContext = getTestContext();
    const values = Array.from(new Array(100).keys()).map(x => [x]);
    const stream = new TestStream([ values ]);
    const concurrencyLevel = 8;

    return executeConcurrent(testContext.client, 'INSERT...', stream, { concurrencyLevel })
      .then(result => {
        assert.strictEqual(result.totalExecuted, values.length);
        assert.strictEqual(testContext.index, values.length);
        // We should validate that there was a high number of pauses.
        // The exact number of pauses depends on how fast client.execute() is
        assert.ok(stream.pauseCounter > Math.floor(values.length / concurrencyLevel) - 1);
      });
  });

  context('when collectResults is false', () => {
    it('should throw when accessing resultItems property', () => {
      const testContext = getTestContext();
      const stream = new TestStream([[['a', 'b'], ['c', 'd']], [['e', 'f']]], { endAsync: false });

      return executeConcurrent(testContext.client, 'Q1', stream, { collectResults: false })
        .then(result => assert.throws(() => result.resultItems,
          /Property resultItems can not be accessed when collectResults is set to false/));
    });
  });

  context('when collectResults is true', () => {
    it('should set the resultItems', () => {
      const testContext = getTestContext();
      const values = Array.from(new Array(100).keys()).map(x => [x]);
      const stream = new TestStream([ values.slice(0, 2), values.slice(2, 10), values.slice(10) ]);

      return executeConcurrent(testContext.client, 'INSERT...', stream, { concurrencyLevel: 8, collectResults: true })
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.strictEqual(result.errors.length, 0);
          assert.ok(stream.pauseCounter > 0);
          result.resultItems.forEach((rs, index) => {
            helper.assertInstanceOf(rs, types.ResultSet);
            assert.ok(Array.isArray(rs.rows));
            assert.deepStrictEqual(rs.rows[0]['col1'], [ index ]);
          });
        });
    });
  });

  context('when raiseOnFirstError is true', () => {
    it('should throw when first error is encountered', () => {
      const testContext = getTestContext([23, 81]);
      const values = Array.from(new Array(100).keys()).map(x => [x]);
      const stream = new TestStream([ values ]);
      const concurrencyLevel = 5;
      let error;

      return executeConcurrent(testContext.client, 'Q1', stream, { raiseOnFirstError: true, concurrencyLevel })
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, Error);
          assert.strictEqual(error.message, 'Test error 23');
          return new Promise(r => setTimeout(r, 100));
        })
        // It should not continue executing
        .then(() => assert.ok(testContext.index < 70));
    });
  });

  context('when raiseOnFirstError is false', () => {
    it('should continue executing when first error is encountered', () => {
      const errorIndexes = [ 23, 81 ];
      const testContext = getTestContext(errorIndexes);
      const values = Array.from(Array(100).keys()).map(x => [ x, 0 ]);
      const stream = new TestStream([ values ]);

      let error;

      return executeConcurrent(testContext.client, 'Q1', stream, { raiseOnFirstError: false })
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, values.length);
          assert.strictEqual(result.errors.length, 2);
          assert.deepStrictEqual(result.errors.map(e => e.message), errorIndexes.map(i => `Test error ${i}`) );
        });
    });

    it('should set the errors in the resultItems when collectResults is set to true', () => {
      const errorIndexes = [ 13, 70 ];
      const testContext = getTestContext(errorIndexes);
      const values = Array.from(Array(100).keys()).map(x => [ x, 0 ]);
      const stream = new TestStream([ values ]);
      const options = { raiseOnFirstError: false, collectResults: true, concurrencyLevel: 8 };
      let error;

      return executeConcurrent(testContext.client, 'Q1', stream, options)
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, values.length);
          assert.strictEqual(result.errors.length, 2);
          assert.deepStrictEqual(result.errors.map(e => e.message), errorIndexes.map(i => `Test error ${i}`) );
          assert.strictEqual(result.resultItems.length, values.length);

          result.resultItems.forEach((rs, index) => {
            if (errorIndexes.indexOf(index) === -1) {
              helper.assertInstanceOf(rs, types.ResultSet);
              assert.ok(Array.isArray(rs.rows));
            } else {
              helper.assertInstanceOf(rs, Error);
              helper.assertContains(rs.message, 'Test error');
            }
          });
        });
    });

    it('should stop collecting errors when maxErrors is exceeded', () => {
      const errorIndexes = Array.from(Array(100).keys()).slice(31, 46);
      const testContext = getTestContext(errorIndexes);
      const values = Array.from(Array(100).keys()).map(x => [ x, x ]);
      const stream = new TestStream([ values.slice(0, 10), values.slice(10) ]);
      const options = { raiseOnFirstError: false, maxErrors: 10 };
      let error;

      return executeConcurrent(testContext.client, 'Q1', stream, options)
        .catch(err => error = err)
        .then(result => {
          assert.strictEqual(error, undefined);
          assert.ok(result);
          assert.strictEqual(result.totalExecuted, values.length);
          assert.strictEqual(result.errors.length, options.maxErrors);
          assert.deepStrictEqual(
            result.errors.map(e => e.message),
            errorIndexes.slice(0, 10).map(i => `Test error ${i}`) );
        });
    });
  });
});

describe('executeConcurrent(client, queryAndParameters)', () => {
  it('should use the different query and parameters', () => {
    const queryAndParams = [
      { query: 'Q1', params: ['a'] },
      { query: 'Q2', params: ['b'] },
      { query: 'Q3', params: ['c'] },
    ];

    const testContext = getTestContext();

    return executeConcurrent(testContext.client, queryAndParams)
      .then(result => {
        assert.strictEqual(result.totalExecuted, queryAndParams.length);
        assert.deepStrictEqual(testContext.executions.map(x => ({ query: x.query, params: x.params })), queryAndParams);
      });
  });

  it('should set the execution profile', () => {
    const testContext = getTestContext();
    const queryAndParams = [
      { query: 'Q1', params: ['a'] },
      { query: 'Q2', params: ['b'] },
    ];

    const options = { executionProfile: 'ep1' };

    return executeConcurrent(testContext.client, queryAndParams, options)
      .then(result => {
        assert.strictEqual(result.totalExecuted, queryAndParams.length);
        testContext.executions.forEach(item => {
          assert.strictEqual(item.options.executionProfile, options.executionProfile);
        });
      });
  });
});

function getTestContext(errorIndexes) {
  errorIndexes = errorIndexes || [];

  const testContext = {
    maxInFlight: 0,
    inFlight: 0,
    index: 0,
    executions: [],
    client: {
      execute: function (query, params, options) {
        const index = testContext.index++;
        testContext.maxInFlight = Math.max(++testContext.inFlight, testContext.maxInFlight);
        testContext.executions.push({ query, params, options });

        return new Promise((resolve, reject) => setTimeout(() => {
          testContext.inFlight--;

          if (errorIndexes.indexOf(index) === -1) {
            resolve(new types.ResultSet({ rows: [ { col1: params } ]}));
          } else {
            reject(new Error(`Test error ${index}`));
          }
        }, 1));
      }
    }
  };

  return testContext;
}

class TestStream extends Readable {

  constructor(values, options) {
    super({ objectMode: true });

    options = options || utils.emptyObject;
    this.pauseCounter = 0;
    this.resumeCounter = 0;
    const baseDelay = 20;
    const endAsync = typeof options.endAsync === 'boolean' ? options.endAsync : true;

    if (values) {
      values.forEach((parametersGroups, i) => {
        setTimeout(() => {
          parametersGroups.forEach(parameters => {
            if (parameters instanceof Error) {
              return this.emit('error', parameters);
            }

            this.push(parameters);
          });

          if (i === values.length - 1 && !endAsync) {
            this.push(null);
          }
        }, baseDelay * i);
      });

      if (endAsync) {
        // Signal end on timeout
        setTimeout(() => this.push(null), baseDelay * values.length);
      }
    }
  }

  _read() {

  }

  pause() {
    this.pauseCounter++;
    super.pause();
  }
}