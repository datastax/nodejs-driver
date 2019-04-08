'use strict';

const assert = require('assert');
const types = require('../../../lib/types');
const helper = require('../../test-helper');
const executeConcurrent = require('../../../lib/concurrent').executeConcurrent;

describe('executeConcurrent(client, query, parameters)', function () {
  this.timeout(10000);

  it('should validate parameters');

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

  context('with collectResults set to true', () => {

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

  context('with collectResults set to false', () => {
    it('should throw when accessing resultItems property', () => {
      const testContext = getTestContext();
      const parameters = Array.from(Array(10).keys()).map(x => [ x, 0 ]);

      return executeConcurrent(testContext.client, 'Q1', parameters, { collectResults: false })
        .then(result => assert.throws(() => result.resultItems,
          /Property resultItems can not be accessed when collectResults is set to false/));
    });
  });

  context('with raiseOnFirstError set to true', () => {
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

  context('with raiseOnFirstError set to false', () => {
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

function getTestContext(errorIndexes) {
  errorIndexes = errorIndexes || [];

  const testContext = {
    maxInFlight: 0,
    inFlight: 0,
    index: 0,
    client: {
      execute: function (query, parameters, options) {
        const index = testContext.index++;
        testContext.maxInFlight = Math.max(++testContext.inFlight, testContext.maxInFlight);

        return new Promise((resolve, reject) => setTimeout(() => {
          testContext.inFlight--;

          if (errorIndexes.indexOf(index) === -1) {
            resolve(new types.ResultSet({ rows: [ { col1: parameters } ]}));
          } else {
            reject(new Error(`Test error ${index}`));
          }
        }, 1));
      }
    }
  };

  return testContext;
}