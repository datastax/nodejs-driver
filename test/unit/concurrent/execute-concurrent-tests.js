'use strict';

const assert = require('assert');
const types = require('../../../lib/types');
const executeConcurrent = require('../../../lib/concurrent').executeConcurrent;

describe('executeConcurrent(client, query, parameters)', function () {
  this.timeout(10000);

  it('should validate parameters');

  it('should support a higher concurrency level than the number of items', () => {

  });

  it('should execute per each parameter set');

  it('should execute per each parameter set when total items divided by concurrency level is not an integer', () => {
    const concurrencyLevel = 3;
    const testContext = getTestContext();

    const parameters = Array.from(Array(11).keys()).map(x => [ x, 0 ]);

    return executeConcurrent(testContext.client, 'Q1', parameters, { concurrencyLevel, collectResults: true })
      .then(result => {
        assert.ok(result);
        assert.deepStrictEqual(result.errors, []);
        assert.deepStrictEqual(result.resultItems.map(x => x.rows[0]['col1']), parameters);
        assert.strictEqual(testContext.maxInFlight, concurrencyLevel);
      });
  });

  context('with collectResults set', () => {

  });

  context('with collectResults not set', () => {

  });

  context('with raiseOnFirstError set', () => {

  });
});

function getTestContext() {
  const testContext = {
    maxInFlight: 0,
    inFlight: 0,
    client: {
      execute: function (query, parameters, options) {
        testContext.maxInFlight = Math.max(++testContext.inFlight, testContext.maxInFlight);

        return new Promise(r => setTimeout(() => {
          testContext.inFlight--;
          r(new types.ResultSet({ rows: [ { col1: parameters } ]}));
        }, 1));
      }
    }
  };

  return testContext;
}