'use strict';

const helper = require('../test-helper');
const specExecModule = require('../../lib/policies/speculative-execution');

describe('NoSpeculativeExecutionPolicy', () => {
  describe('#getOptions()', () => {
    it('should return an empty Map', () => {
      helper.assertMapEqual(new specExecModule.NoSpeculativeExecutionPolicy().getOptions(), new Map());
    });
  });
});

describe('ConstantSpeculativeExecutionPolicy', () => {
  describe('#getOptions()', () => {
    it('should return a Map with the policy options', () => {
      helper.assertMapEqual(new specExecModule.ConstantSpeculativeExecutionPolicy(200, 1).getOptions(),
        new Map([['delay', 200], ['maxSpeculativeExecutions', 1]]));
    });
  });
});
