'use strict';

const assert = require('assert');
const ModelMapper = require('../../../lib/mapper/model-mapper');

const handlerParameters = {
  select: { executor: null, executorCall: null },
  insert: { executor: null, executorCall: null },
  update: { executor: null, executorCall: null }
};
const instance = getInstance(handlerParameters);

describe('ModelMapper', () => {
  describe('#insert()', () => {
    testParameters('insert');
  });

  describe('#update()', () => {
    testParameters('update');
  });

  describe('#find()', () => {
    testParameters('find', 'select');
  });

  describe('#get()', () => {
    testParameters('get', 'select');
  });
});

function testParameters(methodName, handlerMethodName) {
  it('should call the handler to obtain the executor and invoke it', () => {
    const doc = { a: 1};
    const docInfo = { b: 2 };
    const executionOptions = { c : 3 };

    handlerMethodName = handlerMethodName || methodName;

    return instance[methodName](doc, docInfo, executionOptions)
      .then(() => {
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo });
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo, executionOptions });
      });
  });

  it('should set the executionOptions when the second parameter is a string', () => {
    const doc = { a: 100 };
    const executionOptions = 'exec-profile';

    return instance[methodName](doc, executionOptions)
      .then(() => {
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo: null });
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo: null, executionOptions });
      });
  });

  it('should set the executionOptions when the third parameter is a string', () => {
    const doc = { a: 10 };
    const docInfo = { b: 20 };
    const executionOptions = 'exec-profile2';

    return instance[methodName](doc, docInfo, executionOptions)
      .then(() => {
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo });
        assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo, executionOptions });
      });
  });
}

function getInstance(handlerParameters) {
  return new ModelMapper('abc', {
    getInsertExecutor: (doc, docInfo) => {
      handlerParameters.insert.executor = { doc, docInfo };
      return Promise.resolve((doc, docInfo, executionOptions) => {
        handlerParameters.insert.executorCall = { doc, docInfo, executionOptions};
        return {};
      });
    },
    getUpdateExecutor: (doc, docInfo) => {
      handlerParameters.update.executor = { doc, docInfo };
      return Promise.resolve((doc, docInfo, executionOptions) => {
        handlerParameters.update.executorCall = { doc, docInfo, executionOptions};
        return {};
      });
    },
    getSelectExecutor: (doc, docInfo) => {
      handlerParameters.select.executor = { doc, docInfo };
      return Promise.resolve((doc, docInfo, executionOptions) => {
        handlerParameters.select.executorCall = { doc, docInfo, executionOptions};
        return { first: () => null };
      });
    }
  });
}