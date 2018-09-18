'use strict';

const RequestLogger = require('../../lib/tracker').RequestLogger;
const helper = require('../test-helper');
const assert = require('assert');
const ExecutionInfo = require('../../lib/execution-info').ExecutionInfo;

describe('RequestLogger', () => {
  describe('#onSuccess()', () => {
    context('when request is slow', () => {
      const logger = new RequestLogger({ slowThreshold: 200 });
      const loggerShortMessage = new RequestLogger({
        slowThreshold: 200, messageMaxQueryLength: 20, messageMaxParameterValueLength: 5 });

      let message;
      const handler = (m) => message = m;

      logger.emitter.on('slow', handler);
      loggerShortMessage.emitter.on('slow', handler);

      beforeEach(() => message = null);

      it('should include the query and parameters in the log message', () => {
        const query = 'INSERT EXAMPLE';
        logger.onSuccess({ address: '::1' }, query, ['a', true, 1.01], getExecutionInfo(false), 10, 20, [ 1, 2034125 ]);
        helper.assertContains(message, query, true);
        helper.assertContains(message, 'Slow request, took 1002 ms');
        helper.assertContains(message, '[a,true,1.01]');
        helper.assertContains(message, 'not prepared');
      });

      it('should stringify object parameters in the log message', () => {
        const query = 'UPDATE EXAMPLE';
        logger.onSuccess({ address: '::1' }, query, { a: 1, b: true, c: 1.01 }, getExecutionInfo(), 10, 20, [ 1, 0 ]);
        helper.assertContains(message, query, true);
        helper.assertContains(message, 'Slow request, took 1000 ms');
        helper.assertContains(message, '[a:1,b:true,c:1.01]');
        helper.assertContains(message, 'not prepared');
      });

      it('should include size information and round to KB', () => {
        logger.onSuccess({ address: '::1' }, 'Q', [], getExecutionInfo(), 12700, 231, [ 1, 0 ]);
        helper.assertContains(message, 'request size 12 KB / response size 231 bytes');
      });

      it('should include batch information and queries', () => {
        const queries = [ { query: 'Q1' }, { query: 'Q2', params: [ 'a', 'b' ]}];
        logger.onSuccess({ address: '::1' }, queries, null, getExecutionInfo(), 80, 231, [ 1, 0 ]);
        helper.assertContains(message, 'request size 80 bytes / response size 231 bytes');
        helper.assertContains(message, 'BATCH w/ 2 not prepared queries (Q1 [],Q2 [a,b])');
      });

      it('should include batch information and truncate queries info', () => {
        const queries = [ { query: 'INSERT1' }, { query: 'INSERT2' }, { query: 'INSERT3', params: [ 'a', 'b' ]}];
        loggerShortMessage.onSuccess({ address: '::1' }, queries, null, getExecutionInfo(), 1, 1, [ 1, 0 ]);
        helper.assertContains(message, 'BATCH w/ 3 not prepared queries (INSERT1 [],INSERT2 [...],...)');
      });

      it('should truncate query in the message when query is too long', () => {
        const query = 'INSERT EXECUTE A B C this_should_not_be_included';
        loggerShortMessage.onSuccess({ address: '::1' }, query, ['a', true], getExecutionInfo(true), 1, 2, [ 2, 0 ]);
        helper.assertContains(message, 'Slow request, took 2000 ms');
        helper.assertContains(message, 'INSERT EXECUTE A B C [...]');
        assert.strictEqual(message.indexOf('this_should_not_be_included'), -1);
      });

      it('should truncate parameters in the message when parameters are too long', () => {
        const query = 'INSERT';
        const params = ['abcdefghijk', 1];
        loggerShortMessage.onSuccess({ address: '::1' }, query, params, getExecutionInfo(true), 1, 2, [ 2, 0 ]);
        helper.assertContains(message, 'Slow request, took 2000 ms');
        helper.assertContains(message, 'INSERT [abcde,1]');
      });

      it('should truncate parameters in the message when query and parameters are too long', () => {
        const query = 'INSERT SAMPLE';
        const params = ['abcde', true, 1];
        loggerShortMessage.onSuccess({ address: '::1' }, query, params, getExecutionInfo(true), 1, 2, [ 2, 0 ]);
        helper.assertContains(message, 'Slow request, took 2000 ms');
        helper.assertContains(message, 'INSERT SAMPLE [abcde,...]');
      });
    });

    context('when request is large', () => {
      const logger = new RequestLogger({ requestSizeThreshold: 500 });
      let message;
      logger.emitter.on('large', m => message = m);

      beforeEach(() => message = null);

      it('should include the query and parameters in the log message', () => {
        const query = 'INSERT EXAMPLE';
        logger.onSuccess({ address: '::1' }, query, ['a', true, 1.01], getExecutionInfo(false), 1000, 20, [ 0, 201 ]);
        helper.assertContains(message, query, true);
        helper.assertContains(message, 'Request exceeded length');
        helper.assertContains(message, 'INSERT EXAMPLE [a,true,1.01]');
        helper.assertContains(message, 'not prepared');
      });

      it('should not include query preparation info when has been prepared', () => {
        const query = 'UPDATE EXAMPLE';
        logger.onSuccess({ address: '::1' }, query, [], getExecutionInfo(true), 1000, 20, [ 0, 201 ]);
        helper.assertContains(message, query, true);
        helper.assertContains(message, 'Request exceeded length');
        assert.strictEqual(message.indexOf('prepared'), -1);
      });
    });

    context('when request is normal', () => {

      const logger = new RequestLogger({ slowThreshold: 200, requestSizeThreshold: 500 });

      let message;
      const otherMessages = [];
      const handler = (m) => otherMessages.push(m);

      logger.emitter.on('normal', m => message = m);
      logger.emitter.on('slow', handler)
        .on('large', handler)
        .on('failure', handler);

      beforeEach(() => {
        message = null;
        otherMessages.length = 0;
      });

      it('should not log normal requests by default', () => {
        logger.onSuccess({ address: '::1' }, 'QUERY', [], getExecutionInfo(), 1, 1, [ 0, 1 ]);
        // Disabled initially
        assert.strictEqual(message, null);
        assert.strictEqual(otherMessages.length, 0);
      });

      it('should log normal requests when logNormalRequests is enabled', () => {
        logger.logNormalRequests = true;
        const query = 'SELECT EXAMPLE';
        logger.onSuccess({ address: '::1' }, query, [], getExecutionInfo(), 1, 1, [ 0, 1 ]);
        assert.deepEqual(otherMessages, []);
        helper.assertContains(message, query, true);
        helper.assertContains(message, 'Request completed normally');
        helper.assertContains(message, 'SELECT EXAMPLE []');
        helper.assertContains(message, 'not prepared');
      });
    });
  });

  describe('#onError()', () => {
    const logger = new RequestLogger({ requestSizeThreshold: 500, logErroredRequests: true });
    let largeMessage;
    let errorMessage;
    logger.emitter.on('large', m => largeMessage = m);
    logger.emitter.on('failure', m => errorMessage = m);

    beforeEach(() => {
      largeMessage = null;
      errorMessage = null;
      logger.logErroredRequests = true;
    });

    context('when request is large', () => {
      it('should include stack trace when large request failed', () => {
        const err = new Error('Sample error');
        logger.onError({ address: '::1' }, 'Q', [1, 2, 'a'], getExecutionInfo(), 27891, err, [ 1, 0 ]);
        assert.strictEqual(errorMessage, null);
        helper.assertContains(largeMessage, 'Request exceeded length and execution failed');
        helper.assertContains(largeMessage, 'request size 27 KB');
        helper.assertContains(largeMessage, '[1,2,a]');
        helper.assertContains(largeMessage, err.stack.substr(0, 200));
      });
    });

    context('when logErroredRequests is enabled initially', () => {
      it('should include stack trace when large request failed', () => {
        const err = new Error('Sample error');
        logger.onError({ address: '::1' }, 'Q', [1, 2, 'a'], getExecutionInfo(), 80, err, [ 1, 0 ]);
        assert.strictEqual(largeMessage, null);
        helper.assertContains(errorMessage, 'Request execution failed');
        helper.assertContains(errorMessage, err.stack.substr(0, 200));
      });

      it('should allow to be disabled using logErroredRequests property', () => {
        const err = new Error('Sample error');
        logger.onError({ address: '::1' }, 'Q', [1, 2, 'a'], getExecutionInfo(), 80, err, [ 1, 0 ]);
        assert.strictEqual(typeof errorMessage, 'string');

        // Disable it
        logger.logErroredRequests = false;
        errorMessage = null;
        logger.onError({ address: '::1' }, 'Q', [1, 2, 'a'], getExecutionInfo(), 80, err, [ 1, 0 ]);
        assert.strictEqual(errorMessage, null);
      });
    });
  });
});

function getExecutionInfo(prepare) {
  const info = ExecutionInfo.empty();
  info.getIsPrepared = () => prepare;
  return info;
}