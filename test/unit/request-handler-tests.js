var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var RequestHandler = require('../../lib/request-handler.js');
var helper = require('../test-helper.js');
var errors = require('../../lib/errors.js');
var types = require('../../lib/types');
var utils = require('../../lib/utils.js');
var retry = require('../../lib/policies/retry.js');

var options = (function () {
  var loadBalancing = require('../../lib/policies/load-balancing.js');
  var reconnection = require('../../lib/policies/reconnection.js');
  return {
    policies: {
      loadBalancing: new loadBalancing.RoundRobinPolicy(),
      reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
      retry: new retry.RetryPolicy()
    }
  };
})();
describe('RequestHandler', function () {
  describe('#handleError()', function () {
    it('should retrow on syntax error', function (done) {
      var handler = new RequestHandler(null, options);
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.syntaxError;
      handler.retry = function () {
        assert.fail();
      };
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        done();
      });
    });

    it('should retrow on unauthorized error', function (done) {
      var handler = new RequestHandler(null, options);
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.unauthorized;
      handler.retry = function () {
        assert.fail();
      };
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        done();
      });
    });

    it('should retry on overloaded error', function (done) {
      var handler = new RequestHandler(null, options);
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.overloaded;

      var retryCalled = false;
      handler.retry = function (cb) {
        retryCalled = true;
        cb();
      };

      handler.handleError(responseError, function (err) {
        assert.equal(err, null);
        assert.strictEqual(retryCalled, true);
        done();
      });
    });

    it('should rely on the RetryPolicy onWriteTimeout', function (done) {
      var policy = new retry.RetryPolicy();
      var policyCalled = false;
      policy.onWriteTimeout = function (info) {
        assert.notEqual(info, null);
        policyCalled = true;
        return {decision: retry.RetryPolicy.retryDecision.retry}
      };
      var handler = new RequestHandler(null, utils.extend({}, options, { policies: { retry: policy }}));
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.writeTimeout;
      var retryCalled = false;
      handler.retry = function (cb) {
        retryCalled = true;
        cb();
      };

      handler.handleError(responseError, function (err) {
        assert.equal(err, null);
        assert.strictEqual(retryCalled, true);
        assert.strictEqual(policyCalled, true);
        done();
      });
    });

    it('should rely on the RetryPolicy onUnavailable', function (done) {
      var policy = new retry.RetryPolicy();
      var policyCalled = false;
      policy.onUnavailable = function (info) {
        assert.notEqual(info, null);
        policyCalled = true;
        return {decision: retry.RetryPolicy.retryDecision.retrow};
      };
      var handler = new RequestHandler(null, utils.extend({}, options, { policies: { retry: policy }}));
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.unavailableException;
      handler.retry = function () {
        assert.fail();
      };
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        assert.strictEqual(policyCalled, true);
        done();
      });
    });
  });
  describe('#prepareMultiple()', function () {
    it('should prepare each query serially and callback with the response', function (done) {
      var handler = new RequestHandler(null, options);
      var prepareCounter = 0;
      var eachCounter = 0;
      var connection = {
        prepareOnce: function (q, cb) {
          prepareCounter++;
          setImmediate(function () {
            cb(null, {});
          });
        }
      };
      handler.getNextConnection = function (o, cb) {
        cb(null, connection);
      };
      var eachCallback = function () { eachCounter++; };
      handler.prepareMultiple(['q1', 'q2'], [eachCallback, eachCallback], {}, function (err) {
        assert.ifError(err);
        assert.strictEqual(2, eachCounter);
        assert.strictEqual(2, prepareCounter);
        done();
      });
    });
    it('should retry with a handler when there is an error', function (done) {
      var handler = new RequestHandler(null, options);
      var retryCounter = 0;
      var connection = {
        prepareOnce: function (q, cb) {
          var err;
          if (retryCounter === 0) {
            err = new errors.ResponseError(types.responseErrorCodes.overloaded, 'dummy error');
          }
          setImmediate(function () {
            cb(err, {});
          });
        }
      };
      handler.getNextConnection = function (o, cb) {
        cb(null, connection);
      };
      handler.retry = function (cb) {
        retryCounter++;
        setImmediate(cb);
      };
      handler.prepareMultiple(['q1', 'q2'], [helper.noop, helper.noop], {}, function (err) {
        assert.ifError(err);
        assert.ok(handler.retryHandler);
        assert.strictEqual(1, retryCounter);
        done();
      });
    });
    it('should not retry when there is an query error', function (done) {
      var handler = new RequestHandler(null, options);
      var connection = {
        prepareOnce: function (q, cb) {
          setImmediate(function () {
            cb(new errors.ResponseError(types.responseErrorCodes.syntaxError, 'syntax error'));
          });
        }
      };
      handler.getNextConnection = function (o, cb) {
        cb(null, connection);
      };
      handler.prepareMultiple(['q1', 'q2'], [helper.noop, helper.noop], {}, function (err) {
        helper.assertInstanceOf(err, errors.ResponseError);
        assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
        done();
      });
    });
  });
});