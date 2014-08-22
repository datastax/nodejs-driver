var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var RequestHandler = rewire('../../lib/request-handler.js');
var errors = require('../../lib/errors.js');
var types = require('../../lib/types.js');
var utils = require('../../lib/utils.js');
var retry = require('../../lib/policies/retry.js');

var options = (function () {
  var loadBalancing = require('../../lib/policies/load-balancing.js');
  var reconnection = require('../../lib/policies/reconnection.js');;
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
      var handler = new RequestHandler(options);
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
      var handler = new RequestHandler(options);
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
      var handler = new RequestHandler(options);
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
      var handler = new RequestHandler(utils.extend({}, options, { policies: { retry: policy }}));
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
      var handler = new RequestHandler(utils.extend({}, options, { policies: { retry: policy }}));
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
});