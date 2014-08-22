var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var RequestHandler = rewire('../../lib/request-handler.js');
var errors = require('../../lib/errors.js');
var types = require('../../lib/types.js');

var options = (function () {
  var loadBalancing = require('../../lib/policies/load-balancing.js');
  var reconnection = require('../../lib/policies/reconnection.js');;
  var retry = require('../../lib/policies/retry.js');
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
      handler.retry = function () {
        done();
      };
      handler.handleError(responseError, function (err) {
        assert.notEqual(err, null);
      });
    });
  });
});