var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var RequestHandler = require('../../lib/request-handler');
var requests = require('../../lib/requests');
var helper = require('../test-helper');
var errors = require('../../lib/errors');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var retry = require('../../lib/policies/retry');

var options = (function () {
  var loadBalancing = require('../../lib/policies/load-balancing.js');
  var reconnection = require('../../lib/policies/reconnection.js');
  return {
    policies: {
      loadBalancing: new loadBalancing.RoundRobinPolicy(),
      reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
      retry: new retry.RetryPolicy()
    },
    socketOptions: {
      readTimeout: 0
    },
    logEmitter: helper.noop
  };
})();
describe('RequestHandler', function () {
  describe('#handleError()', function () {
    it('should retrow on syntax error', function (done) {
      var handler = new RequestHandler(null, options);
      handler.host = { address: '1'};
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
      handler.host = { address: '1'};
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
      handler.host = { address: '1'};
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
      handler.host = { address: '1'};
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
      handler.host = { address: '1'};
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
      handler.host = { address: '1'};
      var retryCounter = 0;
      var connection = {
        prepareOnce: function (q, cb) {
          var err;
          if (retryCounter === 0) {
            err = new errors.ResponseError(types.responseErrorCodes.overloaded, 'dummy error');
          }
          setImmediate(function () {
            cb(err, { flags: utils.emptyObject});
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
      handler.host = { address: '1'};
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
  describe('#prepareAndRetry()', function () {
    it('should only re-prepare of ExecuteRequest and BatchRequest', function (done) {
      var handler = new RequestHandler(null, options);
      handler.request = {};
      handler.host = {};
      handler.prepareAndRetry(new Buffer(0), function (err) {
        helper.assertInstanceOf(err, errors.DriverInternalError);
        done();
      });
    });
    it('should prepare all BatchRequest queries and send request again on the same connection', function (done) {
      var handler = new RequestHandler(null, options);
      handler.request = {};
      handler.host = {};
      handler.request = new requests.BatchRequest([
        { info: { queryId: new Buffer('10')}, query: '1'},
        { info: { queryId: new Buffer('20')}, query: '2'}
      ], {});
      var connection = { prepareOnce: function (q, cb) {
        queriesPrepared.push(q);
        setImmediate(function () { cb(null, { id: new Buffer(q)})});
      }};
      handler.connection = connection;
      var queriesPrepared = [];
      handler.sendOnConnection = function (request, o, cb) {
        helper.assertInstanceOf(request, requests.BatchRequest);
        assert.strictEqual(handler.connection, connection);
        setImmediate(cb);
      };
      handler.prepareAndRetry(new Buffer(0), function (err) {
        assert.ifError(err);
        assert.strictEqual(queriesPrepared.toString(), handler.request.queries.map(function (x) {return x.query; }).toString());
        done();
      });
    });
    it('should prepare distinct BatchRequest queries', function (done) {
      var handler = new RequestHandler(null, options);
      handler.request = {};
      handler.host = {};
      handler.request = new requests.BatchRequest([
        { info: { queryId: new Buffer('zz')}, query: 'SAME QUERY'},
        { info: { queryId: new Buffer('zz')}, query: 'SAME QUERY'}
      ], {});
      var connection = { prepareOnce: function (q, cb) {
        queriesPrepared.push(q);
        setImmediate(function () { cb(null, { id: new Buffer(q)})});
      }};
      handler.connection = connection;
      var queriesPrepared = [];
      handler.sendOnConnection = function (request, o, cb) {
        helper.assertInstanceOf(request, requests.BatchRequest);
        assert.strictEqual(handler.connection, connection);
        setImmediate(cb);
      };
      handler.prepareAndRetry(new Buffer(0), function (err) {
        assert.ifError(err);
        //Only 1 query
        assert.strictEqual(queriesPrepared.length, 1);
        assert.strictEqual(queriesPrepared.join(','), 'SAME QUERY');
        done();
      });
    });
  });
  describe('#send()', function () {
    it('should return a ResultSet with valid columns', function (done) {
      var handler = new RequestHandler(null, options);
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, {
            meta: {
              columns: [
                { type: { code: types.dataTypes.text, info: null}, name: 'col1'},
                { type: { code: types.dataTypes.list, info: { code: types.dataTypes.uuid, info: null}}, name: 'col2'}
              ],
              pageState: new Buffer('1234aa', 'hex')},
            flags: utils.emptyObject
          });
        });
      }};
      handler.getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      handler.send(new requests.QueryRequest('Dummy QUERY'), {}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.ok(util.isArray(result.columns));
        assert.strictEqual(result.columns.length, 2);
        assert.strictEqual(util.inspect(result.columns[0].type), util.inspect({code: types.dataTypes.text, info: null}));
        assert.strictEqual(result.columns[0].type.code, types.dataTypes.text);
        assert.strictEqual(result.columns[0].name, 'col1');
        assert.strictEqual(result.columns[1].type.code, types.dataTypes.list);
        assert.strictEqual(result.columns[1].type.info.code, types.dataTypes.uuid);
        assert.strictEqual(result.columns[1].name, 'col2');
        assert.ok(result.info);
        assert.strictEqual(result.pageState, '1234aa');
        done();
      });
    });
    it('should return a ResultSet with null columns when there is no metadata', function (done) {
      var handler = new RequestHandler(null, options);
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, { flags: utils.emptyObject });
        });
      }};
      handler.getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      handler.send(new requests.QueryRequest('Dummy QUERY'), {}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.strictEqual(result.columns, null);
        done();
      });
    });
    it('should use the retry policy defined in the QueryOptions', function (done) {
      var handler = new RequestHandler(null, options);
      var connectionCalled = 0;
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          if (connectionCalled++ < 2) {
            return cb(new errors.ResponseError(types.responseErrorCodes.readTimeout, 'dummy timeout'));
          }
          cb(null, { meta: {
            columns: [
              { type: { code: types.dataTypes.text, info: null}, name: 'col1'},
              { type: { code: types.dataTypes.list, info: { code: types.dataTypes.uuid, info: null}}, name: 'col2'}
            ],
            pageState: new Buffer('1234aa', 'hex')
          }});
        });
      }};
      handler.getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      var policy = new retry.RetryPolicy();
      var policyCalled = 0;
      policy.onReadTimeout = function () {
        policyCalled++;
        return {decision: retry.RetryPolicy.retryDecision.retry};
      };
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('Dummy QUERY'), { retry: policy}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        //2 error responses, 2 retry decisions
        assert.strictEqual(policyCalled, 2);
        done();
      });
    });
    it('should callback with OperationTimedOutError when queryOptions.retryOnTimeout is set to false', function (done) {
      var handler = newInstance( { socketOptions: { readTimeout: 1234 }});
      handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop };
      var connection = { sendStream: function (r, o, cb) {
        cb(new errors.OperationTimedOutError('Testing timeout'))
      }};
      handler.getNextConnection = function (o, cb) {
        cb(null, connection);
      };
      var queryOptions = { retryOnTimeout: false};
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('q'), queryOptions, function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        assert.strictEqual(err.message, 'Testing timeout');
        done();
      });
    });
    it('should retry sending using the next host', function (done) {
      var handler = newInstance();
      var getNextConnectionCounter = 0;
      handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop, setUp: helper.noop };
      var connection1 = { sendStream: function (r, o, cb) {
        cb(new errors.OperationTimedOutError('Testing timeout'))
      }};
      var connection2 = { sendStream: function (r, o, cb) {
        cb(null, {});
      }};
      handler.getNextConnection = function (o, cb) {
        if (getNextConnectionCounter++ === 0) {
          return cb(null, connection1);
        }
        cb(null, connection2);
      };
      var queryOptions = { retryOnTimeout: true };
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('q'), queryOptions, function (err) {
        assert.ifError(err);
        assert.strictEqual(getNextConnectionCounter, 2);
        done();
      });
    });
    it('should retry sending using the next host when is a PREPARE request', function (done) {
      var handler = newInstance();
      var getNextConnectionCounter = 0;
      handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop, setUp: helper.noop };
      var connection1 = { sendStream: function (r, o, cb) {
        cb(new errors.OperationTimedOutError('Testing timeout'))
      }};
      var connection2 = { sendStream: function (r, o, cb) {
        cb(null, {});
      }};
      handler.getNextConnection = function (o, cb) {
        if (getNextConnectionCounter++ === 0) {
          return cb(null, connection1);
        }
        cb(null, connection2);
      };
      //even though it is set to false, it should be retried
      var queryOptions = { retryOnTimeout: false };
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.PrepareRequest('q'), queryOptions, function (err) {
        assert.ifError(err);
        assert.strictEqual(getNextConnectionCounter, 2);
        done();
      });
    });
  });
  describe('#onTimeout', function () {
    it('should check host health', function (done) {
      var checkHealth = 0;
      var handler = newInstance();
      handler.host = { address: '1.1.1.1:9042', checkHealth: function () {
        checkHealth++;
      }};
      handler.connection = { onTimeout: helper.noop };
      //noinspection JSCheckFunctionSignatures
      handler.onTimeout(new errors.OperationTimedOutError('Testing'), function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        assert.strictEqual(checkHealth, 1);
        done();
      });
    });
  });
});

/** @returns {RequestHandler} */
function newInstance(customOptions) {
  return new RequestHandler(null, utils.extend({}, options, customOptions));
}
