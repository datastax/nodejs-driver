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
    logEmitter: helper.noop
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
  describe('#retry()', function () {
    it('should cause a getNextConnection', function () {
      var handler = new RequestHandler(null, options);
      handler.connection = {};
      handler.sendOnConnection = helper.noop;
      handler.getNextConnection = function (o, cb) {
        called = true;
        cb(null, {});
      };
      handler.retry(helper.noop);
      assert.ok(called);
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
      var queriesPrepared = [];
      var connection = {
        sendStream: function (r, o, cb) {
          queriesPrepared.push(r.query);
          setImmediate(function () { cb(null, { id: new Buffer(r.query)})});
        }
      };
      handler.connection = connection;
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
      var connection = {
        sendStream: function (r, o, cb) {
          queriesPrepared.push(r.query);
          setImmediate(function () { cb(null, { id: new Buffer(r.query)})});
        }
      };
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
          cb(null, { meta: {
            columns: [{ type: [types.dataTypes.text], name: 'col1'}],
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
      handler.send(new requests.QueryRequest('Dummy QUERY'), {}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.ok(util.isArray(result.columns));
        assert.strictEqual(result.columns.length, 1);
        assert.strictEqual(result.columns[0].type, types.dataTypes.text);
        assert.strictEqual(result.columns[0].name, 'col1');
        assert.ok(result.info);
        assert.strictEqual(result.pageState, '1234aa');
        done();
      });
    });
    it('should return a ResultSet with null columns when there is no metadata', function (done) {
      var handler = new RequestHandler(null, options);
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, {});
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
    it('should queue multiple requests', function (done) {
      var handler = new RequestHandler(null, options);
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, {query: r.query});
        });
      }};
      handler.getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      var history = '';
      var _send = handler._send;
      handler._send = function (r, o, c) {
        history += r.query;
        return _send.call(this, r, o, c);
      };
      handler.send(new requests.QueryRequest('q1'), {}, handleResponse.bind({query: 'q1'}));
      handler.send(new requests.QueryRequest('q2'), {}, handleResponse.bind({query: 'q2'}));
      var count = 0;
      function handleResponse (err, result) {
        if (err) return done(err);
        history += this.query;
        if (++count === 2) {
          assert.strictEqual(history, 'q1q1q2q2');
          done();
        }
      }
    });
  });
});
