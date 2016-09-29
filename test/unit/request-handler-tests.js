var assert = require('assert');
var util = require('util');
var rewire = require('rewire');

var RequestHandler = require('../../lib/request-handler');
var requests = require('../../lib/requests');
var helper = require('../test-helper');
var errors = require('../../lib/errors');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var retry = require('../../lib/policies/retry');
var ProfileManager = require('../../lib/execution-profile').ProfileManager;

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
  describe('#getDecision()', function () {
    it('should retry when there was a socket error and mutation was not applied', function () {
      var handler = newInstance();
      var result = handler.getDecision({ isSocketError: true, requestNotWritten: true });
      assert.strictEqual(result.decision, retry.RetryPolicy.retryDecision.retry);
    });
    it('should use the retry policy when there was a socket error and mutation was applied', function () {
      var handler = newInstance();
      handler.request = {};
      var requestErrorCalled = 0;
      handler.retryPolicy = { onRequestError: function () {
        requestErrorCalled++;
      }};
      handler.getDecision({ isSocketError: true });
      assert.strictEqual(requestErrorCalled, 1);
    });
  });
  describe('#handleError()', function () {
    it('should retrow on syntax error', function (done) {
      var handler = newInstance();
      handler.host = { address: '1'};
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.syntaxError;
      handler.retry = function () {
        assert.fail();
      };
      handler.requestOptions = {};
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        done();
      });
    });
    it('should retrow on unauthorized error', function (done) {
      var handler = newInstance();
      handler.host = { address: '1'};
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.unauthorized;
      handler.retry = function () {
        assert.fail();
      };
      handler.requestOptions = {};
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        done();
      });
    });
    it('should retry on overloaded error', function (done) {
      var handler = newInstance();
      handler.host = { address: '1'};
      handler.request = {};
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.overloaded;
      var retryCalled = false;
      handler.retry = function (c, useCurrentHost, cb) {
        retryCalled = true;
        assert.strictEqual(useCurrentHost, false);
        cb();
      };
      handler.requestOptions = {};
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
        return { decision: retry.RetryPolicy.retryDecision.retry };
      };
      var handler = newInstance({ policies: { retry: policy }});
      handler.host = { address: '1'};
      var responseError = new errors.ResponseError(0, 'Test error');
      responseError.code = types.responseErrorCodes.writeTimeout;
      var retryCalled = false;
      handler.retry = function (c, useCurrentHost, cb) {
        retryCalled = true;
        assert.strictEqual(useCurrentHost, undefined);
        cb();
      };
      handler.requestOptions = {};
      assert.strictEqual(handler.client.profileManager.getDefault().retry, policy);
      assert.strictEqual(handler.retryPolicy, policy);
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
        return { decision: retry.RetryPolicy.retryDecision.rethrow };
      };
      var handler = newInstance({ policies: { retry: policy }});
      handler.host = { address: '1'};
      var responseError = new errors.ResponseError(types.responseErrorCodes.unavailableException, 'Test error');
      handler.retry = function () {
        assert.fail();
      };
      handler.requestOptions = {};
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err, responseError);
        assert.strictEqual(policyCalled, true);
        done();
      });
    });
    it('should include the coordinator in the error object', function (done) {
      var handler = newInstance();
      handler.host = { address: '1'};
      var responseError = new errors.ResponseError();
      responseError.code = types.responseErrorCodes.readTimeout;
      handler.retry = function () {
        assert.fail();
      };
      handler.requestOptions = {};
      handler.handleError(responseError, function (err) {
        assert.strictEqual(err.coordinator, handler.host.address);
        done();
      });
    });
    it('should return an empty ResultSet when retry decision is ignore', function (done) {
      var handler = newInstance();
      handler.host = { address: '1'};
      handler.request = { };
      handler.getDecision = function () {
        return { decision: retry.RetryPolicy.retryDecision.ignore };
      };
      handler.requestOptions = {};
      handler.handleError({}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.ok(!result.rowLength);
        done();
      });
    });
  });
  describe('#prepareMultiple()', function () {
    it('should prepare each query serially and callback with the response', function (done) {
      var handler = newInstance();
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
      var handler = newInstance();
      handler.request = {};
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
      handler.retry = function (c, uh, cb) {
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
      var handler = newInstance();
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
      var handler = newInstance();
      handler.request = {};
      handler.host = {};
      handler.prepareAndRetry(new Buffer(0), function (err) {
        helper.assertInstanceOf(err, errors.DriverInternalError);
        done();
      });
    });
    it('should prepare all BatchRequest queries and send request again on the same connection', function (done) {
      var handler = newInstance();
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
      var handler = newInstance();
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
  describe('#retry()', function () {
    it('should set consistency level when provided', function (done) {
      var handler = newInstance();
      handler.request = { consistency: types.consistencies.localQuorum };
      var request;
      handler.sendOnConnection = function (r, options, cb) {
        request = r;
        cb();
      };
      var retryConsistency = types.consistencies.three;
      handler.retry(retryConsistency, true, function (err) {
        assert.ifError(err);
        assert.ok(request);
        assert.strictEqual(request.consistency, retryConsistency);
        done();
      });
    });
    it('should use the next host when specified', function (done) {
      var handler = newInstance();
      handler.request = { consistency: types.consistencies.localQuorum };
      var sendCalled = 0;
      handler.send = function (r, options, cb) {
        sendCalled++;
        cb();
      };
      handler.retry(null, false, function (err) {
        assert.ifError(err);
        //RequestHandler#send() uses next host reusing hosts iterator from the previous query plan
        assert.strictEqual(sendCalled, 1);
        done();
      });
    });
  });
  describe('#send()', function () {
    it('should return a ResultSet with valid columns', function (done) {
      var handler = newInstance();
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
      //noinspection JSCheckFunctionSignatures
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
      var handler = newInstance();
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
    it('should use the retry policy defined in the constructor', function (done) {
      var policy = new retry.RetryPolicy();
      var policyCalled = 0;
      policy.onReadTimeout = function () {
        policyCalled++;
        return {decision: retry.RetryPolicy.retryDecision.retry};
      };
      var handler = newInstance(null, null, null, policy);
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
      handler.send(new requests.QueryRequest('Dummy QUERY'), { }, function (err, result) {
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

    var captureStackTraceOptions = [
      {options:{}, expected:false},
      {options:{captureStackTrace:true}, expected:true},
      {options:{captureStackTrace:false}, expected:false}
    ];

    captureStackTraceOptions.forEach(function(test) {
      var expect = test.expected ? '' : ' not';
      it('should return an error' + expect + ' including calling stack trace if captureStackTrace is ' + test.options.captureStackTrace, function (done) {
        var handler = newInstance();
        // Capture the current stack minus the top line in the call stack (since line number is not exact).
        var stack = {};
        Error.captureStackTrace(stack);
        stack = stack.stack.split('\n');
        stack.splice(0,2);
        stack = stack.join('\n');

        handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop, setUp: helper.noop };
        var connection1 = { sendStream: function (r, o, cb) {
          setImmediate(function () {
            cb(new errors.ResponseError(types.responseErrorCodes.syntaxError, 'syntax error'));
          });
        }};
        handler.getNextConnection = function (o, cb) {
          return cb(null, connection1);
        };
        //noinspection JSCheckFunctionSignatures
        handler.send(new requests.QueryRequest('q'), test.options, function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          if(test.expected) {
            assert.ok(err.stack.indexOf('(event loop)') != -1, err.stack + '\n\tdoes not contain (event loop)');
            assert.ok(err.stack.indexOf(stack) != -1, err.stack + '\n\tdoes not contain\n' + stack);
          } else {
            assert.ok(err.stack.indexOf('(event loop)') == -1, err.stack + '\n\tcontains (event loop)');
            assert.ok(err.stack.indexOf(stack) == -1, err.stack + '\n\tcontains\n' + stack);
          }
          done();
        });
      });
    });
  });
  describe('#onTimeout()', function () {
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
  describe('#iterateThroughHosts()', function () {
    var getHost = function (address, isUp) {
      return {
        isUp: function () { return isUp !== false; },
        setDistance: helper.noop,
        address: address,
        setDown: function () {
          //noinspection JSPotentiallyInvalidUsageOfThis
          this.isDown = true;
        }
      }
    };
    it('should synchronously get next connection when pool warmed', function (done) {
      var handler = newInstance();
      var hosts = utils.arrayIterator([ getHost() ]);
      handler.getPooledConnection = function (h, cb) {
        cb(null, {});
      };
      var sync = true;
      handler.iterateThroughHosts(hosts, function (err, c) {
        assert.ifError(err);
        assert.ok(c);
        assert.ok(sync);
        done();
      });
      sync = false;
    });
    it('should callback with NoHostAvailableError when all host down', function (done) {
      var handler = newInstance();
      var hosts = utils.arrayIterator([ getHost('2001::1', false), getHost('2001::2', false) ]);
      var sync = true;
      handler.iterateThroughHosts(hosts, function (err, c) {
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        assert.ok(err.innerErrors);
        assert.deepEqual(Object.keys(err.innerErrors), ['2001::1', '2001::2']);
        assert.strictEqual(typeof err.innerErrors['2001::1'], 'string');
        assert.strictEqual(typeof err.innerErrors['2001::2'], 'string');
        assert.ok(!c);
        assert.ok(sync);
        done();
      });
      sync = false;
    });
  });
});

/** @returns {RequestHandler} */
function newInstance(customOptions, client, loadBalancingPolicy, retryPolicy) {
  var o = utils.extend({}, options, customOptions);
  return new RequestHandler(
    client || newClient(o), loadBalancingPolicy || o.policies.loadBalancing, retryPolicy || o.policies.retry);
}

function newClient(o) {
  //noinspection JSCheckFunctionSignatures
  return {
    profileManager: new ProfileManager(o),
    options: o
  };
}