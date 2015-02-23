var assert = require('assert');
var async = require('async');
var util = require('util');

var Connection = require('../../lib/connection.js');
var requests = require('../../lib/requests');
var defaultOptions = require('../../lib/client-options.js').defaultOptions();
var types = require('../../lib/types');
var utils = require('../../lib/utils.js');
var helper = require('../test-helper.js');

describe('Connection', function () {
  describe('#sendStream()', function () {
    it('should set the timeout for the idle request', function (done) {
      var options = utils.extend({logEmitter: helper.noop}, defaultOptions);
      options.pooling.heartBeatInterval = 60000;
      var connection = new Connection('address1', 2, options);
      connection.writeQueue = {
        push: function (r, cb) {
          setImmediate(cb);
          setTimeout(function () {
            Object
              .keys(connection.streamHandlers)
              .map(function (k) {
                var h = connection.streamHandlers[k];
                delete connection.streamHandlers[k];
                return h;
              })
              .forEach(function (h) {
                setImmediate(h.callback);
              });
          }, 50);
        }
      };
      assert.ok(!connection.idleTimeout);
      connection.sendStream({dummy: 'request'}, {}, function () {
        assert.ok(connection.idleTimeout);
        connection.close();
        done();
      });
    });
    it('should not set the timeout for the idle request when heartBeatInterval is 0', function (done) {
      var options = utils.extend({logEmitter: helper.noop}, defaultOptions);
      options.pooling.heartBeatInterval = 0;
      var connection = new Connection('address1', 2, options);
      connection.writeQueue = {
        push: function (r, cb) {
          setImmediate(cb);
          setTimeout(function () {
            Object
              .keys(connection.streamHandlers)
              .map(function (k) {
                var h = connection.streamHandlers[k];
                delete connection.streamHandlers[k];
                return h;
              })
              .forEach(function (h) {
                setImmediate(h.callback);
              });
          }, 50);
        }
      };
      assert.ok(!connection.idleTimeout);
      connection.sendStream({dummy: 'request'}, {}, function () {
        assert.ok(!connection.idleTimeout);
        connection.close();
        done();
      });
    });
  });
  describe('#idleTimeoutHandler()', function () {
    it('should emit idleRequestError if there was an error while executing the request', function (done) {
      var options = utils.extend({logEmitter: helper.noop}, defaultOptions);
      var connection = new Connection('address1', 2, options);
      connection.sendStream = function (req, options, cb) {
        helper.assertInstanceOf(req, requests.QueryRequest);
        setImmediate(function () { cb (new Error('Dummy'))});
      };
      connection.on('idleRequestError', function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
      connection.idleTimeoutHandler();
    });
  });
});

function newInstance(address){
  if (!address) {
    address = helper.baseOptions.contactPoints[0];
  }
  var logEmitter = function () {};
  var options = utils.extend({logEmitter: logEmitter}, defaultOptions);
  return new Connection(address, 1, options);
}