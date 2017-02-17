"use strict";
var assert = require('assert');
var util = require('util');
var events = require('events');
var rewire = require('rewire');

var Connection = require('../../lib/connection');
var requests = require('../../lib/requests');
var defaultOptions = require('../../lib/client-options.js').defaultOptions();
var utils = require('../../lib/utils.js');
var helper = require('../test-helper.js');

describe('Connection', function () {
  describe('constructor', function () {
    it('should parse host endpoint into address and port', function () {
      var values = [
        ['127.0.0.1:9042', '127.0.0.1', '9042'],
        ['10.1.1.255:8888', '10.1.1.255', '8888'],
        ['::1:8888', '::1', '8888'],
        ['::1:1234', '::1', '1234'],
        ['aabb::eeff:11:2233:4455:6677:8899:9999', 'aabb::eeff:11:2233:4455:6677:8899', '9999']
      ];
      values.forEach(function (item) {
        var c = new Connection(item[0], 4, defaultOptions);
        assert.strictEqual(c.address, item[1]);
        assert.strictEqual(c.port, item[2]);
      });
    });
  });
  describe('#prepareOnce()', function () {
    function prepareAndAssert(connection, query) {
      return (function (cb) {
        connection.prepareOnce(query, function (err, r) {
          assert.ifError(err);
          assert.strictEqual(query, r);
          cb();
        });
      });
    }
    it('should prepare different queries', function (done) {
      var connection = newInstance();
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          cb(null, r.query);
        });
      };
      utils.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY2'),
        prepareAndAssert(connection, 'QUERY3')
      ], function (err) {
        assert.ifError(err);
        done();
      });
    });
    it('should prepare different queries with keyspace', function (done) {
      var connection = newInstance();
      connection.keyspace = 'ks1';
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          cb(null, r.query);
        });
      };
      utils.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY2'),
        prepareAndAssert(connection, 'QUERY3')
      ], function (err) {
        assert.ifError(err);
        done();
      });
    });
    it('should prepare the same query once', function (done) {
      var connection = newInstance();
      var ioCount = 0;
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          ioCount++;
          cb(null, r.query);
        });
      };
      utils.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1')
      ], function (err) {
        assert.ifError(err);
        assert.strictEqual(ioCount, 1);
        done();
      });
    });
    it('should prepare the same query once with keyspace', function (done) {
      var connection = newInstance();
      connection.keyspace = 'ks1';
      var ioCount = 0;
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          ioCount++;
          cb(null, r.query);
        });
      };
      utils.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1')
      ], function (err) {
        assert.ifError(err);
        assert.strictEqual(ioCount, 1);
        done();
      });
    });
  });
  describe('#sendStream()', function () {
    it('should set the timeout for the idle request', function (done) {
      var options = utils.extend({logEmitter: helper.noop}, defaultOptions);
      options.pooling.heartBeatInterval = 60000;
      var connection = newInstance('address1', 2, options);
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
      var connection = newInstance('address1', 2, options);
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
      var connection = newInstance('address1', 2, options);
      connection.sendStream = function (req, options, cb) {
        helper.assertInstanceOf(req, requests.QueryRequest);
        setImmediate(function () { cb (new Error('Dummy'));});
      };
      connection.on('idleRequestError', function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
      connection.idleTimeoutHandler();
    });
  });
  describe('#close', function () {
    it('should allow socket.close event to be emitted before calling back when connected', function (done) {
      var ConnectionInjected = rewire('../../lib/connection');
      function SocketMock() {
      }
      util.inherits(SocketMock, events.EventEmitter);
      SocketMock.prototype.connect = function (p, a, cb) {
        setImmediate(cb);
      };
      SocketMock.prototype.destroy = function () {
        var self = this;
        setImmediate(function () {
          self.emit('close');
        });
      };
      SocketMock.prototype.end = SocketMock.prototype.destroy;
      SocketMock.prototype.setTimeout = helper.noop;
      SocketMock.prototype.setKeepAlive = helper.noop;
      SocketMock.prototype.setNoDelay = helper.noop;
      SocketMock.prototype.pipe = function () {return this;};
      ConnectionInjected.__set__("net", { Socket: SocketMock});
      var c = new ConnectionInjected('127.0.0.1:9042', 9042, utils.extend({}, defaultOptions));
      c.logEmitter = helper.noop;
      c.sendStream = function (r, o, cb) {
        cb(null, {});
      };
      c.open(function (err) {
        assert.ifError(err);
        assert.ok(c.connected);
        //it is now connected
        //noinspection JSUnresolvedVariable
        var socket = c.netClient;
        var closeEmitted = 0;
        socket.on('close', function () {
          closeEmitted++;
        });
        c.close(function (err) {
          assert.ifError(err);
          assert.strictEqual(closeEmitted, 1);
          done();
        });
      });
    });
    it('should allow socket.close event to be emitted before calling back when disconnected', function (done) {
      var ConnectionInjected = rewire('../../lib/connection');
      function SocketMock() {
      }
      util.inherits(SocketMock, events.EventEmitter);
      SocketMock.prototype.connect = function (p, a, cb) {
        setImmediate(cb);
      };
      SocketMock.prototype.end = function () {
        var self = this;
        setImmediate(function () {
          self.emit('close');
        });
      };
      SocketMock.prototype.setTimeout = helper.noop;
      SocketMock.prototype.setKeepAlive = helper.noop;
      SocketMock.prototype.setNoDelay = helper.noop;
      SocketMock.prototype.pipe = function () {return this;};
      ConnectionInjected.__set__("net", { Socket: SocketMock});
      var c = new ConnectionInjected('127.0.0.1:9042', 9042, utils.extend({}, defaultOptions));
      c.logEmitter = helper.noop;
      c.sendStream = function (r, o, cb) {
        cb(null, {});
      };
      c.open(function (err) {
        assert.ifError(err);
        assert.ok(c.connected);
        //force destroy
        c.connected = false;
        //noinspection JSUnresolvedVariable
        var socket = c.netClient;
        var closeEmitted = 0;
        socket.on('close', function () {
          closeEmitted++;
        });
        c.close(function (err) {
          assert.ifError(err);
          assert.strictEqual(closeEmitted, 1);
          done();
        });
      });
    });
  });
});

function newInstance(address, protocolVersion, options){
  address = address || helper.baseOptions.contactPoints[0];
  options = utils.extend({logEmitter: helper.noop}, defaultOptions, options);
  return new Connection(address + ':' + 9000, protocolVersion || 1, options);
}