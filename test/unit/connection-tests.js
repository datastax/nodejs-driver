"use strict";
const assert = require('assert');
const util = require('util');
const events = require('events');
const rewire = require('rewire');

const Connection = require('../../lib/connection');
const requests = require('../../lib/requests');
const defaultOptions = require('../../lib/client-options').defaultOptions();
const utils = require('../../lib/utils');
const errors = require('../../lib/errors');
const helper = require('../test-helper');

const idleQuery = 'SELECT key from system.local';

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
      const connection = newInstance();
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
      const connection = newInstance();
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
      const connection = newInstance();
      let ioCount = 0;
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
      const connection = newInstance();
      connection.keyspace = 'ks1';
      let ioCount = 0;
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
    this.timeout(1000);
    it('should set the timeout for the idle request', function (done) {
      const sent = [];
      var writeQueueFake = getWriteQueueFake(sent);
      var c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 20 } }, writeQueueFake);
      c.sendStream(new requests.QueryRequest('QUERY1'), null, utils.noop);
      setTimeout(function () {
        // 2 requests were sent, the user query plus the idle query
        assert.deepEqual(sent.map(function (op) {
          return op.request.query;
        }), [ 'QUERY1', idleQuery ]);
        c.close();
        done();
      }, 30);
    });
    it('should not set the timeout for the idle request when heartBeatInterval is 0', function (done) {
      const sent = [];
      var writeQueueFake = getWriteQueueFake(sent);
      var c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 0 } }, writeQueueFake);
      c.sendStream(new requests.QueryRequest('QUERY1'), null, utils.noop);
      setTimeout(function () {
        // Only 1 request was sent, no idle query
        assert.deepEqual(sent.map(function (op) {
          return op.request.query;
        }), [ 'QUERY1' ]);
        c.close();
        done();
      }, 20);
    });
    it('should reset the timeout after each new request', function (done) {
      const sent = [];
      var writeQueueFake = getWriteQueueFake(sent);
      var c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 20 } }, writeQueueFake);
      for (let i = 0; i < 4; i++) {
        setTimeout(function (query) {
          c.sendStream(new requests.QueryRequest(query), null, utils.noop);
        }, 10 * i, 'QUERY' + i);
      }
      setTimeout(function () {
        // Only 4 request were sent, no idle query
        assert.deepEqual(sent.map(function (op) {
          return op.request.query;
        }), Array.apply(null, new Array(4)).map(function (x, i) { return 'QUERY' + i; }));
        c.close();
        done();
      }, 40);
    });
    it('should set the request timeout', function (done) {
      var writeQueueFake = getWriteQueueFake();
      var c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 0 } }, writeQueueFake);
      c.sendStream(new requests.QueryRequest('QUERY1'), { readTimeout: 20 }, function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        c.close();
        done();
      });
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
        const self = this;
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
        var socket = c.netClient;
        let closeEmitted = 0;
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
        const self = this;
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
        var socket = c.netClient;
        let closeEmitted = 0;
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

/** @return {Connection} */
function newInstance(address, protocolVersion, options, writeQueue){
  address = address || helper.baseOptions.contactPoints[0];
  options = utils.deepExtend({ logEmitter: helper.noop }, defaultOptions, options);
  var c = new Connection(address + ':' + 9000, protocolVersion || 1, options);
  c.connected = !!writeQueue;
  c.writeQueue = writeQueue;
  return c;
}

function getWriteQueueFake(sent) {
  sent = sent || [];
  return ({
    push: function (op, writeCallback) {
      sent.push(op);
      setImmediate(writeCallback);
    }
  });
}