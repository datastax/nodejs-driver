/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const EventEmitter = require('events');
const proxyquire = require('proxyquire');
const sinon = require('sinon');

const Connection = require('../../lib/connection');
const requests = require('../../lib/requests');
const defaultOptions = require('../../lib/client-options').defaultOptions();
const utils = require('../../lib/utils');
const errors = require('../../lib/errors');
const ExecutionOptions = require('../../lib/execution-options').ExecutionOptions;
const helper = require('../test-helper');

describe('Connection', function () {
  describe('constructor', function () {
    it('should parse host endpoint into address and port', function () {
      const values = [
        ['127.0.0.1:9042', '127.0.0.1', '9042'],
        ['10.1.1.255:8888', '10.1.1.255', '8888'],
        ['::1:8888', '::1', '8888'],
        ['::1:1234', '::1', '1234'],
        ['aabb::eeff:11:2233:4455:6677:8899:9999', 'aabb::eeff:11:2233:4455:6677:8899', '9999']
      ];
      values.forEach(function (item) {
        const c = new Connection(item[0], 4, defaultOptions);
        assert.strictEqual(c.address, item[1]);
        assert.strictEqual(c.port, item[2]);
      });
    });
  });
  describe('#prepareOnce()', function () {
    function prepareAndAssert(connection, query) {
      return (function (cb) {
        connection.prepareOnce(query, 'ks', function (err, r) {
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
    let clock;

    before(() => clock = sinon.useFakeTimers());
    after(() => clock.restore());

    it('should set the timeout for the idle request', function () {
      const sent = [];
      const writeQueueFake = getWriteQueueFake(sent);
      const c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 20 } }, writeQueueFake);
      c.sendStream(new requests.QueryRequest('QUERY1'), null, utils.noop);

      clock.tick(20);

      // 2 requests were sent, the user query plus the idle 'options' query
      assert.deepEqual(sent.map(function (op) {
        if (op.request instanceof requests.QueryRequest) {
          return op.request.query;
        }
        return op.request;
      }), [ 'QUERY1', requests.options ]);
      c.close();
    });
    it('should not set the timeout for the idle request when heartBeatInterval is 0', function () {
      const sent = [];
      const writeQueueFake = getWriteQueueFake(sent);
      const c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 0 } }, writeQueueFake);
      c.sendStream(new requests.QueryRequest('QUERY1'), null, utils.noop);

      clock.tick(20);

      // Only 1 request was sent, no idle query
      assert.deepEqual(sent.map(function (op) {
        if (op.request instanceof requests.QueryRequest) {
          return op.request.query;
        }
        return op.request;
      }), [ 'QUERY1' ]);
      c.close();
    });
    it('should reset the timeout after each new request', function () {
      const sent = [];
      const writeQueueFake = getWriteQueueFake(sent);
      const c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 20 } }, writeQueueFake);
      for (let i = 0; i < 4; i++) {
        clock.tick(10);
        c.sendStream(new requests.QueryRequest('QUERY' + i), null, utils.noop);
      }

      // Only 4 request were sent, no idle query
      assert.deepEqual(sent.map(function (op) {
        if (op.request instanceof requests.QueryRequest) {
          return op.request.query;
        }
        return op.request;
      }), Array.apply(null, new Array(4)).map((x, i) => 'QUERY' + i));
      c.close();
    });
    it('should set the request timeout', function (done) {
      const writeQueueFake = getWriteQueueFake();
      const c = newInstance(undefined, undefined, { pooling: { heartBeatInterval: 0 } }, writeQueueFake);

      c.sendStream(new requests.QueryRequest('QUERY1'), getExecOptions({ readTimeout: 20 }), function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        c.close();
        done();
      });

      clock.tick(20);
    });
  });
  describe('#close', function () {
    it('should allow socket.close event to be emitted before calling back when connected', function (done) {

      class Socket extends BaseSocketMock {
        destroy() {
          setImmediate(() => this.emit('close'));
        }

        end() {
          this.destroy();
        }
      }

      const ConnectionInjected = proxyquire('../../lib/connection', { 'net': { Socket } });

      const c = new ConnectionInjected('127.0.0.1:9042', 9042, utils.extend({}, defaultOptions));
      c.logEmitter = helper.noop;
      c.sendStream = function (r, o, cb) {
        cb(null, {});
      };
      c.open(function (err) {
        assert.ifError(err);
        assert.ok(c.connected);
        //it is now connected
        const socket = c.netClient;
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
      class Socket extends BaseSocketMock {
        destroy() {
          setImmediate(() => this.emit('close'));
        }

        end() {
          setImmediate(() => this.emit('close'));
        }
      }

      const ConnectionInjected = proxyquire('../../lib/connection', { 'net': { Socket } });
      const c = new ConnectionInjected('127.0.0.1:9042', 9042, utils.extend({}, defaultOptions));
      c.logEmitter = helper.noop;
      c.sendStream = function (r, o, cb) {
        cb(null, {});
      };
      c.open(function (err) {
        assert.ifError(err);
        assert.ok(c.connected);
        //force destroy
        c.connected = false;
        const socket = c.netClient;
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
  const c = new Connection(address + ':' + 9000, protocolVersion || 1, options);
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

function getExecOptions(options) {
  const result = ExecutionOptions.empty();
  result.getReadTimeout = () => options.readTimeout;
  return result;
}

class BaseSocketMock extends EventEmitter {
  connect(p, a, cb) {
    setImmediate(cb);
  }

  destroy() {}
  end() {}
  setTimeout() {}
  setKeepAlive() {}
  setNoDelay() {}
  pipe() {
    return this;
  }
}