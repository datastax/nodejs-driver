/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"use strict";
const assert = require('chai').assert;
const sinon = require('sinon');

const Connection = require('../../../lib/connection.js');
const defaultOptions = require('../../../lib/client-options.js').defaultOptions();
const utils = require('../../../lib/utils.js');
const requests = require('../../../lib/requests.js');
const protocolVersion = require('../../../lib/types').protocolVersion;
const helper = require('../../test-helper.js');
const errors = require('../../../lib/errors');
const types = require('../../../lib/types');
const vit = helper.vit;

describe('Connection', function () {
  this.timeout(240000);
  describe('#open()', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should open', function (done) {
      const localCon = newInstance();
      localCon.open(function (err) {
        assert.ifError(err);
        assert.ok(localCon.connected, 'Must be status connected');
        localCon.close(done);
      });
    });
    it('should use the max supported protocol version', function (done) {
      const localCon = newInstance(null, null);
      localCon.open(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCon.protocolVersion, getProtocolVersion());
        localCon.close(done);
      });
    });
    vit('3.0', 'should callback in error when protocol version is not supported server side', function (done) {
      // Attempting to connect with protocol v2
      const localCon = newInstance(null, 2);
      localCon.open(function (err) {
        helper.assertInstanceOf(err, Error);
        assert.ok(!localCon.connected);
        helper.assertContains(err.message, 'protocol version');
        localCon.close(done);
      });
    });
    vit('2.0', 'should limit the max protocol version based on the protocolOptions', function (done) {
      const options = utils.extend({}, defaultOptions);
      options.protocolOptions.maxVersion = protocolVersion.getLowerSupported(getProtocolVersion());
      const localCon = newInstance(null, null, options);
      localCon.open(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCon.protocolVersion, options.protocolOptions.maxVersion);
        localCon.close(done);
      });
    });
    it('should fail when the host does not exits', function (done) {
      const localCon = newInstance('1.1.1.1');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected);
        localCon.close(done);
      });
    });
    it('should fail when the host exists but port closed', function (done) {
      const localCon = newInstance('127.0.0.1:8090');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected);
        localCon.close(done);
      });
    });

    context('using timers', () => {
      let clock;

      before(() => clock = sinon.useFakeTimers({ shouldAdvanceTime: true }));
      after(() => clock.restore());

      it('should set the timeout for the heartbeat', function (done) {
        const c = sinon.spy(newInstance());

        c.open(function (err) {
          assert.ifError(err);

          const initialSendCalls = c.sendStream.callCount;

          // Default is 30s
          clock.tick(defaultOptions.pooling.heartBeatInterval);

          assert.strictEqual(c.sendStream.callCount, initialSendCalls + 1);

          const request = c.sendStream.getCall(c.sendStream.callCount-1).args[0];

          assert.strictEqual(request, requests.options);

          c.close();
          done();
        });
      });
    });
  });

  describe('#open with ssl', function () {
    before(helper.ccmHelper.start(1, {ssl: true}));
    after(helper.ccmHelper.remove);
    it('should open to a ssl enabled host', function (done) {
      const localCon = newInstance();
      localCon.options.sslOptions = {};
      localCon.open(function (err) {
        assert.ifError(err);
        assert.ok(localCon.connected, 'Must be status connected');
        localCon.sendStream(getRequest(helper.queries.basic), null, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.rows.length);
          localCon.close(done);
        });
      });
    });
  });
  describe('#changeKeyspace()', function () {
    before(helper.ccmHelper.start(1));
    // after(helper.ccmHelper.remove);
    it('should change active keyspace', function (done) {
      const localCon = newInstance();
      const keyspace = helper.getRandomName();
      utils.series([
        localCon.open.bind(localCon),
        function creating(next) {
          const query = 'CREATE KEYSPACE ' + keyspace + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1};';
          localCon.sendStream(getRequest(query), null, next);
        },
        function changing(next) {
          localCon.changeKeyspace(keyspace, next);
        },
        function asserting(next) {
          assert.strictEqual(localCon.keyspace, keyspace);
          next();
        }
      ], done);
    });
    it('should be case sensitive', function (done) {
      const localCon = newInstance();
      const keyspace = helper.getRandomName().toUpperCase();
      assert.notStrictEqual(keyspace, keyspace.toLowerCase());
      utils.series([
        localCon.open.bind(localCon),
        function creating(next) {
          const query = 'CREATE KEYSPACE "' + keyspace + '" WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1};';
          localCon.sendStream(getRequest(query), null, next);
        },
        function changing(next) {
          localCon.changeKeyspace(keyspace, next);
        },
        function asserting(next) {
          assert.strictEqual(localCon.keyspace, keyspace);
          next();
        }
      ], done);
    });
  });
  describe('#sendStream()', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should queue pending if there is not an available stream id', function (done) {
      const options = utils.extend({}, defaultOptions);
      options.socketOptions.readTimeout = 0;
      options.policies.retry = new helper.RetryMultipleTimes(3);
      const connection = newInstance(null, null, options);
      const maxRequests = connection.protocolVersion < 3 ? 128 : Math.pow(2, 15);
      utils.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          utils.times(maxRequests + 10, function (n, next) {
            const request = getRequest(helper.queries.basic);
            connection.sendStream(request, null, err => {
              if (err) {
                helper.assertInstanceOf(err, errors.ResponseError);
                assert.strictEqual(err.code, types.responseErrorCodes.readTimeout);
              }

              next();
            });
          }, seriesNext);
        }
      ], done);
    });
    it('should callback the pending queue if the connection is there is a socket error', function (done) {
      const options = utils.extend({}, defaultOptions);
      options.socketOptions.readTimeout = 0;
      options.policies.retry = new helper.RetryMultipleTimes(3);
      const connection = newInstance(null, null, options);
      const maxRequests = connection.protocolVersion < 3 ? 128 : Math.pow(2, 15);
      let killed = false;
      utils.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          utils.times(maxRequests + 10, function (n, next) {
            if (n === maxRequests + 9) {
              connection.netClient.destroy();
              killed = true;
              return next();
            }
            const request = getRequest('SELECT key FROM system.local');
            connection.sendStream(request, null, function (err) {
              if (killed && err) {
                assert.ok(err.isSocketError);
                err = null;
              }
              next(err);
            });
          }, seriesNext);
        },
        connection.close.bind(connection)
      ], done);
    });
    it('should not consume streamIds for requests that cant be serialized', function (done) {
      const options = utils.extend({}, defaultOptions);
      options.socketOptions.readTimeout = 0;
      options.policies.retry = new helper.RetryMultipleTimes(3);
      const connection = newInstance(null, null, options);
      const testError = new Error('Dummy serialization error');
      utils.series([
        next => connection.open(next),
        next => {
          setImmediate(() => {
            assert.strictEqual(connection.getInFlight(), 0);
            next();
          });
        },
        seriesNext => {
          utils.timesSeries(1, function (n, next) {
            const request = getRequest(helper.queries.basic);
            request.write = () => { throw testError; };
            connection.sendStream(request, null, err => {
              assert.strictEqual(err, testError);
              next();
            });
          }, seriesNext);
        },
        next => {
          setImmediate(() => {
            assert.strictEqual(connection.getInFlight(), 0);
            next();
          });
        },
        next => connection.close(next)
      ], done);
    });
  });
});

/** @returns {Connection} */
function newInstance(address, protocolVersion, options){
  if (!address) {
    address = helper.baseOptions.contactPoints[0];
  }
  if (typeof protocolVersion === 'undefined') {
    protocolVersion = getProtocolVersion();
  }
  //var logEmitter = function (name, type) { if (type === 'verbose') { return; } console.log.apply(console, arguments);};
  options = utils.deepExtend({logEmitter: helper.noop}, options || defaultOptions);

  const c = new Connection(address + ':' + options.protocolOptions.port, protocolVersion, options);

  after(() => c.close());
  return c;
}

function getRequest(query) {
  return new requests.QueryRequest(query, null, null);
}

/**
 * Gets the max supported protocol version for the current Cassandra version
 * @returns {number}
 */
function getProtocolVersion() {
  // Expected protocol version by server

  const serverInfo = helper.getServerInfo();

  if (serverInfo.isDse) {
    if (helper.isDseGreaterThan('6.0')) {
      return protocolVersion.dseV2;
    }

    if (serverInfo.version.startsWith('5.1')) {
      return protocolVersion.dseV1;
    }

    if (serverInfo.version.startsWith('4.8')) {
      return protocolVersion.v3;
    }

    return protocolVersion.v4;
  }

  if (serverInfo.version.startsWith('2.1')) {
    return protocolVersion.v3;
  }

  return protocolVersion.v4;
}