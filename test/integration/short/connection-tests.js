/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');

const Connection = require('../../../lib/connection.js');
const defaultOptions = require('../../../lib/client-options.js').defaultOptions();
const utils = require('../../../lib/utils.js');
const requests = require('../../../lib/requests.js');
const protocolVersion = require('../../../lib/types').protocolVersion;
const helper = require('../../test-helper.js');
const vit = helper.vit;

describe('Connection', function () {
  this.timeout(120000);
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
    it('should set the timeout for the heartbeat', function (done) {
      const options = utils.extend({}, defaultOptions);
      options.pooling.heartBeatInterval = 100;
      const c = newInstance(null, undefined, options);
      let sendCounter = 0;
      c.open(function (err) {
        assert.ifError(err);
        const originalSend = c.sendStream;
        c.sendStream = function() {
          sendCounter++;
          originalSend.apply(c, arguments);
        };
        setTimeout(function () {
          assert.ok(sendCounter > 3, 'sendCounter ' + sendCounter);
          done();
        }, 600);
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
          localCon.sendStream(getRequest(query), {}, next);
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
          localCon.sendStream(getRequest(query), {}, next);
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
  describe('#execute()', function () {
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
            connection.sendStream(request, null, next);
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
  return new Connection(address + ':' + options.protocolOptions.port, protocolVersion, options);
}

function getRequest(query) {
  return new requests.QueryRequest(query, null, null);
}

/**
 * Gets the max supported protocol version for the current Cassandra version
 * @returns {number}
 */
function getProtocolVersion() {
  // expected protocol version
  var dseVersion = helper.getDseVersion();
  if (dseVersion.indexOf('5.1') === 0) {
    return protocolVersion.dseV1;
  }
  if (dseVersion.indexOf('5.0') === 0) {
    return protocolVersion.v4;
  }
  if (dseVersion.indexOf('4.8') === 0) {
    return protocolVersion.v3;
  }
  return 4;
}