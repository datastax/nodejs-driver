"use strict";
var assert = require('assert');
var util = require('util');

var Connection = require('../../../lib/connection.js');
var defaultOptions = require('../../../lib/client-options.js').defaultOptions();
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var requests = require('../../../lib/requests.js');
var helper = require('../../test-helper.js');
var vit = helper.vit;

describe('Connection', function () {
  this.timeout(120000);
  describe('#open()', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should open', function (done) {
      var localCon = newInstance();
      localCon.open(function (err) {
        assert.ifError(err);
        assert.ok(localCon.connected, 'Must be status connected');
        localCon.close(done);
      });
    });
    it('should use the max supported protocol version', function (done) {
      var localCon = newInstance(null, null);
      localCon.open(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCon.protocolVersion, getProtocolVersion());
        assert.strictEqual(localCon.checkingVersion, true);
        localCon.close(done);
      });
    });
    vit('2.0', 'should limit the max protocol version based on the protocolOptions', function (done) {
      var options = utils.extend({}, defaultOptions);
      options.protocolOptions.maxVersion =  getProtocolVersion() - 1;
      var localCon = newInstance(null, null, options);
      localCon.open(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCon.protocolVersion, options.protocolOptions.maxVersion);
        assert.strictEqual(localCon.checkingVersion, true);
        localCon.close(done);
      });
    });
    it('should open with all the protocol versions supported', function (done) {
      var maxProtocolVersionSupported = getProtocolVersion();
      var minProtocolVersionSupported = getMinProtocolVersion();
      if(helper.getCassandraVersion())
      var protocolVersion = minProtocolVersionSupported - 1;
      utils.whilst(function condition() {
        return (++protocolVersion) <= maxProtocolVersionSupported;
      }, function iterator (next) {
        var localCon = newInstance(null, protocolVersion);
        localCon.open(function (err) {
          assert.ifError(err);
          assert.ok(localCon.connected, 'Must be status connected');
          localCon.close(next);
        });
      }, done);
    });
    it('should fail when the host does not exits', function (done) {
      var localCon = newInstance('1.1.1.1');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected);
        localCon.close(done);
      });
    });
    it('should fail when the host exists but port closed', function (done) {
      var localCon = newInstance('127.0.0.1:8090');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected);
        localCon.close(done);
      });
    });
    it('should set the timeout for the hearbeat', function (done) {
      var options = utils.extend({}, defaultOptions);
      options.pooling.heartBeatInterval = 200;
      var c = newInstance(null, undefined, options);
      var sendCounter = 0;
      c.open(function (err) {
        assert.ifError(err);
        assert.ok(c.idleTimeout);
        var originalSend = c.sendStream;
        c.sendStream = function() {
          sendCounter++;
          originalSend.apply(c, Array.prototype.slice.call(arguments));
        };
      });
      setTimeout(function () {
        //wait for requests to take place
        //at least 3 requests
        assert.ok(sendCounter > 2, 'sendCounter ' + sendCounter);
        done();
      }, 1000)
    });
  });
  describe('#open with ssl', function () {
    before(helper.ccmHelper.start(1, {ssl: true}));
    after(helper.ccmHelper.remove);
    it('should open to a ssl enabled host', function (done) {
      var localCon = newInstance();
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
    after(helper.ccmHelper.remove);
    it('should change active keyspace', function (done) {
      var localCon = newInstance();
      var keyspace = helper.getRandomName();
      utils.series([
        localCon.open.bind(localCon),
        function creating(next) {
          var query = 'CREATE KEYSPACE ' + keyspace + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1};';
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
      var localCon = newInstance();
      var keyspace = helper.getRandomName().toUpperCase();
      assert.notStrictEqual(keyspace, keyspace.toLowerCase());
      utils.series([
        localCon.open.bind(localCon),
        function creating(next) {
          var query = 'CREATE KEYSPACE "' + keyspace + '" WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1};';
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
      var options = utils.extend({}, defaultOptions);
      options.socketOptions.readTimeout = 0;
      options.policies.retry = new helper.RetryMultipleTimes(3);
      var connection = newInstance(null, null, options);
      var maxRequests = connection.protocolVersion < 3 ? 128 : Math.pow(2, 15);
      utils.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          utils.times(maxRequests + 10, function (n, next) {
            var request = getRequest(helper.queries.basic);
            connection.sendStream(request, null, next);
          }, seriesNext);
        }
      ], done);
    });
    it('should callback the pending queue if the connection is there is a socket error', function (done) {
      var options = utils.extend({}, defaultOptions);
      options.socketOptions.readTimeout = 0;
      options.policies.retry = new helper.RetryMultipleTimes(3);
      var connection = newInstance(null, null, options);
      var maxRequests = connection.protocolVersion < 3 ? 128 : Math.pow(2, 15);
      var killed = false;
      utils.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          utils.times(maxRequests + 10, function (n, next) {
            if (n === maxRequests + 9) {
              connection.netClient.destroy();
              killed = true;
              return next();
            }
            var request = getRequest('SELECT key FROM system.local');
            connection.sendStream(request, null, function (err) {
              if (killed && err) {
                assert.ok(err.isSocketError);
                err = null;
              }
              next(err);
            });
          }, seriesNext);
        }
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
  options = utils.extend({logEmitter: helper.noop}, options || defaultOptions);
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
  //expected protocol version
  if (helper.getCassandraVersion().indexOf('2.1.') === 0) {
    return 3;
  }
  if (helper.getCassandraVersion().indexOf('2.0.') === 0) {
    return 2;
  }
  if (helper.getCassandraVersion().indexOf('1.') === 0) {
    return 1;
  }
  return 4;
}

/**
 * Gets the minimum supported protocol version for the current Cassandra version
 *
 * For < C* 3.0 returns 1.  Otherwise returns maximum supported protocol
 * version - 1.
 *
 * @returns {number}
 */
function getMinProtocolVersion() {
  if (helper.getCassandraVersion().indexOf('2') === 0
    || helper.getCassandraVersion().indexOf('1') === 0) {
    return 1;
  } else {
    return getProtocolVersion() - 1;
  }
}