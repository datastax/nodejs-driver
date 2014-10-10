var assert = require('assert');
var util = require('util');
var async = require('async');

var Connection = require('../../../lib/connection.js');
var defaultOptions = require('../../../lib/client-options.js').defaultOptions();
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var requests = require('../../../lib/requests.js');
var helper = require('../../test-helper.js');

describe('Connection', function () {
  this.timeout(30000);
  describe('#open()', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should open', function (done) {
      var localCon = newInstance();
      localCon.open(function (err) {
        assert.ifError(err);
        assert.ok(localCon.connected && !localCon.connecting, 'Must be status connected');
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
    it('should fail when the host does not exits', function (done) {
      var localCon = newInstance('1.1.1.1');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected && !localCon.connecting);
        localCon.close(done);
      });
    });
    it('should fail when the host exists but port closed', function (done) {
      var localCon = newInstance('127.0.0.1:8090');
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected && !localCon.connecting);
        localCon.close(done);
      });
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
        assert.ok(localCon.connected && !localCon.connecting, 'Must be status connected');
        localCon.sendStream(getRequest('SELECT * FROM system.schema_keyspaces'), null, function (err, result) {
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
      async.series([
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
      async.series([
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
      var connection = newInstance();
      async.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          async.times(connection.maxRequests * 2, function (n, next) {
            var request = getRequest('SELECT * FROM system.schema_keyspaces');
            connection.sendStream(request, null, next);
          }, seriesNext);
        }
      ], done);
    });
    it('should callback the pending queue if the connection is there is a socket error', function (done) {
      var connection = newInstance();
      var killed = false;
      async.series([
        connection.open.bind(connection),
        function asserting(seriesNext) {
          async.times(connection.maxRequests * 2, function (n, next) {
            if (n === connection.maxRequests * 2 - 1) {
              connection.netClient.destroy();
              killed = true;
              return next();
            }
            var request = getRequest('SELECT * FROM system.schema_keyspaces');
            connection.sendStream(request, null, function (err) {
              if (killed && err) {
                assert.ok(err.isServerUnhealthy);
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

function newInstance(address, protocolVersion){
  if (!address) {
    address = helper.baseOptions.contactPoints[0];
  }
  if (typeof protocolVersion === 'undefined') {
    protocolVersion = getProtocolVersion();
  }
  //var logEmitter = function (name, type) { if (type === 'verbose') { return; } console.log.apply(console, arguments);};
  var logEmitter = function () {};
  var options = utils.extend({logEmitter: logEmitter}, defaultOptions);
  return new Connection(address, protocolVersion, options);
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
  var expectedVersion = 1;
  if (helper.getCassandraVersion().indexOf('2.') === 0) {
    expectedVersion = 2;
  }
  //protocol v3 not supported yet
  return expectedVersion;
}