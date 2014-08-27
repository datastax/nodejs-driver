var assert = require('assert');
var util = require('util');
var async = require('async');

var Connection = require('../../../index.js').Connection;
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var writers = require('../../../lib/writers.js');
var helper = require('../../test-helper.js');

describe('Connection', function () {
  this.timeout(120000);
  before(helper.ccmHelper.start(1));
  after(helper.ccmHelper.remove);
  describe('#open()', function () {
    it('should open', function (done) {
      var localCon = newInstance();
      localCon.open(function (err) {
        assert.equal(err, null);
        assert.ok(localCon.connected && !localCon.connecting, 'Must be status connected');
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
  });
  describe('#changeKeyspace()', function () {
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
});

function newInstance(address){
  if (!address) {
    address = helper.baseOptions.contactPoints[0];
  }
  return new Connection(address, {});
}

function getRequest(query) {
  return new writers.QueryWriter(query, [], types.consistencies.one, null, null);
}