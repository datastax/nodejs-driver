var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var ControlConnection = require('../../../lib/control-connection.js');
var utils = require('../../../lib/utils.js');
var clientOptions = require('../../../lib/client-options.js');

describe('ControlConnection', function () {
  this.timeout(120000);
  describe('#init()', function () {
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should retrieve local host and peers', function (done) {
      var cc = newInstance();
      cc.init(function (err) {
        assert.ifError(err);
        assert.strictEqual(cc.hosts.length, 2);
        assert.ok(cc.protocolVersion);
        cc.hosts.forEach(function (h) {
          assert.ok(h.datacenter);
          assert.ok(h.rack);
          assert.ok(h.tokens);
        });
        done();
      });
    });
    it('should subscribe to SCHEMA_CHANGE events and refresh keyspace information', function (done) {
      var cc = newInstance();
      async.series([
        cc.init.bind(cc),
        function createKeyspace(next) {
          var query = "CREATE KEYSPACE sample_change_1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}";
          helper.ccmHelper.exec(['node1', 'cqlsh', '--exec', query], helper.wait(500, next));
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(keyspaceInfo);
          assert.ok(keyspaceInfo.strategy);
          assert.equal(keyspaceInfo.strategyOptions.replication_factor, 3);
          assert.ok(keyspaceInfo.strategy.indexOf('SimpleStrategy') > 0);
          next();
        },
        function alterKeyspace(next) {
          var query = "ALTER KEYSPACE sample_change_1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}";
          helper.ccmHelper.exec(['node1', 'cqlsh', '--exec', query], helper.wait(500, next));
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(keyspaceInfo);
          assert.equal(keyspaceInfo.strategyOptions.replication_factor, 2);
          next();
        },
        function alterKeyspace(next) {
          var query = "DROP keyspace sample_change_1";
          helper.ccmHelper.exec(['node1', 'cqlsh', '--exec', query], helper.wait(500, next));
        },
        function (next) {
          var keyspaceInfo = cc.metadata.keyspaces['sample_change_1'];
          assert.ok(!keyspaceInfo);
          next();
        }
      ], done);
    });
    it('should subscribe to STATUS_CHANGE events', function (done) {
      var cc = newInstance();
      cc.init(function () {
        helper.ccmHelper.exec(['node2', 'stop'], function (err) {
          if (err) return done(err);
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 2);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 1);
            done();
          }, 3000);
        });
      });
    });
    it('should subscribe to TOPOLOGY_CHANGE add events and refresh ring info', function (done) {
      var cc = newInstance();
      async.series([
        cc.init.bind(cc),
        function (next) {
          //add a node
          helper.ccmHelper.bootstrapNode(3, next);
        },
        function (next) {
          //start the node
          helper.ccmHelper.startNode(3, next);
        },
        function (next) {
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 3);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 2);
            next();
          }, 3000);
        }
      ], done);
    });
    it('should subscribe to TOPOLOGY_CHANGE remove events and refresh ring info', function (done) {
      var cc = newInstance();
      async.series([
        cc.init.bind(cc),
        function (next) {
          //decommission node
          helper.ccmHelper.exec(['node2', 'decommission'], helper.wait(3000, next));
        },
        function (next) {
          var hosts = cc.hosts.slice(0);
          assert.strictEqual(hosts.length, 1);
          next();
        }
      ], done);
    });
    it('should reconnect when host used goes down', function (done) {
      var cc = newInstance();
      cc.init(function () {
        //initialize the load balancing policy
        cc.options.policies.loadBalancing.init(null, cc.hosts, function () {});
        //it should be using the first node: kill it
        helper.ccmHelper.exec(['node1', 'stop'], function (err) {
          if (err) return done(err);
          //A little help here
          cc.hosts.slice(0)[0].setDown();
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 2);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 1);
            done();
          }, 3000);
        });
      });
    });
  });
  describe('#metadata', function () {
    before(helper.ccmHelper.start(3, {vnodes: true}));
    after(helper.ccmHelper.remove);
    it('should contain keyspaces information', function (done) {
      var cc = newInstance();
      cc.init(function () {
        assert.equal(cc.hosts.length, 3);
        assert.ok(cc.metadata);
        assert.strictEqual(cc.hosts.slice(0)[0].tokens.length, 256);
        assert.ok(cc.metadata.keyspaces);
        assert.ok(cc.metadata.keyspaces['system']);
        assert.ok(cc.metadata.keyspaces['system'].strategy);
        assert.strictEqual(typeof cc.metadata.keyspaces['system'].tokenToReplica, 'function');
        done();
      });
    });
  });
});

/** @returns {ControlConnection} */
function newInstance() {
  var options = clientOptions.extend(utils.extend({}, helper.baseOptions));
  //disable the heartbeat
  options.pooling.heartBeatInterval = 0;
  return new ControlConnection(options);
}