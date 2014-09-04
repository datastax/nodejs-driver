var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var loadBalancing = require('../../../lib/policies/load-balancing.js');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;


describe('DCAwareRoundRobinPolicy', function () {
  this.timeout(120000);
  it('should never hit remote dc if not set', function (done) {
    var countByHost = {};
    async.series([
      function createTestCluster(next) {
        //1 cluster with 3 dcs with 2 nodes each
        helper.ccmHelper.start('2:2:2')(next);
      },
      function testCase(next) {
        var options = utils.deepExtend({}, helper.baseOptions, {policies: {loadBalancing: new DCAwareRoundRobinPolicy()}});
        var client = new Client(options);
        var prevHost = null;
        async.times(120, function (n, timesNext) {
          client.execute('SELECT * FROM system.schema_columnfamilies', function (err, result) {
            assert.ifError(err);
            assert.ok(result && result.rows);
            var hostId = result._queriedHost;
            assert.ok(hostId);
            var h = client.hosts.get(hostId);
            assert.ok(h);
            assert.strictEqual(h.datacenter, 'dc1');
            prevHost = h;
            countByHost[hostId] = (countByHost[hostId] || 0) + 1;
            timesNext();
          });
        }, next);
      },
      function assertHosts(next) {
        var hostsQueried = Object.keys(countByHost);
        assert.strictEqual(hostsQueried.length, 2);
        assert.strictEqual(countByHost[hostsQueried[0]], countByHost[hostsQueried[1]]);
        next();
      },
      function destroyCluster(next) {
        //1 cluster with 3 dcs with 2 nodes each
        helper.ccmHelper.remove(next);
      }
    ], done);
  });
});



/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}
