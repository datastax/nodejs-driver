"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var loadBalancing = require('../../../lib/policies/load-balancing.js');
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
var WhiteListPolicy = loadBalancing.WhiteListPolicy;
var ExecutionProfile = require('../../../lib/execution-profile.js').ExecutionProfile;

describe('ProfileManager', function() {
  this.timeout(120000);
  before(helper.ccmHelper.start('2:2:2'));
  after(helper.ccmHelper.remove);

  var dc1Profile = new ExecutionProfile('default', {
    loadBalancing: new DCAwareRoundRobinPolicy('dc1')
  });
  var dc2Profile = new ExecutionProfile('DC2', {
    loadBalancing: new DCAwareRoundRobinPolicy('dc2')
  });
  var wlProfile = new ExecutionProfile('whitelist', {
    loadBalancing: new WhiteListPolicy(new DCAwareRoundRobinPolicy('dc3'), [helper.ipPrefix + '6' + ":9042"])
  });
  var emptyProfile = new ExecutionProfile('empty');

  var profiles = [dc1Profile, dc2Profile, wlProfile, emptyProfile];

  function newInstance(options) {
    options = options || {};
    options = utils.extend({
      profiles: profiles
    }, options, helper.baseOptions);
    return new Client(options);
  }

  function ensureOnlyHostsUsed(hostOctets, profile) {
    return (function test(done) {
      var queryOptions = profile ? {executionProfile: profile} : {};
      var hostsUsed = {};

      var client = newInstance();
      utils.series([
        client.connect.bind(client),
        function executeQueries(next) {
          utils.timesLimit(100, 25, function(n, timesNext) {
            client.execute(helper.queries.basic, [], queryOptions, function (err, result) {
              if (err) return timesNext(err);
              hostsUsed[helper.lastOctetOf(result.info.queriedHost)] = true;
              timesNext();
            });
          }, function (err) {
            assert.ifError(err);
            assert.deepEqual(Object.keys(hostsUsed), hostOctets);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done)
    });
  }

  it('should consider all load balancing policies when establishing distance for hosts', function (done) {
    var client = newInstance();
    utils.series([
      client.connect.bind(client),
      function validateHostDistances(next) {
        var hosts = client.hosts;
        assert.strictEqual(hosts.length, 6);
        hosts.forEach(function(h) {
          var n = helper.lastOctetOf(h);
          var distance = client.profileManager.getDistance(h);
          // all hosts except 5 should be at a distance of local since a profile exists for all DCs
          // with DC3 white listing host 6.  While host 5 is ignored in whitelist profile, it is remote in others
          // so it should be considered remote.
          var expectedDistance = n == 5 ? types.distance.remote : types.distance.local;
          assert.strictEqual(distance, expectedDistance, "Expected distance of " + expectedDistance + " for host 5");
          assert.ok(h.isUp());
        });
        next();
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should only use hosts from the load balancing policy in the default profile', ensureOnlyHostsUsed(['1', '2']));
  it('should only use hosts from the load balancing policy in the default profile when profile doesn\'t have policy', ensureOnlyHostsUsed(['1', '2'], 'empty'));
  it('should only use hosts from the load balancing policy in the default profile when specified', ensureOnlyHostsUsed(['1', '2'], 'default'));
  it('should only use hosts from the load balancing policy in DC2 profile', ensureOnlyHostsUsed(['3', '4'], 'DC2'));
  it('should only use hosts from the load balancing policy in whitelist profile', ensureOnlyHostsUsed(['6'], 'whitelist'));
});