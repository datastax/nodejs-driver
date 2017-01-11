"use strict";
var assert = require('assert');

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
  before(helper.ccmHelper.start('2:2'));
  after(helper.ccmHelper.remove);

  /**
   * Updates the input policy's init method to increment a _initCalled counter to track the number of times
   * init is called.
   */
  function decorateInitWithCounter(policy) {
    var baseInit = policy.init;
    policy._initCalled = 0;
    policy.init = function () {
      policy._initCalled++;
      baseInit.apply(policy, arguments);
    };
    return policy;
  }

  function createProfiles() {
    // A profile that targets dc1.
    var dc1Profile = new ExecutionProfile('default', {
      loadBalancing: decorateInitWithCounter(new DCAwareRoundRobinPolicy('dc1'))
    });
    // A profile that targets 127.0.0.4 specifically.
    var wlProfile = new ExecutionProfile('whitelist', {
      loadBalancing: decorateInitWithCounter(new WhiteListPolicy(new DCAwareRoundRobinPolicy('dc2'), [helper.ipPrefix + '4:9042']))
    });
    // A profile with no defined lbp, it should fallback on the default profile's lbp.
    var emptyProfile = new ExecutionProfile('empty');

    return [dc1Profile, wlProfile, emptyProfile];
  }


  function newInstance(options, profiles) {
    options = options || {};
    options = utils.deepExtend({
      profiles: profiles || createProfiles()
    }, helper.baseOptions, options);
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
              if (err) {
                return timesNext(err);
              }
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
      ], done);
    });
  }

  it('should init each profile\'s load balancing policy exactly once', function(done) {
    var client = newInstance();
    utils.series([
      client.connect.bind(client),
      function validateInitCount(next) {
        // Ensure each lbp was only init'd once.
        client.options.profiles.forEach(function (p) {
          assert.strictEqual(p.loadBalancing._initCalled, 1);
        });
        next();
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should consider all load balancing policies when establishing distance for hosts', function (done) {
    var client = newInstance();

    utils.series([
      client.connect.bind(client),
      function validateHostDistances(next) {
        var hosts = client.hosts;
        assert.strictEqual(hosts.length, 4);
        hosts.forEach(function(h) {
          var n = helper.lastOctetOf(h);
          var distance = client.profileManager.getDistance(h);
          // all hosts except 3 should be at a distance of local since a profile exists for all DCs
          // with DC2 white listing host 4.  While host 5 is ignored in whitelist profile, it is remote in others
          // so it should be considered remote.
          var expectedDistance = n === '3' ? types.distance.remote : types.distance.local;
          assert.strictEqual(distance, expectedDistance, "Expected distance of " + expectedDistance + " for host " + n);
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
  it('should only use hosts from the load balancing policy in whitelist profile', ensureOnlyHostsUsed(['4'], 'whitelist'));
  it('should fallback on client load balancing policy when default profile has no lbp', function (done) {
    var policy = new DCAwareRoundRobinPolicy('dc2');
    var profiles = [new ExecutionProfile('default'), new ExecutionProfile('empty')];
    // also provide retry policy since the default would be overridden by provided policies options.
    var client = newInstance({policies: {loadBalancing: policy, retry: new helper.RetryMultipleTimes(3)}}, profiles);
    var hostsUsed = {};

    utils.series([
      client.connect.bind(client),
      function executeQueries(next) {
        utils.timesLimit(100, 25, function(n, timesNext) {
          client.execute(helper.queries.basic, [], {executionProfile: 'empty'}, function (err, result) {
            if (err) {return timesNext(err);}
            hostsUsed[helper.lastOctetOf(result.info.queriedHost)] = true;
            timesNext();
          });
        }, function (err) {
          assert.ifError(err);
          // Should have only used hosts 3 and 4 since those are in dc2 and the client policy is DCAware on dc2.
          assert.deepEqual(Object.keys(hostsUsed), ['3', '4']);
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
});