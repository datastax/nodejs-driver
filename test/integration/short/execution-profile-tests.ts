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
import assert from "assert";
import helper from "../../test-helper";
import Client from "../../../lib/client";
import types from "../../../lib/types/index";
import utils from "../../../lib/utils";
import simulacron from "../simulacron";
import { AllowListPolicy, DCAwareRoundRobinPolicy } from "../../../lib/policies/load-balancing";
import { ExecutionProfile } from "../../../lib/execution-profile";

'use strict';
describe('ProfileManager', function() {
  this.timeout(40000);
  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  /** @type {SimulacronCluster} */
  let cluster = null;
  const nodes = [];
  let node3 = null;
  let node4 = null;

  before(done => {
    cluster = new simulacron.SimulacronCluster();
    cluster.register([2, 2], null, done);
  });

  before(() => {
    node3 = cluster.dc(1).node(0).address;
    node4 = cluster.dc(1).node(1).address;
    nodes.push(cluster.dc(0).node(0).address, cluster.dc(0).node(1).address, node3, node4);
  });


  /**
   * Updates the input policy's init method to increment a _initCalled counter to track the number of times
   * init is called.
   */
  function decorateInitWithCounter(policy) {
    const baseInit = policy.init;
    policy._initCalled = 0;
    policy.init = function () {
      policy._initCalled++;
      baseInit.apply(policy, arguments);
    };
    return policy;
  }

  function createProfiles() {
    // A profile that targets dc1.
    const dc1Profile = new ExecutionProfile('default', {
      loadBalancing: decorateInitWithCounter(new DCAwareRoundRobinPolicy('dc1'))
    });
    // A profile that targets 127.0.0.4 specifically.
    const wlProfile = new ExecutionProfile('allowlist', {
      loadBalancing: decorateInitWithCounter(new AllowListPolicy(new DCAwareRoundRobinPolicy('dc2'), [ node4 ]))
    });
    // A profile with no defined lbp, it should fallback on the default profile's lbp.
    const emptyProfile = new ExecutionProfile('empty');

    return [dc1Profile, wlProfile, emptyProfile];
  }


  function newInstance(options, profiles) {
    options = options || {};
    options = utils.deepExtend({
      profiles: profiles || createProfiles(),
      contactPoints: cluster.getContactPoints()
    }, options);

    return helper.shutdownAfterThisTest(new Client(options));
  }

  function ensureOnlyHostsUsed(nodeIndexes, profile) {
    return (function test(done) {
      const queryOptions = profile ? {executionProfile: profile} : {};
      const hostsUsed = new Set();

      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function executeQueries(next) {
          utils.timesLimit(100, 25, function(n, timesNext) {
            client.execute(helper.queries.basic, [], queryOptions, function (err, result) {
              if (err) {
                return timesNext(err);
              }
              hostsUsed.add(result.info.queriedHost);
              timesNext();
            });
          }, function (err) {
            assert.ifError(err);
            assert.deepStrictEqual(Array.from(hostsUsed).sort(), nodeIndexes.map(i => nodes[i]).sort());
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  }

  it('should init each profile\'s load balancing policy exactly once', function(done) {
    const client = newInstance();
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
    const client = newInstance();

    utils.series([
      client.connect.bind(client),
      function validateHostDistances(next) {
        const hosts = client.hosts;
        assert.strictEqual(hosts.length, 4);

        hosts.forEach(function(h) {
          const distance = client.profileManager.getDistance(h);
          // all hosts except 3 should be at a distance of local since a profile exists for all DCs
          // with DC2 allow-listing host 4.  While host 5 is ignored in allowlist profile, it is remote in others
          // so it should be considered remote.
          const expectedDistance = h.address === node3 ? types.distance.ignored : types.distance.local;
          assert.strictEqual(distance, expectedDistance, "Expected distance of " + expectedDistance + " for host " + h.address);
          assert.ok(h.isUp());
        });
        next();
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should only use hosts from the load balancing policy in the default profile', ensureOnlyHostsUsed([0, 1]));
  it('should only use hosts from the load balancing policy in the default profile when profile doesn\'t have policy', ensureOnlyHostsUsed([0, 1], 'empty'));
  it('should only use hosts from the load balancing policy in the default profile when specified', ensureOnlyHostsUsed([0, 1], 'default'));
  it('should only use hosts from the load balancing policy in allowlist profile', ensureOnlyHostsUsed([3], 'allowlist'));
  it('should fallback on client load balancing policy when default profile has no lbp', function (done) {
    const policy = new DCAwareRoundRobinPolicy('dc2');
    const profiles = [new ExecutionProfile('default'), new ExecutionProfile('empty')];
    // also provide retry policy since the default would be overridden by provided policies options.
    const client = newInstance({policies: {loadBalancing: policy, retry: new helper.RetryMultipleTimes(3)}}, profiles);
    const hostsUsed = new Set();

    utils.series([
      client.connect.bind(client),
      function executeQueries(next) {
        utils.timesLimit(100, 25, function(n, timesNext) {
          client.execute(helper.queries.basic, [], {executionProfile: 'empty'}, function (err, result) {
            if (err) {return timesNext(err);}
            hostsUsed.add(result.info.queriedHost);
            timesNext();
          });
        }, function (err) {
          assert.ifError(err);
          // Should have only used hosts 3 and 4 since those are in dc2 and the client policy is DCAware on dc2.
          assert.deepStrictEqual(Array.from(hostsUsed).sort(), [node3, node4]);
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
});