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

'use strict';

const assert = require('assert');
const Client = require('../../../../lib/client');
const helper = require('../../../test-helper');
const vdescribe = helper.vdescribe;
const loadBalancing = require('../../../../lib/policies/load-balancing');
const DefaultLoadBalancingPolicy = loadBalancing.DefaultLoadBalancingPolicy;
const ExecutionProfile = require('../../../../lib/execution-profile').ExecutionProfile;
const utils = require('../../../../lib/utils');
const graphModule = require('../../../../lib/graph');
const graphTestHelper = require('./graph-test-helper');

vdescribe('dse-5.0', 'Client with spark workload', function () {
  this.timeout(360000);
  before(function (done) {
    const client = new Client(helper.getOptions());
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {workloads: ['graph', 'spark']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        const replicationConfig = "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
        const query = `system.graph("name1")
          .option("graph.replication_config").set(replicationConfig)
          .option("graph.system_replication_config").set(replicationConfig)
          .ifNotExists()
          ${helper.isDseGreaterThan('6.8') ? '.classicEngine()' : ''}
          .create()`;
        client.executeGraph(query, {replicationConfig: replicationConfig}, {graphName: null}, function(err) {
          assert.ifError(err);
          next();
        });
      },
      function waitForWorkers(next) {
        // Wait for master to come online before altering keyspace as it needs to meet LOCAL_QUORUM CL to start, and
        // that can't be met with 1/NUM_NODES available.
        helper.waitForWorkers(client, 1, next);
      },
      function updateDseLeases(next) {
        // Set the dse_leases keyspace to RF of 2, this will prevent election of new job tracker until all nodes
        // are available, preventing weird cases where 1 node thinks the wrong node is a master.
        client.execute(
          "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '2'}",
          next);
      },
      function addNode(next) {
        helper.ccm.bootstrapNode({ nodeIndex: 2, dc: 'dc1' }, next);
      },
      function setNodeWorkload(next) {
        helper.ccm.setWorkload(2, ['graph', 'spark'], next);
      },
      function startNode(next) {
        helper.ccm.startNode(2, next);
      },
      function waitForWorkers(next) {
        helper.waitForWorkers(client, 2, next);
      },
      next => graphTestHelper.createModernGraph(client, next),
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  describe('#connect()', function () {
    it('should obtain DSE workload', function (done) {
      const client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        const host = client.hosts.values()[0];
        if (helper.isDseGreaterThan('5.1')) {
          assert.deepEqual(host.workloads, [ 'Analytics', 'Cassandra', 'Graph' ]);
        }
        else {
          assert.deepEqual(host.workloads, [ 'Analytics' ]);
        }
        done();
      });
    });
  });
  describe('#executeGraph()', function () {

    function executeAnalyticsQueries(queryOptions, options, shouldQueryMasterOnly) {
      return wrapClient(function(client, done) {
        helper.findSparkMaster(client, function (serr, sparkMaster) {
          assert.ifError(serr);
          utils.timesSeries(5, function (n, timesNext) {
            client.executeGraph('g.V().count()', null, queryOptions, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.ok(result.info);
              assert.strictEqual(6, result.first());
              if(shouldQueryMasterOnly) {
                // Ensure the master was the queried host.
                let queriedHost = result.info.queriedHost;
                const portSep = queriedHost.lastIndexOf(":");
                queriedHost = portSep !== -1 ? queriedHost.substr(0, portSep) : queriedHost;
                assert.strictEqual(queriedHost, sparkMaster);
              }
              timesNext(err, result.info.queriedHost);
            });
          }, function () {
            done();
          });
        });
      }, options);
    }

    it('should make an OLAP query using \'a\' traversal source', executeAnalyticsQueries({graphSource: 'a'}));
    it('should make an OLAP query using profile with \'a\' traversal source',
      executeAnalyticsQueries({executionProfile: 'analytics'}, {profiles: [new ExecutionProfile('analytics', {graphOptions: {source: 'a'}})]}));
    it('should make an OLAP query with default profile using \'a\' traversal source',
      executeAnalyticsQueries({}, {profiles: [new ExecutionProfile('default', {graphOptions: {source: 'a'}})]}));
    it('should contact spark master directly to make an OLAP query when using DefaultLoadBalancingPolicy',
      executeAnalyticsQueries({graphSource: 'a'}, {policies: {loadBalancing: new DefaultLoadBalancingPolicy()}}, true));
    it('should contact spark master directly to make an OLAP query when using profile with DefaultLoadBalancingPolicy',
      executeAnalyticsQueries({executionProfile: 'analytics'}, {profiles: [new ExecutionProfile('analytics',
        {loadBalancing: new DefaultLoadBalancingPolicy(), graphOptions: {source: 'a'}})]}, true)
    );
    context('with no callback specified', function () {
      it('should return a promise for OLAP query', function () {
        const client = newInstance();
        const p = client.executeGraph('g.V().count()', { graphSource: 'a' });
        helper.assertInstanceOf(p, Promise);
        return p.then(function (result) {
          helper.assertInstanceOf(result, graphModule.GraphResultSet);
          assert.strictEqual(typeof result.first(), 'number');
        });
      });
    });
  });
});

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    const client = newInstance(options);
    utils.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
}

function newInstance(options) {
  const opts = helper.getOptions(utils.extend(options || {}, { graphOptions : { name: 'name1' }}));
  return new Client(opts);
}
