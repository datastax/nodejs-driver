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

const { assert } = require('chai');
const util = require('util');
const Client = require('../../../../lib/client');
const promiseUtils = require('../../../../lib/promise-utils');
const helper = require('../../../test-helper');
const vdescribe = helper.vdescribe;
const loadBalancing = require('../../../../lib/policies/load-balancing');
const DefaultLoadBalancingPolicy = loadBalancing.DefaultLoadBalancingPolicy;
const ExecutionProfile = require('../../../../lib/execution-profile').ExecutionProfile;
const utils = require('../../../../lib/utils');
const graphModule = require('../../../../lib/datastax/graph');
const graphTestHelper = require('./graph-test-helper');

vdescribe('dse-5.0', 'Client with spark workload', function () {
  this.timeout(360000);

  const client = new Client(helper.getOptions());
  let sparkWorkersReady = false;

  before(done => helper.ccm.startAll(1, {workloads: ['graph', 'spark']}, done));
  before(() => client.connect());
  before(() => {
    const replicationConfig = "{'class' : 'SimpleStrategy', 'replication_factor' : 1}";
    const query = `system.graph("name1")
          .option("graph.replication_config").set(replicationConfig)
          .option("graph.system_replication_config").set(replicationConfig)
          .ifNotExists()
          ${helper.isDseGreaterThan('6.8') ? '.classicEngine()' : ''}
          .create()`;
    return client.executeGraph(query, {replicationConfig: replicationConfig}, { graphName: null });
  });

  // Wait for master to come online before altering keyspace as it needs to meet LOCAL_QUORUM CL to start, and
  // that can't be met with 1/NUM_NODES available.
  before(() => waitForWorkers(client, 1));

  // Set the dse_leases keyspace to RF of 2, this will prevent election of new job tracker until all nodes
  // are available, preventing weird cases where 1 node thinks the wrong node is a master.
  before(() => client.execute(
    `ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '2'}`));
  before(done => helper.ccm.bootstrapNode({ nodeIndex: 2, dc: 'dc1' }, done));
  before(done => helper.ccm.setWorkload(2, ['graph', 'spark'], done));
  before(done => helper.ccm.startNode(2, done));

  before(async () => {
    sparkWorkersReady = await waitForWorkers(client, 2);
  });

  before(done => graphTestHelper.createModernGraph(client, done));

  before(() => client.shutdown());
  after(helper.ccm.remove.bind(helper.ccm));

  beforeEach(function() {
    if (!sparkWorkersReady) {
      // Graph OLAP tests are run under best effort.
      // Given the constrained resources on Jenkins, setup of spark+graph can fail from time to time.
      this.skip();
    }
  });

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
        findSparkMaster(client, function (serr, sparkMaster) {
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
  return helper.shutdownAfterThisTest(new Client(opts));
}

/**
 * Checks the spark cluster until there are the number of desired expected workers.  This
 * is required as by the time a node is up and listening on the CQL interface it is not a given
 * that it is partaking as a spark worker.
 *
 * Unfortunately there isn't a very good way to check that workers are listening as spark will choose an arbitrary
 * port for the worker and there is no other interface that exposes how many workers are active.  The best
 * alternative is to do a GET on http://master:7080/ and do a regular expression match to resolve the number of
 * active workers.  This could be somewhat fragile and break easily in future releases.
 * @param {Client} client
 * @param {number} expectedWorkers
 * @returns {Promise<boolean>} Returns true when workers were found.
 */
async function waitForWorkers(client, expectedWorkers) {
  helper.trace("Waiting for %d spark workers", expectedWorkers);

  const workerRE = /Alive Workers:.*(\d+)<\/li>/;
  let numWorkers = 0;
  const maxAttempts = 1000;
  const delay = 100;
  const findSparkMasterAsync = util.promisify(findSparkMaster);

  for (let attempts = 1; numWorkers < expectedWorkers && attempts <= maxAttempts; attempts++) {
    await promiseUtils.delay(delay);
    let master;
    try {
      master = await findSparkMasterAsync(client);
    } catch (err) {
      await promiseUtils.delay(delay);
      continue;
    }

    try {
      const body = await helper.makeWebRequest({ host: master, port: 7080, path: '/'});
      const match = body.match(workerRE);
      if (match) {
        numWorkers = parseFloat(match[1]);
        helper.trace("(%d/%d) Found workers: %d/%d", attempts, maxAttempts, numWorkers, expectedWorkers);
      } else {
        helper.trace("(%d/%d) Found no workers in body", attempts, maxAttempts);
      }
    } catch (err) {
      helper.trace("(%d/%d) Got error while fetching workers: %s", attempts, maxAttempts, err.message);
    }
  }

  if(numWorkers < expectedWorkers) {
    helper.trace('WARNING: After %d attempts only %d/%d workers were active.', maxAttempts, numWorkers, expectedWorkers);
    return false;
  }

  return true;
}

/**
 * Identifies the host that is the spark master (the one that is listening on port 7077)
 * and returns it.
 * @param {Client} client instance that contains host metadata.
 * @param {Function} callback invoked with the host that is the spark master or error.
 */
function findSparkMaster(client, callback) {
  client.execute('call DseClientTool.getAnalyticsGraphServer();', function(err, result) {
    if(err) {
      return callback(err);
    }
    const row = result.first();
    const host = row.result.ip;
    callback(null, host);
  });
}