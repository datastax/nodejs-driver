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

const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');
const errors = require('../../../lib/errors');

const { responseErrorCodes } = require('../../../lib/types');
const Client = require('../../../lib/client');
const { AllowListPolicy, DCAwareRoundRobinPolicy } = require('../../../lib/policies').loadBalancing;

const query = "select * from data";
const clusterSize = 3;
const buffer10Kb = utils.allocBuffer(10240);

describe('Client', function() {
  this.timeout(30000);
  describe('#execute(query, params, {host: h})', () => {

    const setupInfo = simulacron.setup([4], { initClient: false });
    const cluster = setupInfo.cluster;
    let client;

    before(() => {
      client = new Client({
        contactPoints: [simulacron.startingIp],
        localDataCenter: 'dc1',
        policies: {
          // define an LBP that includes all nodes except node 3
          loadBalancing: new AllowListPolicy(new DCAwareRoundRobinPolicy(), [
            cluster.node(0).address,
            cluster.node(1).address,
            cluster.node(2).address
          ])
        }
      });
      return client.connect();
    });
  
    after(() => client.shutdown());

    it('should send request to host used in options', (done) => {
      utils.times(10, (n, next) => {
        const nodeIndex = n % clusterSize;
        const node = cluster.node(nodeIndex);
        const host = client.hosts.get(node.address);
        client.execute(query, [], { host: host }, (err, result) => {
          assert.ifError(err);
          assert.strictEqual(result.info.queriedHost, node.address);
          assert.deepEqual(Object.keys(result.info.triedHosts), [node.address]);
          next();
        });
      }, done);
    });

    it('should send request to host used in options using async function', async () => {
      for (let i = 0; i < 10; i++) {
        const nodeIndex = i % clusterSize;
        const node = cluster.node(nodeIndex);
        const host = client.hosts.get(node.address);
        const result = await client.execute(query, [], { host: host });
        assert.strictEqual(result.info.queriedHost, node.address);
        assert.deepStrictEqual(Object.keys(result.info.triedHosts), [node.address]);
      }
    });

    it('should prepare and send execute requests', async () => {
      for (let i = 0; i < clusterSize; i++) {
        const result = await client.execute(query, [], { prepare: true });
        assert.ok(result.info.queriedHost);
      }
    });

    it('should throw an error if host used raises an error', (done) => {
      const node = cluster.node(0);
      const host = client.hosts.get(node.address);
      node.prime({
        when: {
          query: query
        },
        then: {
          result: 'unavailable',
          alive: 0,
          required: 1,
          consistency_level: 'LOCAL_ONE'
        }
      }, () => {
        client.execute(query, [], { host: host }, (err, result) => {
          assert.ok(err);
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.strictEqual(Object.keys(err.innerErrors).length, 1);
          const nodeError = err.innerErrors[node.address];
          assert.strictEqual(nodeError.code, responseErrorCodes.unavailableException);
          done();
        });
      });
    });
  
    it('should throw an error if host used in options is ignored by load balancing policy', () => {
      // since node 3 is not included in our LBP, the request should fail as we have no
      // connectivity to that node.
      const node = cluster.node(3);
      const host = client.hosts.get(node.address);
      let caughtErr = null;
      return client.execute(query, [], { host: host })
        .catch((err) => {
          caughtErr = err;
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          // no hosts should have been attempted.
          assert.strictEqual(Object.keys(err.innerErrors).length, 0);
        })
        .then(() => assert.ok(caughtErr));
    });
  });

  describe('#execute()', () => {

    const setupInfo = simulacron.setup([3], { initClient: false });
    const simulacronCluster = setupInfo.cluster;
    const maxRequestsPerConnection = 2048;
    let client;

    before(() => {
      client = new Client({
        contactPoints: [simulacron.startingIp],
        localDataCenter: 'dc1',
        pooling: { maxRequestsPerConnection }
      });

      return client.connect();
    });

    afterEach(() => simulacronCluster.resumeReadsAsync());

    after(() => client.shutdown());

    context('when connections are paused', () => {
      const query = 'INSERT INTO table1 (id) VALUES (?)';

      it('should not write more requests to the socket after the server paused reading', async () => {
        const writeQueues = client.hosts.values().map(h => h.pool.connections[0].writeQueue.queue);

        await simulacronCluster.pauseReadsAsync();

        // The TCP send and receive buffer size depends on the OS
        // We don't know how much data is needed to be flushed in order for them to signal as full
        // Send 20Mb+ to each node
        const pausedRequests = Array(maxRequestsPerConnection * client.hosts.length).fill(0)
          .map(() => client.execute(query, [ buffer10Kb ]));

        await waitForWriteQueueToStabilize(writeQueues);

        // Assert that there are still requests that haven't been written
        assert.isAbove(getTotalLength(writeQueues), 1);

        await simulacronCluster.resumeReadsAsync();
        await Promise.all(pausedRequests);
      });

      it('should continue routing traffic to non-paused nodes', async () => {
        const hostIndex = 2;
        const simulacronHost = simulacronCluster.node(hostIndex);
        const pauseHostAddress = simulacronHost.address;
        const nonPausedNodes = client.hosts.values().filter(h => h.address !== pauseHostAddress);
        assert.lengthOf(nonPausedNodes, 2);
        const writeQueues = client.hosts.values().map(h => h.pool.connections[0].writeQueue.queue);

        await simulacronHost.pauseReadsAsync();

        const initialRequests = Array(maxRequestsPerConnection * client.hosts.length).fill(0)
          .map(() => client.execute(query, [ buffer10Kb ]));

        // Non-paused nodes should process the requests correctly
        await Promise.all(nonPausedNodes.map(h =>
          helper.wait.until(() => client.getState().getInFlightQueries(h) === 0)));

        // There are still requests that haven't been written
        await waitForWriteQueueToStabilize(writeQueues);
        assert.isAbove(getTotalLength(writeQueues), 1);

        const buffer = utils.allocBuffer(1);
        // Non-paused nodes should continue processing requests
        await Promise.all(nonPausedNodes.map(host => client.execute(query, [ buffer ], { host })));

        await simulacronHost.resumeReadsAsync();
        await Promise.all(initialRequests);
      });
    });
  });
});

async function waitForWriteQueueToStabilize(writeQueues) {
  // Assert that the write queue doesn't process any more items
  let itemsInQueueLastValue = getTotalLength(writeQueues);
  await helper.delayAsync(200);

  // Wait for the amount of items in the write queue to stabilize
  await helper.wait.until(() => {
    const itemsInQueue = getTotalLength(writeQueues);
    if (itemsInQueue !== itemsInQueueLastValue) {
      itemsInQueueLastValue = itemsInQueue;
      return false;
    }
    return true;
  });
}

function getTotalLength(writeQueues) {
  return writeQueues.reduce((r, q) => r + q.length, 0);
}