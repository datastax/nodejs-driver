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
import util from "util";
import helper from "../../test-helper";
import Client from "../../../lib/client";
import utils from "../../../lib/utils";
import types from "../../../lib/types/index";
import { RoundRobinPolicy, AllowListPolicy, TokenAwarePolicy} from "../../../lib/policies/load-balancing";


const vdescribe = helper.vdescribe;

const maxInFlightRequests = 16;

/** Pre-calculated replicas based on the key and default partitioner */
const replicasByKey = {
  rf1: new Map([
    [0, [2]],
    [1, [2]],
    [2, [2]],
    [3, [1]],
    [4, [3]],
    [5, [2]],
    [6, [3]],
    [7, [3]],
    [8, [2]],
    [9, [1]],
    [10, [2]],
    [11, [2]],
    [12, [1]],
    [13, [2]],
    [14, [1]],
    [15, [3]],
  ]),
  rf2: new Map([
    [0, [2,3]],
    [1, [2,3]],
    [2, [2,3]],
    [3, [1,2]],
    [4, [3,1]],
    [5, [2,3]],
    [6, [3,1]],
    [7, [3,1]],
    [8, [2,3]],
    [9, [1,2]],
    [10, [2,3]],
    [11, [2,3]],
    [12, [1,2]],
    [13, [2,3]],
    [14, [1,2]],
    [15, [3,1]]
  ]),
  rf2Composite: new Map([
    ['a0', [3,1]],
    ['a1', [2,3]],
    [utils.stringRepeat('b', 10) + '0', [3,1]],
    [utils.stringRepeat('b', 10) + '1', [3,1]],
    [utils.stringRepeat('c', 20) + '0', [3,1]],
    [utils.stringRepeat('c', 20) + '1', [1,2]],
    [utils.stringRepeat('d', 300) + '0', [3,1]],
    [utils.stringRepeat('d', 300) + '1', [1,2]],
    ['a0b', [2,3]],
    ['a1b', [3,1]],
    ['a2b', [3,1]],
    ['a3b', [2,3]],
    ['a4b', [3,1]],
    ['a5b', [2,3]],
    ['a6b', [2,3]],
    ['a7b', [2,3]],
    ['a8b', [3,1]],
    ['a9b', [3,1]],
    ['a10b', [3,1]],
    ['a11b', [1,2]],
    ['a12b', [3,1]],
    ['a13b', [2,3]],
    ['a14b', [3,1]],
    ['a15b', [2,3]],
  ])
};

context('with a reusable 3 node cluster', function () {
  this.timeout(180000);
  // pass in 3:0 to exercise CCM dc set up logic which will use a consistent data center name
  // for both Apache Cassandra and DSE.
  helper.setup('3:0', {
    queries: [
      'CREATE KEYSPACE ks_simple_rp1 WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1}',
      'CREATE KEYSPACE ks_network_rp1 WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'dc1\' : 1}',
      'CREATE KEYSPACE ks_network_rp2 WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'dc1\' : 2}',
      'CREATE TABLE ks_simple_rp1.table_a (id int primary key, name int)',
      'CREATE TABLE ks_network_rp1.table_b (id int primary key, name int)',
      'CREATE TABLE ks_network_rp2.table_c (id int primary key, name int)',
      'CREATE TABLE ks_network_rp2.table_composite (id1 text, id2 text, primary key ((id1, id2)))',
      // Try to prevent consistency issues in the query trace
      'ALTER KEYSPACE system_traces WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'1\'}'
    ]
  });
  vdescribe('2.0', 'AllowListPolicy', function () {
    it('should use the hosts in the allow list only', function (done) {
      const policy = new AllowListPolicy(new RoundRobinPolicy(), ['127.0.0.1:9042', '127.0.0.2:9042']);
      const client = newInstance(policy);
      utils.timesLimit(100, maxInFlightRequests, function (n, next) {
        client.execute(helper.queries.basic, function (err, result) {
          assert.ifError(err);
          const lastOctet = helper.lastOctetOf(result.info.queriedHost);
          assert.ok(lastOctet === '1' || lastOctet === '2');
          next();
        });
      }, function (err) {
        assert.ifError(err);
        client.shutdown(done);
      });
    });
  });
  vdescribe('2.0', 'TokenAwarePolicy', function () {
    it('should target the correct replica for partition with logged keyspace', function (done) {
      utils.series([
        function testCaseWithSimpleStrategy(next) {
          testNoHops('ks_simple_rp1', 'table_a', 1, next);
        },
        function testCaseWithNetworkStrategy(next) {
          testNoHops('ks_network_rp1', 'table_b', 1, next);
        },
        function testCaseWithNetworkStrategyAndRp2(next) {
          testNoHops('ks_network_rp2', 'table_c', 2, next);
        }
      ], done);
    });
    it('should target the correct partition on a different keyspace', function (done) {
      testNoHops('ks_simple_rp1', 'ks_network_rp2.table_c', 2, done);
    });
    it('should target correct replica for composite routing key', function (done) {
      const client = new Client({
        policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
        keyspace: 'ks_network_rp2',
        contactPoints: helper.baseOptions.contactPoints
      });
      const query = 'INSERT INTO table_composite (id1, id2) VALUES (?, ?)';
      const queryOptions = { traceQuery: true, prepare: true, consistency: types.consistencies.all };
      utils.mapSeries([
        utils.stringRepeat('a', 1),
        utils.stringRepeat('b', 10),
        utils.stringRepeat('c', 20),
        utils.stringRepeat('d', 300),
      ], function eachValue(value, next) {
        utils.timesLimit(2, maxInFlightRequests, function eachTime(n, timesNext) {
          client.execute(query, [ value, n.toString() ], queryOptions, function (err, result) {
            assert.ifError(err);
            assertExpectedReplicas('rf2Composite', `${value}${n}`, result);
            timesNext();
          });
        }, next);
      }, helper.finish(client, done));
    });
    it('should balance between replicas with logged keyspace', function (done) {
      testAllReplicasAreUsedAsCoordinator('ks_network_rp2', 'table_c', 2, done);
    });
    it('should balance between replicas on a different keyspace', function (done) {
      testAllReplicasAreUsedAsCoordinator('ks_simple_rp1', 'ks_network_rp2.table_c', 2, done);
    });
    it('should target the correct replica using batches', function (done) {
      const rf = 2;
      const client = new Client({
        policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
        keyspace: 'ks_network_rp2',
        contactPoints: helper.baseOptions.contactPoints
      });
      const query = 'INSERT INTO table_c (id, name) VALUES (?, ?)';
      const queryOptions = { prepare: true, consistency: types.consistencies.all };

      utils.timesLimit(16, maxInFlightRequests, function (n, timesNext) {
        const params = [ n, n ];
        client.batch([{ query, params }], queryOptions, function (err, result) {
          assert.ifError(err);
          assertExpectedReplicas(rf, n, result);
          timesNext();
        });
      }, helper.finish(client, done));
    });
    it('should target the correct replica using routing indexes', function (done) {
      const client = new Client({
        policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
        keyspace: 'ks_network_rp2',
        contactPoints: helper.baseOptions.contactPoints
      });
      const query = 'INSERT INTO table_composite (id2, id1) VALUES (?, ?)';
      const queryOptions = { consistency: types.consistencies.localOne, routingIndexes: [ 1, 0] };
      utils.timesLimit(16, maxInFlightRequests, function (n, timesNext) {
        const params = [ 'a' + n, 'b' ];
        client.execute(query, params, queryOptions, function (err, result) {
          assert.ifError(err);
          assertExpectedReplicas('rf2Composite', `${params[0]}${params[1]}`, result);
          timesNext();
        });
      }, helper.finish(client, done));
    });
    it('should target the correct replica using user-provided Buffer routingKey', function (done) {
      // Use [0] which should map to node 1
      testWithQueryOptions((client) => ({
        routingKey: Buffer.from([0])
      }), '1', done);
    });
    it('should target the correct replica using user-provided Token routingKey', function (done) {
      testWithQueryOptions((client) => {
        // Find TokenRange for host 2 and use the end token (which is inclusive).
        const host = helper.findHost(client, 2);
        const ranges = client.metadata.getTokenRangesForHost('ks_network_rp1', host);
        const range = ranges.values().next().value;
        return { routingKey: range.end };
      }, '2', done);
    });
    it('should target the correct replica using user-provided TokenRange routingKey', function (done) {
      testWithQueryOptions((client) => {
        // Find TokenRange for host 3 and use it.
        const host = helper.findHost(client, 3);
        const ranges = client.metadata.getTokenRangesForHost('ks_network_rp1', host);
        const range = ranges.values().next().value;
        return { routingKey: range };
      }, '3', done);
    });
    it('should throw TypeError if invalid routingKey type is provided', function (done) {
      const client = new Client({
        policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
        keyspace: 'ks_network_rp1',
        contactPoints: helper.baseOptions.contactPoints
      });
      const query = 'select * from system.local';
      client.execute(query, [], { routingKey: 'this is not valid' }, err => {
        helper.assertInstanceOf(err, TypeError);
        client.shutdown(done);
      });
    });
  });
});

function testWithQueryOptions(optionsFn, expectedHostOctet, done) {
  const client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: 'ks_network_rp1',
    contactPoints: helper.baseOptions.contactPoints
  });
  client.connect(() => {
    const query = 'select * from system.local';
    const queryOptions = optionsFn(client);
    utils.timesLimit(100, maxInFlightRequests, function (n, timesNext) {
      client.execute(query, [], queryOptions, function (err, result) {
        assert.ifError(err);
        const hosts = Object.keys(result.info.triedHosts);
        assert.strictEqual(hosts.length, 1);
        assert.strictEqual(helper.lastOctetOf(hosts[0]), expectedHostOctet);
        timesNext();
      });
    }, helper.finish(client, done));
  });
}

/**
 * Check that the trace event sources only shows nodes that are replicas to fulfill a consistency ALL query.
 * @param {String} loggedKeyspace
 * @param {String} table
 * @param {Number} rf
 * @param {Function} done
 */
function testNoHops(loggedKeyspace, table, rf, done) {
  const client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });

  const query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  const queryOptions = { prepare: true, consistency: types.consistencies.localOne };

  utils.timesLimit(16, maxInFlightRequests, function (n, timesNext) {
    const params = [ n, n ];
    client.execute(query, params, queryOptions, function (err, result) {
      assert.ifError(err);
      assertExpectedReplicas(rf, n, result);
      timesNext();
    });
  }, helper.finish(client, done));
}

function assertExpectedReplicas(rf, key, result) {
  const name = typeof rf === 'number' ? `rf${rf}` : rf;
  const expected = replicasByKey[name].get(key);
  const queriedHostLastOctet = helper.lastOctetOf(result.info.queriedHost);
  assert.ok(
    expected.indexOf(parseInt(queriedHostLastOctet, 10)) >= 0,
    `Expected to be any of [${expected}] but was ${queriedHostLastOctet}`
  );
}

/**
 * Check that the coordinator of a given query and parameters are the same as the ones in the sources
 * @param {String} loggedKeyspace
 * @param {String} table
 * @param {Number} rf
 * @param {Function} done
 */
function testAllReplicasAreUsedAsCoordinator(loggedKeyspace, table, rf, done) {
  const client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });
  const query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  const queryOptions = { prepare: true, consistency: types.consistencies.localOne };
  utils.timesSeries(10, function (i, nextParameters) {
    const params = [ i, i ];
    const coordinators = new Set();
    utils.timesLimit(100, maxInFlightRequests, function (n, timesNext) {
      client.execute(query, params, queryOptions, function (err, result) {
        assert.ifError(err);
        coordinators.add(parseInt(helper.lastOctetOf(result.info.queriedHost), 10));
        timesNext();
      });
    }, function (err) {
      assert.ifError(err);
      assert.deepStrictEqual(Array.from(coordinators).sort(), replicasByKey[`rf${rf}`].get(i).sort());
      nextParameters();
    });
  }, function () {
    client.shutdown();
    done();
  });
}

function newInstance(policy) {
  const options = utils.deepExtend({}, helper.baseOptions, { policies: { loadBalancing: policy}});
  return new Client(options);
}
