"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const Client = require('../../../lib/client');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const loadBalancing = require('../../../lib/policies/load-balancing');
const RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
const TokenAwarePolicy = loadBalancing.TokenAwarePolicy;
const WhiteListPolicy = loadBalancing.WhiteListPolicy;
const vdescribe = helper.vdescribe;

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
  vdescribe('2.0', 'WhiteListPolicy', function () {
    it('should use the hosts in the white list only', function (done) {
      const policy = new WhiteListPolicy(new RoundRobinPolicy(), ['127.0.0.1:9042', '127.0.0.2:9042']);
      const client = newInstance(policy);
      utils.timesLimit(100, 20, function (n, next) {
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
        utils.stringRepeat('b', 0x3fff),
        utils.stringRepeat('c', 0x7fff),
        utils.stringRepeat('d', 0xffe0),
      ], function eachValue(value, next) {
        utils.timesLimit(32, 16, function eachTime(n, timesNext) {
          client.execute(query, [ value, n.toString() ], queryOptions, function (err, result) {
            assert.ifError(err);
            assertReplicas(result, client, 2, timesNext);
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
  });
});

/**
 * Check that the trace event sources only shows nodes that are replicas to fulfill a consistency ALL query.
 * @param {String} loggedKeyspace
 * @param {String} table
 * @param {Number} expectedReplicas
 * @param {Function} done
 */
function testNoHops(loggedKeyspace, table, expectedReplicas, done) {
  const client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });
  const query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  const queryOptions = { traceQuery: true, prepare: true, consistency: types.consistencies.all };
  utils.timesLimit(50, 16, function (n, timesNext) {
    const params = [ n, n ];
    client.execute(query, params, queryOptions, function (err, result) {
      assert.ifError(err);
      assertReplicas(result, client, expectedReplicas, timesNext);
    });
  }, helper.finish(client, done));
}

function assertReplicas(result, client, expectedReplicas, next) {
  getTrace(client, result.info.traceId, function (err, trace) {
    assert.ifError(err);
    // Check where the events are coming from
    const replicas = getReplicas(trace);
    // Verify that only replicas were hit (coordinator + replica)
    assert.strictEqual(Object.keys(replicas).length, expectedReplicas);
    next();
  });
}

/**
 * Check that the coordinator of a given query and parameters are the same as the ones in the sources
 * @param {String} loggedKeyspace
 * @param {String} table
 * @param {Number} expectedReplicas
 * @param {Function} done
 */
function testAllReplicasAreUsedAsCoordinator(loggedKeyspace, table, expectedReplicas, done) {
  const client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });
  const query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  const queryOptions = { traceQuery: true, prepare: true, consistency: types.consistencies.all };
  const results = [];
  utils.timesSeries(10, function (i, nextParameters) {
    const params = [ i, i ];
    const coordinators = {};
    let replicas;
    utils.timesLimit(100, 20, function (n, timesNext) {
      client.execute(query, params, queryOptions, function (err, result) {
        assert.ifError(err);
        coordinators[helper.lastOctetOf(result.info.queriedHost)] = true;
        if (n !== 0) {
          // Only check the trace once
          return timesNext();
        }
        getTrace(client, result.info.traceId, function (err, trace) {
          assert.ifError(err);
          replicas = getReplicas(trace);
          timesNext();
        });
      });
    }, function (err) {
      assert.ifError(err);
      results.push({
        replicas: replicas,
        coordinators: coordinators
      });
      nextParameters();
    });
  }, function () {
    client.shutdown();
    results.forEach(function (item) {
      assert.strictEqual(Object.keys(item.replicas).length, expectedReplicas);
      assert.deepEqual(Object.keys(item.replicas).sort(), Object.keys(item.coordinators).sort(),
        'All replicas should be used as coordinators');
    });
    done();
  });
}

/**
 * Gets the query trace retrying additional times if it fails.
 */
function getTrace(client, traceId, callback) {
  let attempts = 0;
  let trace;
  let error;
  if (!traceId) {
    throw new Error('traceid was not provided');
  }
  // Retry several times
  utils.whilst(
    function condition() {
      return !trace && attempts++ < 20;
    },
    function fn(next) {
      client.metadata.getTrace(traceId, function (err, t) {
        error = err;
        trace = t;
        next();
      });
    },
    function whilstEnd() {
      callback(error, trace);
    }
  );
}

function getReplicas(trace) {
  const replicas = {};
  const regex = /\b(?:from|to) \/([\da-f:.]+)$/i;
  trace.events.forEach(function (event) {
    replicas[helper.lastOctetOf(event['source'].toString())] = true;
    const activityMatches = regex.exec(event['activity']);
    if (activityMatches && activityMatches.length === 2) {
      replicas[helper.lastOctetOf(activityMatches[1])] = true;
    }
  });
  return replicas;
}

function newInstance(policy) {
  const options = utils.deepExtend({}, helper.baseOptions, { policies: { loadBalancing: policy}});
  return new Client(options);
}
