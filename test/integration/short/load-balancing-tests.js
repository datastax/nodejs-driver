/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var loadBalancing = require('../../../lib/policies/load-balancing');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var TokenAwarePolicy = loadBalancing.TokenAwarePolicy;
var WhiteListPolicy = loadBalancing.WhiteListPolicy;
var vdescribe = helper.vdescribe;

context('with a reusable 3 node cluster', function () {
  this.timeout(180000);
  helper.setup(3, {
    queries: [
      'CREATE KEYSPACE ks_simple_rp1 WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1}',
      'CREATE KEYSPACE ks_network_rp1 WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter1\' : 1}',
      'CREATE KEYSPACE ks_network_rp2 WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter1\' : 2}',
      'CREATE TABLE ks_simple_rp1.table_a (id int primary key, name int)',
      'CREATE TABLE ks_network_rp1.table_b (id int primary key, name int)',
      'CREATE TABLE ks_network_rp2.table_c (id int primary key, name int)',
      // Try to prevent consistency issues in the query trace
      'ALTER KEYSPACE system_traces WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'1\'}'
    ]
  });
  vdescribe('2.0', 'WhiteListPolicy', function () {
    it('should use the hosts in the white list only', function (done) {
      var policy = new WhiteListPolicy(new RoundRobinPolicy(), ['127.0.0.1:9042', '127.0.0.2:9042']);
      var client = newInstance(policy);
      utils.timesLimit(100, 20, function (n, next) {
        client.execute(helper.queries.basic, function (err, result) {
          assert.ifError(err);
          var lastOctet = helper.lastOctetOf(result.info.queriedHost);
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
    it('should target the correct partition with logged keyspace', function (done) {
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
  var client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });
  var query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  var queryOptions = { traceQuery: true, prepare: true, consistency: types.consistencies.all };
  var results = [];
  utils.timesLimit(50, 16, function (n, timesNext) {
    var params = [ n, n ];
    client.execute(query, params, queryOptions, function (err, result) {
      assert.ifError(err);
      getTrace(client, result.info.traceId, function (err, trace) {
        assert.ifError(err);
        results.push(trace);
        timesNext();
      });
    });
  }, function loopEnd() {
    client.shutdown();
    // Check where the events are coming from
    results.forEach(function (trace) {
      var replicas = getReplicas(trace);
      assert.strictEqual(Object.keys(replicas).length, expectedReplicas);
    });
    done();
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
  var client = new Client({
    policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy()) },
    keyspace: loggedKeyspace,
    contactPoints: helper.baseOptions.contactPoints
  });
  var query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
  var queryOptions = { traceQuery: true, prepare: true, consistency: types.consistencies.all };
  var results = [];
  utils.timesSeries(10, function (i, nextParameters) {
    var params = [ i, i ];
    var coordinators = {};
    var replicas;
    utils.times(10, function (n, timesNext) {
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
  var attempts = 0;
  var trace;
  var error;
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
  var replicas = {};
  var regex = /\b(?:from|to) \/([\da-f:.]+)$/i;
  trace.events.forEach(function (event) {
    replicas[helper.lastOctetOf(event['source'].toString())] = true;
    var activityMatches = regex.exec(event['activity']);
    if (activityMatches && activityMatches.length === 2) {
      replicas[helper.lastOctetOf(activityMatches[1])] = true;
    }
  });
  return replicas;
}

function newInstance(policy) {
  var options = utils.deepExtend({}, helper.baseOptions, { policies: { loadBalancing: policy}});
  return new Client(options);
}
