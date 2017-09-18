'use strict';
var assert = require('assert');
var responseErrorCodes = require('../../../lib/types').responseErrorCodes;
var simulacron = require('../simulacron');
var helper = require('../../test-helper');
var utils = require('../../../lib/utils');

var Client = require('../../../lib/client.js');
var ConstantSpeculativeExecutionPolicy = require('../../../lib/policies/speculative-execution').ConstantSpeculativeExecutionPolicy;
var NoSpeculativeExecutionPolicy = require('../../../lib/policies/speculative-execution').NoSpeculativeExecutionPolicy;
var OrderedLoadBalancingPolicy = require('../../test-helper').OrderedLoadBalancingPolicy;

var query = "select * from data";
var delay = 100;

describe('Client', function() {

  this.timeout(20000);
  var setupInfo = simulacron.setup([3], { initClient: false });
  var cluster = setupInfo.cluster;
  var assertQueryCount = function (nodeCounts, cb) {
    utils.times(nodeCounts.length, function(index, next) {
      var node = cluster.node(index);
      var expectedCount = nodeCounts[index];
      node.getLogs(function(err, queries) {
        assert.ifError(err);
        var matches = queries.filter(function (el) {
          return el.query === query;
        });
        assert.strictEqual(matches.length, expectedCount, "For node " + node.id);
        next();
      });
    }, cb);
  };

  var node0, node1, node2;
  before(function done() {
    node0 = cluster.node(0);
    node1 = cluster.node(1);
    node2 = cluster.node(2);
  });

  [
    {title: "with default speculativeExecution", policy: null},
    {title: 'with NoSpeculativeExecutionPolicy', policy: new NoSpeculativeExecutionPolicy()}
  ].forEach(function (data) {
    describe(data.title, function () {
      var clientOptions = {
        contactPoints: [simulacron.startingIp],
        policies: { 
          loadBalancing: new OrderedLoadBalancingPolicy()
        }
      };

      if(data.policy !== null) {
        clientOptions.policies.speculativeExecution = data.policy;
      }

      var client = new Client(clientOptions);
      before(client.connect.bind(client));
      after(client.shutdown.bind(client));

      it('should not start speculative execution', function (done) {
        utils.series([
          primeWithDelay(node0, delay * 3),
          function executeQuery(next) {
            client.execute(query, [], { isIdempotent: true }, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.info.speculativeExecutions, 0, "Should not have been any speculative executions");
              assert.strictEqual(Object.keys(result.info.triedHosts).length, 1);

              // Should have got response from 0th node.
              var queriedHost = cluster.node(result.info.queriedHost);
              assert.strictEqual(queriedHost, node0);

              // Should have only sent request to 0th node.
              assertQueryCount([1,0,0], next);
            });
          }
        ], done);
      });
    });
  });
  describe('with ConstantSpeculativeExecutionPolicy', function () {
    var clientOptions = {
      contactPoints: [simulacron.startingIp],
      policies: { 
        speculativeExecution: new ConstantSpeculativeExecutionPolicy(delay, 3), 
        loadBalancing: new OrderedLoadBalancingPolicy()
      }
    };

    var client = new Client(clientOptions);
    before(client.connect.bind(client));
    after(client.shutdown.bind(client));

    it('should not start speculative executions if query is non-idempotent', function (done) {
      utils.series([
        primeWithDelay(node0, delay * 3),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: false }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 0, "Should not have been any speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 1);
            
            // Should have queried 0th node.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node0);
           
            // Should have only sent request to node 0.
            assertQueryCount([1,0,0], next);
          });
        }
      ], done);
    });
    it ('should complete from first speculative execution when faster', function (done) {
      utils.series([
        primeWithDelay(node0, delay * 3),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 2);

            // Should have got response from 1st node.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node1);

            // Should have sent requests to node 0 and 1.
            assertQueryCount([1,1,0], next);
          });
        }
      ], done);
    });
    it ('should complete from initial execution when speculative is started but is slower', function (done) {
      utils.series([
        primeWithDelay(cluster, delay * 4),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 0th node, the initial query.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node0);

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        }
      ], done);
    });
    it ('should complete from second speculative execution when faster', function (done) {
      utils.series([
        primeWithDelay(node0, delay * 3),
        primeWithDelay(node1, delay * 3),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, the second speculative execution.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node2);

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        }
      ], done);
    });
    it ('should retry within initial execution', function (done) {
      utils.series([
        primeWithIsBootstrapping(node0),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 0, "Should have been no speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 2);

            // Should have got response from 1st node, the retry from the first error.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node1);

            // Expect isBootstrapping error on host that failed.
            var failureHost = node0.address;
            var code = result.info.triedHosts[failureHost].code;
            assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");

            // Should have sent requests to two nodes.
            assertQueryCount([1,1,0], next);
          });
        }
      ], done);
    });
    it ('should retry within speculative execution', function (done) {
      utils.series([
        primeWithDelay(node0, delay * 3),
        primeWithIsBootstrapping(node1),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, the retry after the speculative execution.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node2);

            // Expect isBootstrapping error on host that failed.
            var failureHost = node1.address;
            var code = result.info.triedHosts[failureHost].code;
            assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        }
      ], done);
    });
    it ('should wait for last execution to complete', function (done) {
      utils.series([
        primeWithDelay(node0, delay * 3),
        primeWithIsBootstrapping(node1),
        function primeNode2(next) {
          // prime node 2 to respond with bootstrapping error (no more hosts to retry, so should wait for node 0)
          node2.prime({
            when: {
              query: query
            },
            then : {
              result: "is_bootstrapping"
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 0th node.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node0);

            // Expect isBootstrapping error on both hosts that failed.
            [1, 2].forEach(function(id) {
              var failureHost = cluster.node(id).address;
              var code = result.info.triedHosts[failureHost].code;
              assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");
            });

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        }
      ], done);
    });
    it ('should fail if all executions fail and reach end of query plan', function (done) {
      utils.series([
        function primeNodes(next) {
          // prime each node with a shrinking delay
          // node 0: 3*delay
          // node 1: 2*delay
          // node 2: 1*delay
          utils.times(3, function(id, nextT) {
            cluster.node(id).prime({
              when: {
                query: query
              },
              then : {
                result: "is_bootstrapping",
                delay_in_ms: (3 - id) * delay
              }
            }, nextT);
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ok(err);
            assert.strictEqual(Object.keys(err.innerErrors).length, 3);

            // Expect isBootstrapping error on both hosts that failed.
            [0, 1, 2].forEach(function(id) {
              var failureHost = cluster.node(id).address;
              var code = err.innerErrors[failureHost].code;
              assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");
            });

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        }
      ], done);
    });
    it('should allow zero delay', function (done) {
      var clientOptions = {
        contactPoints: cluster.getContactPoints(0),
        policies: { 
          speculativeExecution: new ConstantSpeculativeExecutionPolicy(0, 3),
        }
      };
      var client = new Client(clientOptions);

      utils.series([
        client.connect.bind(client),
        primeWithDelay(node0, delay * 4),
        primeWithDelay(node1, delay * 4),
        primeWithDelay(node2, delay * 2),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, since it had the lowest delay.
            var queriedHost = cluster.node(result.info.queriedHost);
            assert.strictEqual(queriedHost, node2);

            // Should have sent requests to all nodes.
            assertQueryCount([1,1,1], next);
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});

/**
 * @param {SimulacronTopic} topic Cluster, DC, or Node to prime.
 * @param {Number} delay How long to wait before sending response.
 * @returns a task that creates a prime for query with the given delay.
 */
function primeWithDelay(topic, delay) {
  return helper.toTask(topic.prime, topic, {
    when: {
      query: query
    },
    then : {
      result: "success",
      delay_in_ms: delay
    }
  });
}

/**
 * @param {SimulacronTopic} topic Cluster, DC, or Node to prime.
 * @returns A task that creates a prime for query that triggers a 'is_bootstraping' error repsonse.
 */
function primeWithIsBootstrapping(topic) {
  return helper.toTask(topic.prime, topic, {
    when: {
      query: query
    },
    then : {
      result: "is_bootstrapping"
    }
  });
}