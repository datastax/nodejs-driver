"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils');
const errors = require('../../../lib/errors');
const reconnection = require('../../../lib/policies/reconnection');

describe('reconnection', function () {
  this.timeout(120000);
  describe('when a node is back UP', function () {
    it('should re-prepare on demand', function (done) {
      var dummyClient = new Client(helper.baseOptions);
      const keyspace = helper.getRandomName('ks');
      const table = helper.getRandomName('tbl');
      var insertQuery1 = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
      var insertQuery2 = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table);
      const client = new Client(utils.extend({
        keyspace: keyspace,
        contactPoints: helper.baseOptions.contactPoints,
        policies: { reconnection: new reconnection.ConstantReconnectionPolicy(100)}
      }));
      utils.series([
        function removeCcm(next) {
          helper.ccmHelper.remove(function () {
            //Ignore error
            next();
          });
        },
        helper.ccmHelper.start(2),
        function createKs(next) {
          dummyClient.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(dummyClient, next));
        },
        function createTable(next) {
          dummyClient.execute(helper.createTableCql(keyspace + '.' + table), helper.waitSchema(dummyClient, next));
        },
        //Connect using an active keyspace
        client.connect.bind(client),
        function insert1(next) {
          //execute a couple of times to ensure it is prepared on all hosts
          utils.times(15, function (n, timesNext) {
            client.execute(insertQuery1, [types.uuid(), n.toString()], {prepare: 1}, timesNext);
          }, next);
        },
        function killNode(next) {
          helper.ccmHelper.exec(['node1', 'stop', '--not-gently'], next);
        },
        function insert2(next) {
          //Prepare and execute a new Query, that it is NOT prepared on the stopped node :)
          //execute a couple of times to ensure it is prepared on the remaining hosts
          utils.times(3, function (n, timesNext) {
            client.execute(insertQuery2, [types.uuid(), n], {prepare: 1}, timesNext);
          }, next);
        },
        function restart(next) {
          helper.ccmHelper.exec(['node1', 'start'], next);
        },
        function killTheOther(next) {
          helper.ccmHelper.exec(['node2', 'stop'], next);
        },
        function insert1_plus (next) {
          utils.times(15, function (n, timesNext) {
            client.execute(insertQuery1, [types.uuid(), n.toString()], {prepare: 1}, timesNext);
          }, next);
        },
        function insert2_plus (next) {
          utils.times(1, function (n, timesNext) {
            client.execute(insertQuery2, [types.uuid(), n], {prepare: 1}, timesNext);
          }, next);
        }
      ], done);
    });
    it('should reconnect and re-prepare once there is an available host', function (done) {
      var dummyClient = new Client(helper.baseOptions);
      const keyspace = helper.getRandomName('ks');
      const table = helper.getRandomName('tbl');
      var insertQuery1 = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
      const client = new Client({
        keyspace: keyspace,
        contactPoints: helper.baseOptions.contactPoints,
        policies: { reconnection: new reconnection.ConstantReconnectionPolicy(100)}
      });
      utils.series([
        function removeCcm(next) {
          helper.ccmHelper.remove(function () {
            //Ignore error
            next();
          });
        },
        helper.ccmHelper.start(1),
        function createKs(next) {
          dummyClient.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(dummyClient, next));
        },
        function createTable(next) {
          dummyClient.execute(helper.createTableCql(keyspace + '.' + table), helper.waitSchema(dummyClient, next));
        },
        //Connect using an active keyspace
        client.connect.bind(client),
        function insert1(next) {
          //execute a couple of times to ensure it is prepared on all hosts
          utils.times(15, function (n, timesNext) {
            client.execute(insertQuery1, [types.uuid(), n.toString()], {prepare: 1}, timesNext);
          }, next);
        },
        function killNode(next) {
          helper.ccmHelper.exec(['node1', 'stop'], next);
        },
        function insert2(next) {
          //execute a couple of times to even though the cluster is DOWN
          utils.timesSeries(20, function (n, timesNext) {
            client.execute(insertQuery1, [types.uuid(), n.toString()], {prepare: 1}, function (err) {
              //It should callback in error
              helper.assertInstanceOf(err, errors.NoHostAvailableError);
              setTimeout(timesNext, 100);
            });
          }, next);
        },
        function restart(next) {
          helper.ccmHelper.exec(['node1', 'start'], helper.wait(5000, next));
        },
        function insert1_plus(next) {
          //The cluster should be UP!
          client.execute(util.format('SELECT * from %s', table), function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 15);
            utils.times(25, function (n, timesNext) {
              client.execute(insertQuery1, [types.uuid(), n.toString()], {prepare: 1}, timesNext);
            }, next);
          });
        },
        function assertResults(next) {
          client.execute(util.format('SELECT * from %s', table), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            //It should have inserted 15+25
            assert.strictEqual(result.rows.length, 40);
            next();
          });
        }
      ], done);
    });
    it('should re-prepare and execute batches of prepared queries', function (done) {
      var dummyClient = new Client(helper.baseOptions);
      const keyspace = helper.getRandomName('ks');
      const table = helper.getRandomName('tbl');
      var insertQuery1 = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
      var insertQuery2 = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table);
      const queriedHosts = {};
      const client = new Client(utils.extend({
        keyspace: keyspace,
        contactPoints: helper.baseOptions.contactPoints,
        policies: { reconnection: new reconnection.ConstantReconnectionPolicy(100)},
        //disable heartbeat
        pooling: { heartBeatInterval: 0}
      }));
      utils.series([
        function removeCcm(next) {
          helper.ccmHelper.remove(function () {
            //Ignore error
            next();
          });
        },
        helper.ccmHelper.start(3),
        helper.toTask(dummyClient.execute, dummyClient, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(dummyClient.execute, dummyClient, helper.createTableCql(keyspace + '.' + table)),
        //Connect using an active keyspace
        client.connect.bind(client),
        //Kill node1
        helper.toTask(helper.ccmHelper.exec, null, ['node2', 'stop']),
        function (next) {
          setTimeout(next, 5000);
        },
        function insert1(next) {
          utils.times(10, function (n, timesNext) {
            var queries = [
              { query: insertQuery1, params: [types.Uuid.random(), n.toString()]},
              { query: insertQuery2, params: [types.Uuid.random(), n]}
            ];
            client.batch(queries, {prepare: true}, timesNext);
          }, next);
        },
        //restart node1
        helper.toTask(helper.ccmHelper.exec, null, ['node2', 'start']),
        function insertAfterRestart(next) {
          utils.times(15, function (n, timesNext) {
            var queries = [
              { query: insertQuery1, params: [types.Uuid.random(), n.toString()]},
              { query: insertQuery2, params: [types.Uuid.random(), n]}
            ];
            client.batch(queries, {prepare: true}, function (err, result) {
              assert.ifError(err);
              queriedHosts[result.info.queriedHost] = true;
              timesNext();
            });
          }, next);
        }
      ], done);
    });
  });
  describe('when connections are silently dropped', function () {
    it('should callback in err the next request', function (done) {
      //never reconnect
      const client = new Client(utils.extend({}, helper.baseOptions, {policies: {reconnection: new reconnection.ConstantReconnectionPolicy(Number.MAX_VALUE)}}));
      utils.series([
        helper.ccmHelper.start(1),
        client.connect.bind(client),
        function doSomeQueries(next) {
          utils.times(30, function (n, timesNext) {
            client.execute(helper.queries.basic, timesNext);
          }, next);
        },
        function silentlyKillConnections(next) {
          killConnections(client, true);
          setTimeout(next, 2000);
        },
        function issueARequest(next) {
          client.execute('SELECT * FROM system.local', function (err) {
            //The error is expected
            assert.ok(err);
            assert.ok(err.message.indexOf('undefined') < 0);
            assert.ok(Object.keys(err.innerErrors).length > 0);
            next();
          });
        }
      ], function (err) {
        assert.ifError(err);
        helper.ccmHelper.removeIfAny(done);
      });
    });
  });
  describe('when a node is killed during connection initialization', function() {
    it('should properly abort the connection and retry', function(done) {
      const client = newInstance();
      utils.series([
        helper.ccmHelper.start(2),
        function pauseNode2(next) {
          // Pause node2 so establishing connection to it hangs.
          helper.ccmHelper.exec(['node2', 'pause'], next);
        },
        client.connect.bind(client),
        function doSomeQueries(next) {
          // Issue some queries to get a connection attempt on node2.
          utils.times(30, function (n, timesNext) {
            client.execute(helper.queries.basic, function() {
              timesNext();
            });
          });
          // Since queries won't actually complete, wait 5 seconds and then kill node.
          setTimeout(next, 5000);
        },
        function killNode2(next) {
          // Kill node2 non-gently so OS sends a TCP RST on the connection.
          helper.ccmHelper.exec(['node2', 'stop', '--not-gently'], next);
        },
        function startNode2(next) {
          // If we've made it this far without process dying we are in good
          // shape, but validate node2 comes up anyways.
          helper.ccmHelper.startNode(2, next);
        },
        function wait10(next) {
          // Give 10 seconds for node to be marked up.
          setTimeout(next, 10000);
        },
        function isUp(next) {
          // Ensure all hosts are up.
          var hosts = client.hosts.values();
          assert.strictEqual(hosts.length, 2);
          hosts.forEach(function(host) {
            assert.ok(host.isUp());
          });
          next();
        }
      ], function (err) {
        assert.ifError(err);
        helper.ccmHelper.removeIfAny(done);
      });
    });
  });
});

/**
 * @param {Client} client
 * @param {Boolean} destroy
 */
function killConnections(client, destroy) {
  client.hosts.forEach(function (h) {
    h.pool.connections.forEach(function (c) {
      if (destroy) {
        c.netClient.destroy();
      }
      else {
        c.netClient.end();
      }
    });
  });
}

/**
 * @returns {Client}
 */
function newInstance(options) {
  options = options || {};
  options = utils.deepExtend(options, helper.baseOptions);
  return new Client(options);
}