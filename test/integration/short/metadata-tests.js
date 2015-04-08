"use strict";
var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');

describe('Metadata', function () {
  this.timeout(60000);
  before(helper.ccmHelper.start(2, {vnodes: true}));
  after(helper.ccmHelper.remove);
  it('should keep keyspace information up to date', function (done) {
    var client = newInstance();
    client.connect(function (err) {
      assert.ifError(err);
      var m = client.metadata;
      assert.ok(m);
      assert.ok(m.keyspaces);
      assert.ok(m.keyspaces['system']);
      assert.ok(m.keyspaces['system'].strategy);
      async.series([
        helper.toTask(client.execute, client, "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}"),
        helper.toTask(client.execute, client, "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}"),
        helper.toTask(client.execute, client, "CREATE KEYSPACE ks3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}"),
        helper.toTask(client.execute, client, "CREATE KEYSPACE ks4 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : 1}")
      ], function (err) {
        function checkKeyspace(name, strategy, optionName, optionValue) {
          var ks = m.keyspaces[name];
          assert.ok(ks);
          assert.strictEqual(ks.strategy, strategy);
          assert.ok(ks.strategyOptions);
          assert.strictEqual(ks.strategyOptions[optionName], optionValue);
        }
        assert.ifError(err);
        assert.ok(Object.keys(m.keyspaces).length > 4);
        checkKeyspace('ks1', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '3');
        checkKeyspace('ks2', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '2');
        checkKeyspace('ks3', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '1');
        checkKeyspace('ks4', 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1', '1');
        done();
      });
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
