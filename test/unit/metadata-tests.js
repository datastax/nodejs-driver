var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
var Host = require('../../lib/host.js').Host;
var Metadata = require('../../lib/metadata.js');
var tokenizer = require('../../lib/tokenizer.js');
var types = require('../../lib/types.js');
var utils = require('../../lib/utils.js');

describe('Metadata', function () {
  describe('#getReplicas()', function () {
    it('should return depending on the rf and ring size with simple strategy', function () {
      var metadata = new Metadata();
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.ring = [0, 1, 2, 3, 4, 5];
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'SimpleStrategy',
        'strategy_options': {'replication_factor': 3}
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //Primary replica plus the 2 next tokens
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '0');
      assert.strictEqual(replicas[1], '1');
      assert.strictEqual(replicas[2], '2');

      replicas = metadata.getReplicas('dummy', new Buffer([5]));
      assert.ok(replicas);
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '5');
      assert.strictEqual(replicas[1], '0');
      assert.strictEqual(replicas[2], '1');
    });
    it('should return depending on the dc rf with network topology', function () {
      var options = utils.extend({}, helper.baseOptions);
      var metadata = new Metadata();
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.datacenters = {'dc1': 4, 'dc2': 4};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7];
      //load even primary replicas
      metadata.primaryReplicas = {};
      for (var i = 0; i < metadata.ring.length; i ++) {
        var h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        metadata.primaryReplicas[i.toString()] = h;
      }
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': {'dc1': 3, 'dc2': 1}
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //3 replicas from dc1 and 1 replica from dc2
      assert.strictEqual(replicas.length, 4);
      assert.strictEqual(replicas[0].address, '0');
      assert.strictEqual(replicas[1].address, '1');
      assert.strictEqual(replicas[2].address, '2');
      assert.strictEqual(replicas[3].address, '4');
    });
  });
});