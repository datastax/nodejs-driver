var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
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
  });
});