var assert = require('assert');
var SingleNodePolicy = require('../../lib/policies/single-node.js');
var async = require('async');

describe('Single Node Policy', function () {
  it('should yield the same node', function (done) {

    var policy = new SingleNodePolicy();
    var hosts = [];
    var originalHosts = ['A', 'B', 'C', 'E', 'F'];
    var singleHost = originalHosts[0];

    var assert_iterator = function (iterator) {
      for (var i = 0; i < originalHosts.length; i++) {
        var item = iterator.next();
        assert.strictEqual(item.value, singleHost);
      }
    };

    async.waterfall([

      // initialize
      policy.init.bind(policy, null, originalHosts),

      function (cb) {

        // generate 15 query plans
        var ops = [];
        for (var i = 0; i < 15; i++) {
          ops.push(policy.newQueryPlan.bind(policy, null, null));
        }

        async.parallel(ops, cb);
      }
    ], function (err, iterators) {

      assert.ifError(err, 'Query Plan should not return err');

      iterators.forEach(assert_iterator);
      done();

    });
  });
});