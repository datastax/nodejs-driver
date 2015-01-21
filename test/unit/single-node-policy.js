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



//         function (err, iterator) {

//           assert.ifError(err, 'Query Plan should not return err');

//           for (var i = 0; i < originalHosts.length; i++) {
//             var item = iterator.next();
//             assert.strictEqual(item.done, false);
//             hosts.push(item.value);
//           }
//           next();
//         });

//       // run newQueryPlan multiple times
//       function (cb) {

//         async.times(times, function (n, next) {

//             policy.newQueryPlan(null, null, function (err, iterator) {

//               assert.ifError(err, 'Query Plan should not return err');

//               for (var i = 0; i < originalHosts.length; i++) {
//                 var item = iterator.next();
//                 assert.strictEqual(item.done, false);
//                 hosts.push(item.value);
//               }
//               next();
//             });
//           }

//         ],
//         function () {
//           assert.ifError(err, 'Query Plan should not return err');

//           done();
//         });

//       policy.init(null, originalHosts, function () {

//         async.times(times, function (n, next) {

//           policy.newQueryPlan(null, null, function (err, iterator) {

//             assert.ifError(err, 'Query Plan should not return err');

//             for (var i = 0; i < originalHosts.length; i++) {
//               var item = iterator.next();
//               assert.strictEqual(item.done, false);
//               hosts.push(item.value);
//             }
//             next();
//           });
//         }, function (err) {
//           assert.equal(err, null);
//           assert.strictEqual(hosts.length, times * originalHosts.length);
//           //Count the number of times of each element
//           originalHosts.forEach(function (item) {
//             var length = 0;
//             var lastHost = null;
//             hosts.forEach(function (host) {
//               length += (host === item ? 1 : 0);
//               assert.notEqual(lastHost, host);
//               lastHost = host;
//             });
//             assert.strictEqual(length, times);
//           });
//           done();
//         });
//       });


//     });
// });