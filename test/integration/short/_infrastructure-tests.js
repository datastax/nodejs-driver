var assert = require('assert');
var async = require('async');
var helper = require('../../test-helper.js');

describe('Test infrastructure', function () {
  this.timeout(120000);
  it('should be able to run ccm', function (done) {
    var ccm = new helper.Ccm();
    ccm.exec(['list'], function (err, info) {
      done(err);
    });
  });
  it('should be able to create and destroy a cluster', function (done) {
    async.timesSeries(2, function (n, next) {
      var ccm = new helper.Ccm();
      ccm.startAll(2, null, function (err) {
        assert.equal(err, null);
        ccm.remove(next);
      });
    }, done);
  });
});