var assert = require('assert');
var async = require('async');
var helper = require('../helper');

describe('Test infrastructure', function () {
  this.timeout(180000);
  it('should be able to run ccm', function (done) {
    helper.ccm.exec(['list'], done);
  });
  it('should be able to create and destroy a DSE cluster', function (done) {
    async.timesSeries(2, function (n, next) {
      helper.ccm.startAll(2, null, function (err) {
        assert.ifError(err);
        helper.ccm.remove(next);
      });
    }, done);
  });
});