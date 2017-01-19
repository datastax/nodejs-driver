/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var helper = require('../../test-helper.js');
var utils = require('../../../lib/utils');

describe('Test infrastructure', function () {
  this.timeout(120000);
  it('should be able to run ccm', function (done) {
    var ccm = new helper.Ccm();
    ccm.exec(['list'], function (err) {
      done(err);
    });
  });
  it('should be able to create and destroy a cluster', function (done) {
    utils.timesSeries(2, function (n, next) {
      var ccm = new helper.Ccm();
      ccm.startAll(2, null, function (err) {
        assert.equal(err, null);
        ccm.remove(next);
      });
    }, done);
  });
});