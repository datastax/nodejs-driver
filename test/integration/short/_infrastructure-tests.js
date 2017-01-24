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
  this.timeout(180000);
  it('should be able to run ccm', function (done) {
    helper.ccm.exec(['list'], done);
  });
  it('should be able to create and destroy a DSE cluster', function (done) {
    utils.timesSeries(2, function (n, next) {
      helper.ccm.startAll(2, null, function (err) {
        assert.ifError(err);
        helper.ccm.remove(next);
      });
    }, done);
  });
});