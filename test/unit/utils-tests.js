"use strict";

var utils = require('../../lib/utils');

describe('utils', function () {
  describe('timesLimit()', function () {
    it('should handle sync and async functions', function (done) {
      utils.timesLimit(5, 10, function (i, next) {
        if (i == 0) {
          return setImmediate(next);
        }
        next();
      }, done);
    });
  });
});