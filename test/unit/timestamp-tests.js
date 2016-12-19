'use strict';

var assert = require('assert');
var MonotonicTimestampGenerator = require('../../lib/policies/timestamp-generation').MonotonicTimestampGenerator;
var Long = require('../../lib/types').Long;
var helper = require('../test-helper');

describe('MonotonicTimestampGenerator', function () {
  describe('#next()', function () {
    it('should return a Number when the current date is before Jun 06 2255', function () {
      var g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        return 9007199254739;
      };
      var value = g.next();
      assert.strictEqual(typeof value, 'number');
    });
    it('should return a Long when the current date is after Jun 06 2255', function () {
      var g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        return 9007199254740;
      };
      var value = g.next();
      helper.assertInstanceOf(value, Long);
    });
    it('should log a warning once when it drifted into the future', function (done) {
      var g = new MonotonicTimestampGenerator(null, 50);
      var counter = 0;
      g.getDate = function () {
        if (counter++ === 0) {
          return 1000;
        }
        return 0;
      };
      var logs = [];
      var client = {
        log: function (level, message) {
          logs.push({ level: level, message: message });
        }
      };
      for (var i = 0; i < 200; i++) {
        var value = g.next(client);
        assert.strictEqual(value, 1000000 + i);
      }
      assert.strictEqual(logs.length, 1);
      assert.strictEqual(logs[0].level, 'warning');
      assert.strictEqual(logs[0].message.indexOf('Timestamp generated'), 0);
      setTimeout(function () {
        // A second warning should be issued
        assert.strictEqual(g.next(client), 1000200);
        assert.strictEqual(logs.length, 2);
        assert.strictEqual(logs[1].level, 'warning');
        done();
      }, 100)
    });
    it('should use the current date', function () {
      var g = new MonotonicTimestampGenerator();
      var longThousand = Long.fromInt(1000);
      var startDate = Long.fromNumber(Date.now()).multiply(longThousand);
      var value = g.next();
      var endDate = Long.fromNumber(Date.now()).multiply(longThousand);
      var longValue = value instanceof Long ? value : Long.fromNumber(value);
      assert.ok(longValue.greaterThanOrEqual(startDate));
      assert.ok(longValue.lessThanOrEqual(endDate));
    });
    it('should increment the microseconds portion for the same date', function () {
      var g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        // Use a fixed date
        return 1;
      };
      for (var i = 0; i < 1000; i++) {
        var value = g.next();
        assert.strictEqual(value, 1000 + i);
      }
      // Should drift into the future
      assert.strictEqual(g.next(), 2000);
    });
  });
});