/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const assert = require('assert');
const MonotonicTimestampGenerator = require('../../lib/policies/timestamp-generation').MonotonicTimestampGenerator;
const Long = require('../../lib/types').Long;
const helper = require('../test-helper');

describe('MonotonicTimestampGenerator', function () {
  describe('#next()', function () {
    it('should return a Number when the current date is before Jun 06 2255', function () {
      const g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        return 9007199254739;
      };
      const value = g.next();
      assert.strictEqual(typeof value, 'number');
    });
    it('should return a Long when the current date is after Jun 06 2255', function () {
      const g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        return 9007199254740;
      };
      const value = g.next();
      helper.assertInstanceOf(value, Long);
    });
    it('should log a warning once when it drifted into the future', function (done) {
      const g = new MonotonicTimestampGenerator(null, 50);
      let counter = 0;
      g.getDate = function () {
        if (counter++ === 0) {
          return 1000;
        }
        return 0;
      };
      const logs = [];
      const client = {
        log: function (level, message) {
          logs.push({ level: level, message: message });
        }
      };
      for (let i = 0; i < 200; i++) {
        const value = g.next(client);
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
      }, 100);
    });
    it('should use the current date', function () {
      const g = new MonotonicTimestampGenerator();
      const longThousand = Long.fromInt(1000);
      const startDate = Long.fromNumber(Date.now()).multiply(longThousand);
      const value = g.next();
      const endDate = Long.fromNumber(Date.now()).multiply(longThousand);
      const longValue = value instanceof Long ? value : Long.fromNumber(value);
      assert.ok(longValue.greaterThanOrEqual(startDate));
      assert.ok(longValue.lessThanOrEqual(endDate));
    });
    it('should increment the microseconds portion for the same date', function () {
      const g = new MonotonicTimestampGenerator();
      g.getDate = function () {
        // Use a fixed date
        return 1;
      };
      for (let i = 0; i < 1000; i++) {
        const value = g.next();
        assert.strictEqual(value, 1000 + i);
      }
      // Should drift into the future
      assert.strictEqual(g.next(), 2000);
    });
  });
});