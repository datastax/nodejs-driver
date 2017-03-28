"use strict";

var assert = require('assert');
var format = require('util').format;
var Long = require('long');
var MutableLong = require('../../lib/types/mutable-long');

describe('MutableLong', function () {
  describe('fromNumber()', function () {
    it('should convert from a Number', function () {
      var values = [ 1, 2, -1, -999999, 256, 1024 * 1024, 1024 * 1025 + 13, Math.pow(2, 52), -1 * Math.pow(2, 52) ];
      values.forEach(function (value) {
        var ml = MutableLong.fromNumber(value);
        var long = ml.toImmutable();
        assert.strictEqual(long.toNumber(), value);
      });
    });
  });
  describe('#multiply()', function () {
    it('should return the product for values < 2^53', function () {
      [
        // a * b
        [ 1, 0 ],
        [ 2, 2],
        [ 1, 4],
        [ 13 * 163 ],
        [ 22631153906384 * 199 ],
        [ -1, Math.pow(2, 43) ]
      ].forEach(function (item) {
        var expected = item[0] * item[1];
        var a = MutableLong.fromNumber(item[0]);
        var b = MutableLong.fromNumber(item[1]);
        assert.ok(a.multiply(b).equals(MutableLong.fromNumber(expected)), format('failed for value %d*%d',
          item[0], item[1]));
      });
    });
    it('should return the product for int64 values', function () {
      [
        // [a, b, c]
        [ new MutableLong(0x4f23, 0xff32, 0x4220, 0x0d32), new MutableLong(0x07),
          new MutableLong(0x29f5, 0xfa60, 0xcee6, 0x5c5f) ],
        [ new MutableLong(0x2EBF, 0x4611, 0x9867, 0xFFFF), new MutableLong(0x030),
          new MutableLong(0xC3D0, 0x2338, 0x935D, 0xFFEC)]
      ].forEach(function (item) {
        assert.ok(item[0].multiply(item[1]).equals(item[2]));
      });
    });
  });
});