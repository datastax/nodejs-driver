"use strict";

var assert = require('assert');
var format = require('util').format;
var Long = require('long');
var MutableLong = require('../../lib/types/mutable-long');

describe('MutableLong', function () {
  describe('fromNumber() and #toNumber()', function () {
    it('should convert from and to a Number', function () {
      var values = [ 1, 2, -1, -999999, 256, 1024 * 1024, 1024 * 1025 + 13, Math.pow(2, 52), -1 * Math.pow(2, 52) ];
      values.forEach(function (value) {
        var ml = MutableLong.fromNumber(value);
        assert.strictEqual(ml.toNumber(), value);
      });
    });
  });
  describe('fromString()', function () {
    it('should parse from string representation for decimal numbers', function () {
      [
        [ '-1', [ 0xffff, 0xffff, 0xffff, 0xffff ]],
        [ '0', []],
        [ '255', [ 0xff ]],
        [ '4294901760', [ 0, 0xffff ]],
        [ '9223372036854775807', [ 0xffff, 0xffff, 0xffff, 0x7fff ]],
        [ '8354511557626137073', [ 0x89f1, 0x6033, 0x2fff, 0x73f1 ]],
        [ '-6989252372825142799', [ 0x89f1, 0x6033, 0x2fff, 0x9f01 ]],
        [ '-8142173877431989775', [ 0x89f1, 0x6033, 0x2fff, 0x8f01 ]],
      ].forEach(function (item) {
        var expected = new MutableLong(item[1][0], item[1][1], item[1][2], item[1][3]);
        assert.ok(MutableLong.fromString(item[0]).equals(expected));
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
          new MutableLong(0xC3D0, 0x2338, 0x935D, 0xFFEC)],
        [ MutableLong.fromBits(0xe084bca4, 0x28b1), MutableLong.fromBits(0xed558ccd, 0xff51afd7),
          MutableLong.fromBits(0xd7e8bf54, 0x9e9ed5ab)]
      ].forEach(function (item) {
        var a = Long.fromBits(item[0].getLowBitsUnsigned(), item[0].getHighBitsUnsigned(), false);
        var b = Long.fromBits(item[1].getLowBitsUnsigned(), item[1].getHighBitsUnsigned(), false);
        var c = Long.fromBits(item[2].getLowBitsUnsigned(), item[2].getHighBitsUnsigned(), false);
        assert.ok(a.multiply(b).equals(c));
        assert.ok(item[0].multiply(item[1]).equals(item[2]));
      });
    });
  });
  describe('#shiftRightUnsigned()', function () {
    it('should shift across int16 blocks', function () {
      for (var i = 1; i < 64; i++) {
        var l = MutableLong.fromBits(0xffffffff, 0xffffffff).shiftRightUnsigned(i);

        var expectedHigh = 0xffffffff;
        var expectedLow = 0xffffffff;
        if (i < 32) {
          expectedLow = (expectedLow >>> i) | (expectedHigh << (32 - i));
          expectedHigh = expectedHigh >>> i;
        }
        else {
          expectedLow = expectedHigh >>> (i - 32);
          expectedHigh = 0;
        }
        expectedLow = expectedLow >>> 0;
        assert.strictEqual(l.getLowBitsUnsigned().toString(16), expectedLow.toString(16));
        assert.strictEqual(l.getHighBitsUnsigned().toString(16), expectedHigh.toString(16));
      }
    });
  });
  describe('#shiftLeft()', function () {
    it('should shift across int16 blocks', function () {
      for (var i = 1; i < 64; i++) {
        var l = MutableLong.fromBits(1, 0).shiftLeft(i);
        var expectedHigh = 0;
        var expectedLow = 0;
        if (i < 32) {
          expectedLow = 1 << i;
        }
        else {
          expectedHigh = 1 << (i - 32);
        }
        expectedLow = expectedLow >>> 0;
        expectedHigh = expectedHigh >>> 0;
        assert.strictEqual(l.getLowBitsUnsigned().toString(16), expectedLow.toString(16));
        assert.strictEqual(l.getHighBitsUnsigned().toString(16), expectedHigh.toString(16));
      }
    });
  });
  describe('#compare()', function () {
    it('should compare the values provided', function () {
      [
        [ new MutableLong(1), new MutableLong() ],
        [ new MutableLong(1, 2, 3, 4), new MutableLong(0, 1, 2, 3) ],
        [ new MutableLong(1, 2, 2, 2), new MutableLong(0, 0, 2, 2) ],
        [ new MutableLong(0xffff, 0xffff, 0xffff, 0xffff), new MutableLong(0xfffe, 0xffff, 0xffff, 0xffff) ],
        [ new MutableLong(), new MutableLong(0xfffe, 0xffff, 0xffff, 0xffff) ],
      ].forEach(function (item) {
        assert.strictEqual(item[0].compare(item[1]), 1);
        assert.strictEqual(item[1].compare(item[0]), -1);
        assert.strictEqual(item[0].compare(item[0]), 0);
        assert.strictEqual(item[1].compare(item[1]), 0);
      });
    });
    it('should compare random numbers', function () {
      var max = Math.pow(2, 52);
      var length = 100;
      var i;
      var arr1 = new Array(length);
      var arr2 = new Array(length);
      for (i = 0; i < length; i++) {
        var n = 0;
        if (i !== 0) {
          n = Math.floor(Math.random() * max);
          if (i%2 === 1) {
            n = -n;
          }
        }
        arr1[i] = n;
        arr2[i] = MutableLong.fromNumber(n);
      }
      arr1.sort(function compare(a, b) {
        return a - b;
      });
      arr2.sort(function (a, b) {
        return a.compare(b);
      });
      for (i = 0; i < length; i++) {
        var expected = MutableLong.fromNumber(arr1[i]);
        assert.ok(arr2[i].equals(expected));
      }
    });
  });
});