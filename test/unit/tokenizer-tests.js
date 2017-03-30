'use strict';
var assert = require('assert');

var tokenizer = require('../../lib/tokenizer');
var Murmur3Tokenizer = tokenizer.Murmur3Tokenizer;
var RandomTokenizer = tokenizer.RandomTokenizer;
var types = require('../../lib/types');
var MutableLong = require('../../lib/types/mutable-long');
var Long = types.Long;
var helper = require('../test-helper');

describe('Murmur3Tokenizer', function () {
  describe('#rotl64()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.rotl64(Long.fromString('12002002'), 4).toString(), '192032032');
      assert.strictEqual(t.rotl64(Long.fromString('120020021112229'), 27).toString(), '4806971846970835817');
      assert.strictEqual(t.rotl64(Long.fromString('44444441112229'), 31).toString(), '256695637490275382');
      assert.strictEqual(t.rotl64(Long.fromString('44744441112828'), 31).toString(), '-1134251256001325992');
    });
  });
  describe('#fmix()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      [
        [44744441112828, 0x709d544d, 0x9bbee13c],
        [9090, 0x2355d910, 0x97d95964],
        [90913738921, 0x45cf5f22, 0x221d028c],
        [1, 0x34c2cb2c, 0xb456bcfc],
        [-1, 0x4b825f21, 0x64b5720b]
      ].forEach(function (item) {
        var input = Long.fromNumber(item[0]);
        var expected = Long.fromBits(item[1], item[2], false);
        assert.strictEqual(t.fmix(input).toString(), expected.toString());
      });
    });
  });
  describe('#fmix2()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      [
        [44744441112828, 0x709d544d, 0x9bbee13c],
        [9090, 0x2355d910, 0x97d95964],
        [90913738921, 0x45cf5f22, 0x221d028c],
        [1, 0x34c2cb2c, 0xb456bcfc],
        [-1, 0x4b825f21, 0x64b5720b]
      ].forEach(function (item) {
        var input = MutableLong.fromNumber(item[0]);
        t.fmix2(input);
        assert.strictEqual(input.getLowBitsUnsigned(), item[1]);
        assert.strictEqual(input.getHighBitsUnsigned(), item[2]);
      });
    });
  });
  describe('#getBlock()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      [
        [[1, 2, 3, 4, 5, 6, 7, 8], 0x4030201, 0x8070605],
        [[1, -2, 3, 4, 5, 6, 7, -8], 0x403fe01, 0xf8070605],
        [[1, -2, 3, 4, 5, -6, 7, -8], 0x403fe01, 0xf807fa05],
        [[100, -2, 3, 4, 5, -6, 7, 122], 0x403fe64, 0x7a07fa05],
        [[100, -2, 3, 4, -102, -6, 7, 122], 0x403fe64, 0x7a07fa9a]
      ].forEach(function (item) {
        var data = item[0].map(function (x) {
          return Long.fromNumber(x);
        });
        var result = t.getBlock(data, 0, 0);
        assert.strictEqual(result.toString(), Long.fromBits(item[1], item[2], false).toString());
      });
    });
  });
  describe('#getBlock2()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      [
        [[1, 2, 3, 4, 5, 6, 7, 8], 0x4030201, 0x8070605],
        [[1, 254, 3, 4, 5, 6, 7, 248], 0x403fe01, 0xf8070605],
        [[1, 254, 3, 4, 5, 250, 7, 248], 0x403fe01, 0xf807fa05],
        [[100, 254, 3, 4, 5, 250, 7, 122], 0x403fe64, 0x7a07fa05],
        [[100, 254, 3, 4, 154, 250, 7, 122], 0x403fe64, 0x7a07fa9a]
      ].forEach(function (item) {
        var result = t.getBlock2(item[0], 0, 0);
        assert.ok(result.equals(MutableLong.fromBits(item[1], item[2])));
      });
    });
  });
  describe('#hash()', function () {
    it('should hash the according results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.hash([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]).toString(), '-5563837382979743776');
      assert.strictEqual(t.hash([2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]).toString(), '-1513403162740402161');
      assert.strictEqual(t.hash([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]).toString(), '-2824192546314762522');
      assert.strictEqual(t.hash([0, 1, 2, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]).toString(), '6463632673159404390');
      assert.strictEqual(t.hash([254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254]).toString(), '-1672437813826982685');
      assert.strictEqual(t.hash([254, 254, 254, 254]).toString(), '4566408979886474012');
      assert.strictEqual(t.hash([0, 0, 0, 0]).toString(), '-3485513579396041028');
      assert.strictEqual(t.hash([0, 1, 127, 127]).toString(), '6573459401642635627');
      assert.strictEqual(t.hash([226, 231, 226, 231, 226, 231, 1]).toString(), '2222373981930033306');
    });
  });
  describe('#hash2()', function () {
    it('should hash the according results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.hash2([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]).toString(), '-5563837382979743776');
      assert.strictEqual(t.hash2([2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]).toString(), '-1513403162740402161');
      assert.strictEqual(t.hash2([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]).toString(), '-2824192546314762522');
      assert.strictEqual(t.hash2([0, 1, 2, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]).toString(), '6463632673159404390');
      assert.strictEqual(t.hash2([254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254]).toString(), '-1672437813826982685');
      assert.strictEqual(t.hash2([254, 254, 254, 254]).toString(), '4566408979886474012');
      assert.strictEqual(t.hash2([0, 0, 0, 0]).toString(), '-3485513579396041028');
      assert.strictEqual(t.hash2([0, 1, 127, 127]).toString(), '6573459401642635627');
      assert.strictEqual(t.hash2([226, 231, 226, 231, 226, 231, 1]).toString(), '2222373981930033306');
    });
  });
  describe('RandomTokenizer', function () {
    var t = new RandomTokenizer();
    describe('#hash', function () {
      it('should return expected results', function () {
        [
          [[1, 2, 3, 4], '11748876857495436398853550283091289647'],
          [[1, 2, 3, 4, 5, 6], '141904934057871337334287797400233978956'],
          [new Buffer('fffafa000102030405fe', 'hex'), '93979376327542758013347018124903879310'],
          [new Buffer('f000ee0000', 'hex'), '155172302213453714586395175393246848871']
        ].forEach(function (item) {
          assert.strictEqual(t.hash(item[0]).toString(), item[1]);
        });
      });
    });
    describe('#parse()', function () {
      it('should return the Integer representation', function () {
        var val = t.parse('141904934057871337334287797400233978956');
        helper.assertInstanceOf(val, types.Integer);
        assert.ok(val.equals(types.Integer.fromString('141904934057871337334287797400233978956')));
      });
    });
  });
});