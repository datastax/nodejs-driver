'use strict';
const assert = require('assert');

const tokenizer = require('../../lib/tokenizer');
var Murmur3Tokenizer = tokenizer.Murmur3Tokenizer;
var RandomTokenizer = tokenizer.RandomTokenizer;
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const MutableLong = require('../../lib/types/mutable-long');
const helper = require('../test-helper');

describe('Murmur3Tokenizer', function () {
  describe('#rotl64()', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      [
        ['12002002', 4, '192032032'],
        ['120020021112229', 27, '4806971846970835817'],
        ['44444441112229', 31, '256695637490275382'],
        ['44744441112828', 31, '-1134251256001325992']
      ].forEach(function (item) {
        var v = MutableLong.fromString(item[0]);
        t.rotl64(v, item[1]);
        assert.ok(v.equals(MutableLong.fromString(item[2])));
      });
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
        var input = MutableLong.fromNumber(item[0]);
        t.fmix(input);
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
        [[1, 254, 3, 4, 5, 6, 7, 248], 0x403fe01, 0xf8070605],
        [[1, 254, 3, 4, 5, 250, 7, 248], 0x403fe01, 0xf807fa05],
        [[100, 254, 3, 4, 5, 250, 7, 122], 0x403fe64, 0x7a07fa05],
        [[100, 254, 3, 4, 154, 250, 7, 122], 0x403fe64, 0x7a07fa9a]
      ].forEach(function (item) {
        var result = t.getBlock(item[0], 0, 0);
        assert.ok(result.equals(MutableLong.fromBits(item[1], item[2])));
      });
    });
  });
  describe('#hash()', function () {
    it('should hash the according results', function () {
      var t = new Murmur3Tokenizer();
      [
        [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16], '-5563837382979743776'],
        [[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], '-1513403162740402161'],
        [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], '-2824192546314762522'],
        [[0, 1, 2, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255], '6463632673159404390'],
        [[254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254], '-1672437813826982685'],
        [[254, 254, 254, 254], '4566408979886474012'],
        [[0, 0, 0, 0], '-3485513579396041028'],
        [[0, 1, 127, 127], '6573459401642635627'],
        [[226, 231, 226, 231, 226, 231, 1], '2222373981930033306']
      ].forEach(function (item) {
        var v = t.hash(item[0]);
        assert.ok(v.equals(MutableLong.fromString(item[1])));
      });
    });
  });
  describe('RandomTokenizer', function () {
    var t = new RandomTokenizer();
    describe('#hash', function () {
      it('should return expected results', function () {
        [
          [[1, 2, 3, 4], '11748876857495436398853550283091289647'],
          [[1, 2, 3, 4, 5, 6], '141904934057871337334287797400233978956'],
          [utils.allocBufferFromString('fffafa000102030405fe', 'hex'), '93979376327542758013347018124903879310'],
          [utils.allocBufferFromString('f000ee0000', 'hex'), '155172302213453714586395175393246848871']
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