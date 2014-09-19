var assert = require('assert');

var tokenizer = require('../../lib/tokenizer.js');
var Murmur3Tokenizer = tokenizer.Murmur3Tokenizer;
var RandomTokenizer = tokenizer.RandomTokenizer;
var types = require('../../lib/types.js');
var Long = types.Long;

describe('Murmur3Tokenizer', function () {
  describe('rotl64', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.rotl64(Long.fromString('12002002'), 4).toString(), '192032032');
      assert.strictEqual(t.rotl64(Long.fromString('120020021112229'), 27).toString(), '4806971846970835817');
      assert.strictEqual(t.rotl64(Long.fromString('44444441112229'), 31).toString(), '256695637490275382');
      assert.strictEqual(t.rotl64(Long.fromString('44744441112828'), 31).toString(), '-1134251256001325992');
    });
  });
  describe('fmix', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.fmix(Long.fromString('44744441112828')).toString(), '-7224089102552050611');
      assert.strictEqual(t.fmix(Long.fromString('9090')).toString(), '-7504869017411790576');
      assert.strictEqual(t.fmix(Long.fromString('90913738921')).toString(), '2458123773104054050');
      assert.strictEqual(t.fmix(Long.fromString('1')).toString(), '-5451962507482445012');
      assert.strictEqual(t.fmix(Long.fromString('-1')).toString(), '7256831767414464289');
    });
  });
  describe('getBlock', function () {
    it('should return expected results', function () {
      var t = new Murmur3Tokenizer();
      assert.strictEqual(t.getBlock([1, 2, 3, 4, 5, 6, 7, 8], 0, 0).toString(), '578437695752307201');
      assert.strictEqual(t.getBlock([1, -2, 3, 4, 5, 6, 7, -8], 0, 0).toString(), '-574483808854475263');
      assert.strictEqual(t.getBlock([1, -2, 3, 4, 5, -6, 7, -8], 0, 0).toString(), '-574215528017297919');
      assert.strictEqual(t.getBlock([100, -2, 3, 4, 5, -6, 7, 122], 0, 0).toString(), '8793271696913333860');
      assert.strictEqual(t.getBlock([100, -2, 3, 4, -102, -6, 7, 122], 0, 0).toString(), '8793272336863460964');
    });
  });
  describe('hash', function () {
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
  describe('RandomTokenizer', function () {
    describe('hash', function () {
      it('should return expected results', function () {
        var t = new RandomTokenizer();
        assert.strictEqual(t.hash([1, 2, 3, 4]).toString('hex'),                      '08d6c05a21512a79a1dfeb9d2a8f262f');
        assert.strictEqual(t.hash([1, 2, 3, 4, 5, 6]).toString('hex'),                '6ac1e56bc78f031059be7be854522c4c');
        assert.strictEqual(t.hash([254, 222, 1, 1, 1, 1, 1, 0, 129]).toString('hex'), 'ab0b00c618d3ebd330367bb238e0e27b');
      });
    });
  });
});