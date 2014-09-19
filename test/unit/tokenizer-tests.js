var assert = require('assert');

var tokenizer = require('../../lib/tokenizer.js');
var Murmur3Tokenizer = tokenizer.Murmur3Tokenizer;
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
//      assert.strictEqual(t.fmix(Long.fromString('9090')).toString(), '-7504869017411790576');
//      assert.strictEqual(t.fmix(Long.fromString('90913738921')).toString(), '2458123773104054050');
//      assert.strictEqual(t.fmix(Long.fromString('1')).toString(), '-5451962507482445012');
//      assert.strictEqual(t.fmix(Long.fromString('-1')).toString(), '7256831767414464289');
    });
  });
});