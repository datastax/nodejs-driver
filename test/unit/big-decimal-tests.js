'use strict';

const assert = require('assert');
const types = require('../../lib/types');
const utils = require('../../lib/utils');

describe('BigDecimal', function () {
  const BigDecimal = types.BigDecimal;
  const Integer = types.Integer;
  var hexToDecimalValues = [
    ['000000050100', '0.00256'],
    ['0000000304', '0.004'],
    ['0000000103', '0.3'],
    ['0000000303', '0.003'],
    ['0000000001', '1'],
    ['0000000203', '0.03'],
    ['0000000003', '3'],
    ['00000002ffffffff', '-0.01', true],
    ['00000002ff', '-0.01'],
    ['00000001ff', '-0.1'],
    ['00000003ffffff', '-0.001', true],
    ['00000000ff', '-1'],
    ['00000001ff01', '-25.5'],
    ['00000000ff01', '-255'],
    ['00000005d81be4cdb941364e91c67eeb', '-123456789012345678901234.56789'],
    ['0000000527e41b3246bec9b16e398115', '123456789012345678901234.56789'],
    ['000000090785ee10d5da46d900f4369fffffffff', '9999999999999999999999999999.999999999']
  ];
  var intAndScaleToString = [
    ['12345', 1, '1234.5'],
    ['12345', 2, '123.45'],
    ['12345', 4, '1.2345'],
    ['12345', 5, '0.12345'],
    ['12345', 6, '0.012345'],
    ['1', 6, '0.000001'],
    ['-1', 6, '-0.000001'],
    ['-1', 0, '-1'],
    ['-256', 3, '-0.256'],
    ['34', 0, '34'],
    ['34', 10, '0.0000000034']
  ];
  describe('constructor', function () {
    it('should allow Number and Integer unscaled values', function () {
      intAndScaleToString.forEach(function (item) {
        var value1 = new BigDecimal(parseInt(item[0], 10), item[1]);
        var value2 = new BigDecimal(Integer.fromString(item[0]), item[1]);
        assert.ok(value1.equals(value2));
      });
    });
  });
  describe('fromString()', function () {
    it('should convert from string in decimal representation', function () {
      intAndScaleToString.forEach(function (item) {
        var value = new BigDecimal(Integer.fromString(item[0]), item[1]);
        assert.ok(value.equals(BigDecimal.fromString(item[2])), 'BigDecimals not equal for value ' + item[2]);
      });
    });
    it('should throw a TypeError when value is not valid', function () {
      assert.throws(function () {
        BigDecimal.fromString('');
      }, TypeError);
      assert.throws(function () {
        BigDecimal.fromString(null);
      }, TypeError);
    });
  });
  describe('fromBuffer()', function () {
    it('should convert from buffer (scale + unscaledValue in BE)', function () {
      hexToDecimalValues.forEach(function (item) {
        const buffer = utils.allocBufferFromString(item[0], 'hex');
        var value = BigDecimal.fromBuffer(buffer);
        assert.strictEqual(value.toString(), item[1]);
      });
    });
  });
  describe('toBuffer()', function () {
    it('should convert to buffer (scale + unscaledValue in BE)', function () {
      hexToDecimalValues.forEach(function (item) {
        var avoidConvert = item[2];
        if (avoidConvert) {
          return;
        }
        var value = BigDecimal.toBuffer(BigDecimal.fromString(item[1]));
        assert.strictEqual(value.toString('hex'), item[0]);
      });
    });
  });
  describe('fromNumber()', function () {
    it('should convert from string in decimal representation', function () {
      intAndScaleToString.forEach(function (item) {
        var value = BigDecimal.fromNumber(Number(item[2]));
        assert.strictEqual(value.toNumber(), parseFloat(item[2]));
      });
    });
  });
  describe('#toString()', function () {
    it('should convert to string decimal representation', function () {
      intAndScaleToString.forEach(function (item) {
        var value = new BigDecimal(Integer.fromString(item[0]), item[1]);
        assert.strictEqual(value.toString(), item[2]);
      });
    });
  });
  describe('#subtract()', function () {
    it('should substract the values with any scale', function () {
      [
        ['1234.5', '12.345', '1222.155'],
        ['12345', '100.00001', '12244.99999'],
        ['100.00001', '2', '98.00001'],
        ['100.01001', '2.01', '98.00001'],
        ['100', '102', '-2'],
        ['102', '100', '2'],
        ['102.1', '100.05', '2.05'],
        ['10201.00', '10201', '0.00']
      ].forEach(function (item) {
        var first = BigDecimal.fromString(item[0]);
        var second = BigDecimal.fromString(item[1]);
        assert.strictEqual(first.subtract(second).toString(), item[2]);
        // check mutations
        assert.strictEqual(first.toString(), item[0]);
      });
    });
  });
  describe('#add()', function () {
    it('should substract the values with any scale', function () {
      [
        ['1234.5', '12.345', '1246.845'],
        ['12345', '100.00001', '12445.00001'],
        ['100.00001', '2', '102.00001'],
        ['100.01001', '2.01', '102.02001'],
        ['100', '-102', '-2'],
        ['102', '100', '202'],
        ['102.1', '100.05', '202.15'],
        ['10201.00', '-10201', '0.00']
      ].forEach(function (item) {
        var first = BigDecimal.fromString(item[0]);
        var second = BigDecimal.fromString(item[1]);
        assert.strictEqual(first.add(second).toString(), item[2]);
        // check mutations
        assert.strictEqual(first.toString(), item[0]);
      });
    });
  });
  describe('#compare()', function () {
    it('should compare values with different scales', function () {
      [
        ['1234.5', '12.345', 1],
        ['12345', '100.00001', 1],
        ['100.00001', '2', 1],
        ['100.01001', '2.01', 1],
        ['100', '102', -1],
        ['102', '100', 1],
        ['102.1', '100.05', 1],
        ['10201.00', '10201', 0],
        ['-1.010', '-1.01', 0],
        ['-1.01', '-1.01', 0]
      ].forEach(function (item) {
        var first = BigDecimal.fromString(item[0]);
        var second = BigDecimal.fromString(item[1]);
        assert.strictEqual(first.compare(second), item[2]);
        // check mutations
        assert.strictEqual(first.toString(), item[0]);
      });
    });
  });
});