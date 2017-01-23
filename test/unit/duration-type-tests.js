'use strict';
var assert = require('assert');
var util = require('util');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var Duration = types.Duration;
var Long = types.Long;

var values = [
  // [ Duration representation, hex value, standard format, ISO 8601, ISO 8601 Week
  [ new Duration(0, 0, Long.fromNumber(1)), '000002', '1ns'],
  [ new Duration(0, 0, Long.fromNumber(128)), '00008100', '128ns'],
  [ new Duration(0, 0, Long.fromNumber(2)), '000004', '2ns'],
  [ new Duration(0, 0, Long.fromNumber(256)), '00008200', '256ns'],
  [ new Duration(0, 0, Long.fromNumber(33001)), '0000c101d2', '33us1ns'],
  [ new Duration(0, 0, Long.fromNumber(-1)), '000001', '-1ns'],
  [ new Duration(0, 0, Long.fromNumber(-33001)), '0000c101d1', '-33µs1ns'],
  [ new Duration(0, 0, Long.fromNumber(0)), '000000'],
  [ new Duration(0, 0, Long.fromNumber(2251799813685279)), '0000fe1000000000003e' ],
  [ new Duration(-1, -1, Long.fromNumber(-1)), '010101', '-1mo1d1ns'],
  [ new Duration(2, 15, Long.ZERO), '041e00', '2mo15d', 'P2M15D'],
  [ new Duration(0, 14, Long.ZERO), '001c00', '14d', 'P14D', 'P2W'],
  [ new Duration(1, 1, Long.fromNumber(1)), '020202', '1mo1d1ns'],
  [ new Duration(257, 0, Long.fromNumber(0)), '82020000', '21y5mo', 'P21Y5M'],
  [ new Duration(0, 2, Long.fromNumber(120000000000)), '0004f837e11d6000', '2d2m', 'P2DT2M']
];

describe('Duration', function () {
  describe('#toBuffer()', function () {
    it('should represent the values in vint encoding', function () {
      values.forEach(function (item) {
        assert.strictEqual(item[0].toBuffer().toString('hex'), item[1]);
      });
    });
  });
  describe('fromBuffer()', function () {
    it('should decode the vint values', function () {
      values.forEach(function (item) {
        assertDurationEqual(Duration.fromBuffer(utils.allocBufferFromString(item[1], 'hex')), item[0], item[1]);
      });
    });
  });
  describe('fromString()', function () {
    [
      ['should parse standard format', 2],
      ['should parse ISO 8601 format', 3],
      ['should parse ISO 8601 week format', 4]
    ].forEach(function (testInfo) {
      it(testInfo[0], function () {
        var index = testInfo[1];
        values.forEach(function (item) {
          if (!item[index]) {
            return;
          }
          assertDurationEqual(Duration.fromString(item[index]), item[0], item[index]);
        });
      });
    });
    it('should throw TypeError for invalid strings', function() {
      [
        '-PP',
        '-P15',
        'P17-13-50T22:00:00',
        'P7-13-50T22:00:00',
        'PW',
        '-PW',
        '-PTW',
      ].forEach(function (testInfo) {
        assert.throws(function() { Duration.fromString(testInfo); }, 'Unable to convert \'' + testInfo + '\' to a duration');
      });
    });
  });
  describe('#toString()', function () {
    it('should return the standard representation', function () {
      values.forEach(function (item) {
        if (!item[2] || item[2].indexOf('µs') >= 0) {
          // not standard
          return;
        }
        assert.strictEqual(item[0].toString(), item[2]);
      });
    });
  });
});

/**
 * @param {Duration} actual
 * @param {Duration} expected
 * @param {String} input
 */
function assertDurationEqual(actual, expected, input) {
  assert.ok(actual.equals(expected), util.format('%j not equals to %j, from %s', actual, expected, input));
}
