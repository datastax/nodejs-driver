/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var rewire = require('rewire');
var helper = require('../../test-helper');
var moduleName = '../../../lib/geometry/point';
var Point = require(moduleName);

describe('Point', function () {
  describe('constructor', function () {
    it('should validate x and y', function () {
      assert.doesNotThrow(function () {
        // eslint-disable-next-line
        new Point(1, 2.312);
      });
      assert.throws(function () {
        // eslint-disable-next-line
        new Point('1', '2.3');
      }, TypeError);
      assert.throws(function () {
        // eslint-disable-next-line
        new Point(1);
      }, TypeError);
    });
    it('should set #x and #y properties', function () {
      assert.strictEqual(new Point(1, 2).x, 1);
      assert.strictEqual(new Point(1.2, 2.2).y, 2.2);
      assert.strictEqual(new Point(-1, 2).x, -1);
      assert.strictEqual(new Point(1, 0).y, 0);
    });
  });
  describe('fromBuffer()', function () {
    it('should create an instance from WKB', function () {
      [
        ['000000000140000000000000004010000000000000', 2, 4],
        ['0000000001400199999999999a4010cccccccccccd', 2.2, 4.2],
        ['000000000100000000000000000000000000000000', 0, 0],
        ['0000000001400aaaaa8b5964f6c025cccccccccccd', 3.3333331, -10.9]
      ]
        .forEach(function (item) {
          var p = Point.fromBuffer(new Buffer(item[0], 'hex'));
          assert.strictEqual(p.x, item[1]);
          assert.strictEqual(p.y, item[2]);
        });
    });
  });
  describe('#toBuffer()', function () {
    it('should return WKB in a big-endian OS', function () {
      var BEPoint = rewire(moduleName);
      [
        [2, 4, '000000000140000000000000004010000000000000'],
        [2.2, 4.2, '0000000001400199999999999a4010cccccccccccd'],
        [0, 0, '000000000100000000000000000000000000000000'],
        [3.3333331, -10.9, '0000000001400aaaaa8b5964f6c025cccccccccccd']
      ]
        .forEach(function (item) {
          var p = new BEPoint(item[0], item[1]);
          p.useBESerialization = function () {
            return true;
          };
          var buffer = p.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[2]);
        });
    });
    it('should return WKB in a little-endian OS', function () {
      var LEPoint = rewire(moduleName);
      [
        [2, 4, '010100000000000000000000400000000000001040'],
        [2.2, 4.2, '01010000009a99999999990140cdcccccccccc1040'],
        [0, 0, '010100000000000000000000000000000000000000'],
        [3.3333331, -10.9, '0101000000f664598baaaa0a40cdcccccccccc25c0']
      ]
        .forEach(function (item) {
          var p = new LEPoint(item[0], item[1]);
          p.useBESerialization = function () {
            return false;
          };
          var buffer = p.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[2]);
        });
    });
  });
  describe('#toString()', function () {
    it('should return WKT of the object', function () {
      [
        [2, 4, 'POINT (2 4)'],
        [2.2, 4.2, 'POINT (2.2 4.2)'],
        [0, 0, 'POINT (0 0)'],
        [3.3333331, -10.9, 'POINT (3.3333331 -10.9)']
      ]
        .forEach(function (item) {
          var p = new Point(item[0], item[1]);
          assert.strictEqual(p.toString(), item[2]);
        });
    });
  });
  describe('#fromString()', function () {
    it('should parse WKT representation', function () {
      [
        ['POINT(10 20)', 10, 20],
        ['POINT(10.1 20)', 10.1, 20],
        ['POINT (1.2234 .3)', 1.2234, 0.3],
        ['POINT(10 -20.5)', 10, -20.5],
        ['POINT(-10 -20)', -10, -20],
        ['POINT (-10 -20)', -10, -20]
      ].forEach(function (item) {
        var p = Point.fromString(item[0]);
        helper.assertInstanceOf(p, Point);
        assert.strictEqual(p.x, item[1]);
        assert.strictEqual(p.y, item[2]);
      });
    });
    it('should throw TypeError when WKT representation is invalid', function () {
      [
        'POINT  (10 20)',
        'POINT (zz 20)',
        'POINT (1,2234 13)',
        'POINT (10 20 30)',
        'POINT (10 20 30 40)'
      ].forEach(function (item) {
        assert.throws(function () {
          Point.fromString(item);
        }, TypeError);
      });
    });
  });
});