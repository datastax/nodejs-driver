'use strict';
var assert = require('assert');
var rewire = require('rewire');
var helper = require('../helper');
var Point = require('../../lib/geometry/point');
var moduleName = '../../lib/geometry/circle';
var Circle = require(moduleName);

describe('Circle', function () {
  describe('constructor', function () {
    it('should validate x and y', function () {
      var p = new Point(1, 2.312);
      assert.throws(function () {
        new Circle(p);
      }, TypeError);
      assert.throws(function () {
        new Circle(p, 0);
      }, TypeError);
      assert.throws(function () {
        new Circle(p, -1);
      }, TypeError);
      assert.throws(function () {
        new Circle('1', '2.3');
      }, TypeError);
      assert.throws(function () {
        new Circle(1);
      }, TypeError);
    });
    it('should set #center and #radius properties', function () {
      assert.strictEqual(new Circle(new Point(1, 2), 3).radius, 3);
      assert.strictEqual(new Circle(new Point(1.2, 2.2), 100).center.y, 2.2);
      assert.strictEqual(new Circle(new Point(-1, 2), 1).center.x, -1);
      assert.strictEqual(new Circle(new Point(1, 0), 9).center.y, 0);
    });
  });
  describe('fromBuffer()', function () {
    it('should create an instance from binary representation', function () {
      [
        ['0000000065400000000000000040100000000000003ff0000000000000', 2, 4, 1],
        ['0000000065400199999999999a4010cccccccccccd3fb999999999999a', 2.2, 4.2, 0.1],
        ['0000000065000000000000000000000000000000004059000000000000', 0, 0, 100],
        ['0000000065400aaaaa8b5964f6c025cccccccccccd4010cccccccccccd', 3.3333331, -10.9, 4.2],
        ['01650000009a99999999990140cdcccccccccc10409a9999999999b93f', 2.2, 4.2, 0.1]
      ]
        .forEach(function (item) {
          var c = Circle.fromBuffer(new Buffer(item[0], 'hex'));
          assert.strictEqual(c.center.x, item[1]);
          assert.strictEqual(c.center.y, item[2]);
          assert.strictEqual(c.radius, item[3]);
        });
    });
  });
  describe('#toBuffer()', function () {
    it('should return binary representation in a big-endian OS', function () {
      var BECircle = rewire(moduleName);
      BECircle.__set__('os', { endianness: function() { return 'BE';} });
      [
        [2, 4, 1, '0000000065400000000000000040100000000000003ff0000000000000'],
        [2.2, 4.2, 0.1, '0000000065400199999999999a4010cccccccccccd3fb999999999999a'],
        [0, 0, 100, '0000000065000000000000000000000000000000004059000000000000'],
        [3.3333331, -10.9, 4.2, '0000000065400aaaaa8b5964f6c025cccccccccccd4010cccccccccccd']
      ]
        .forEach(function (item) {
          var p = new BECircle(new Point(item[0], item[1]), item[2]);
          var buffer = p.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[3]);
        });
    });
    it('should return binary representation in a little-endian OS', function () {
      var LECircle = rewire(moduleName);
      LECircle.__set__('os', { endianness: function() { return 'LE';} });
      [
        [2, 4, 1, '016500000000000000000000400000000000001040000000000000f03f'],
        [2.2, 4.2, 0.1, '01650000009a99999999990140cdcccccccccc10409a9999999999b93f'],
        [0, 0, 100, '0165000000000000000000000000000000000000000000000000005940'],
        [3.3333331, -10.9, 4.2, '0165000000f664598baaaa0a40cdcccccccccc25c0cdcccccccccc1040']
      ]
        .forEach(function (item) {
          var p = new LECircle(new Point(item[0], item[1]), item[2]);
          var buffer = p.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[3]);
        });
    });
  });
  describe('#toString()', function () {
    it('should return the string representation of the object', function () {
      [
        [2, 3.2, 6.9, 'CIRCLE ((2 3.2) 6.9)'],
        [2.2, 4.2, 1, 'CIRCLE ((2.2 4.2) 1)'],
        [0, 0, 0.2, 'CIRCLE ((0 0) 0.2)'],
        [3.3333331, -10.9, 6, 'CIRCLE ((3.3333331 -10.9) 6)']
      ]
        .forEach(function (item) {
          var p = new Circle(new Point(item[0], item[1]), item[2]);
          assert.strictEqual(p.toString(), item[3]);
        });
    });
  });
});