/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const rewire = require('rewire');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');
const Point = require('../../../lib/geometry/point');
const moduleName = '../../../lib/geometry/line-string';
const LineString = require(moduleName);

describe('LineString', function () {
  describe('constructor', function () {
    it('should validate points provided', function () {
      assert.doesNotThrow(function () {
        // eslint-disable-next-line
        new LineString(new Point(1, 2.312), new Point(2, 5.3));
      });
      assert.doesNotThrow(function () {
        // empty line strings are valid
        // eslint-disable-next-line
        new LineString();
      });
    });
    it('should set #points property', function () {
      [
        [new Point(1, 3), new Point(3, 1)],
        [new Point(0, 1), new Point(3, 4)]
      ]
        .forEach(function (points) {
          const line = new LineString(points);
          assert.strictEqual(line.points.length, points.length);
        });
    });
  });
  describe('fromBuffer()', function () {
    it('should create an instance from WKB', function () {
      [
        [ '000000000200000002000000000000000000000000000000003ff0000000000000bff3333333333333',
          [ new Point(0, 0), new Point(1, -1.2)]],
        [ '000000000200000002c08f4000000000004161249b3ff7ced9401c000000000000c029b6c8b4395810',
          [ new Point(-1000, 8987865.999), new Point(7, -12.857)]],
        [ '0102000000030000000000000000908440b5f171b7353f2040000000000000f03f0000000000000840000000000000f0bf0000000000c05b40',
          [ new Point(658, 8.1234567), new Point(1, 3), new Point(-1, 111)]]
      ]
        .forEach(function (item) {
          const line = LineString.fromBuffer(utils.allocBufferFromString(item[0], 'hex'));
          assert.strictEqual(line.points.length, item[1].length);
          line.points.forEach(function (p, i) {
            assert.strictEqual(p.toString(), item[1][i].toString());
          });
        });
    });
  });
  describe('#toBuffer()', function () {
    it('should return WKB in a big-endian OS', function () {
      const BELineString = rewire(moduleName);
      [
        [ [ new Point(0, 0), new Point(1, -1.2)],
          '000000000200000002000000000000000000000000000000003ff0000000000000bff3333333333333'],
        [ [ new Point(-1000, 8987865.999), new Point(7, -12.857)],
          '000000000200000002c08f4000000000004161249b3ff7ced9401c000000000000c029b6c8b4395810'],
        [ [ new Point(-123, -1), new Point(72.0555, -42)],
          '000000000200000002c05ec00000000000bff00000000000004052038d4fdf3b64c045000000000000'],
        [ [ new Point(658, 8.1234567), new Point(1, 3), new Point(-1, 111)],
          '000000000200000003408490000000000040203f35b771f1b53ff00000000000004008000000000000bff0000000000000405bc00000000000']
      ]
        .forEach(function (item) {
          const line = new BELineString(item[0]);
          line.useBESerialization = function () {
            return true;
          };
          const buffer = line.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
    it('should return WKB in a little-endian OS', function () {
      const LELineString = rewire(moduleName);
      [
        [ [ new Point(0, 0), new Point(1, -1.2)],
          '01020000000200000000000000000000000000000000000000000000000000f03f333333333333f3bf'],
        [ [ new Point(-1000, 8987865.999), new Point(7, -12.857)],
          '0102000000020000000000000000408fc0d9cef73f9b2461410000000000001c40105839b4c8b629c0'],
        [ [ new Point(-123, -1), new Point(72.0555, -42)],
          '0102000000020000000000000000c05ec0000000000000f0bf643bdf4f8d03524000000000000045c0'],
        [ [ new Point(658, 8.1234567), new Point(1, 3), new Point(-1, 111)],
          '0102000000030000000000000000908440b5f171b7353f2040000000000000f03f0000000000000840000000000000f0bf0000000000c05b40']
      ]
        .forEach(function (item) {
          const line = new LELineString(item[0]);
          line.useBESerialization = function () {
            return false;
          };
          const buffer = line.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
  });
  describe('#toString()', function () {
    it('should return WKT of the object', function () {
      [
        [[ new Point(-123, -1), new Point(72.0555, 42)], 'LINESTRING (-123 -1, 72.0555 42)'],
        [[ new Point(658, 8.1234567), new Point(1, 3), new Point(-1, 111) ], 'LINESTRING (658 8.1234567, 1 3, -1 111)']
      ]
        .forEach(function (item) {
          const p = new LineString(item[0]);
          assert.strictEqual(p.toString(), item[1]);
        });
    });
  });
  describe('#fromString()', function () {
    it('should parse WKT representation', function () {
      [
        ['LINESTRING (10 20, 30 40)', [10, 20, 30, 40]],
        ['LINESTRING(10 20, 30 40)', [10, 20, 30, 40]],
        ['LINESTRING (-10 20.9,30  40)', [-10, 20.9, 30, 40]],
        ['LINESTRING (10 20, 30 40, -50.1 -60.1)', [10, 20, 30, 40, -50.1, -60.1]]
      ].forEach(function (item) {
        const l = LineString.fromString(item[0]);
        const coordinates = item[1];
        assert.strictEqual(l.points.length, coordinates.length / 2);
        for (let i = 0; i < coordinates.length / 2; i++) {
          const p = l.points[i];
          assert.strictEqual(p.x, coordinates[i*2]);
          assert.strictEqual(p.y, coordinates[i*2+1]);
        }
      });
    });
    it('should throw TypeError when WKT representation is invalid', function () {
      [
        'LINESTRING (10 20, 30 40 40)',
        'LINESTRING (10 20)',
        'LINESTRING (10 20,,30 40)'
      ].forEach(function (item) {
        assert.throws(function () {
          LineString.fromString(item);
        }, TypeError);
      });
    });
  });
});