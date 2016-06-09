/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var rewire = require('rewire');
var helper = require('../helper');
var Point = require('../../lib/geometry/point');
var moduleName = '../../lib/geometry/polygon';
var Polygon = require(moduleName);

describe('Polygon', function () {
  describe('constructor', function () {
    it('should validate points provided', function () {
      assert.doesNotThrow(function () {
        new Polygon(new Point(1, 2.312));
      });
      assert.doesNotThrow(function () {
        //empty polygons are valid
        new Polygon();
      });
    });
    it('should set #rings property', function () {
      [
        [new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)],
        [new Point(0, 1), new Point(3, 4)]
      ]
        .forEach(function (ring) {
          var polygon = new Polygon(ring);
          assert.strictEqual(polygon.rings.length, 1);
          assert.strictEqual(polygon.rings[0].length, ring.length);
          assert.strictEqual(JSON.stringify(polygon.rings[0]), JSON.stringify(ring));
        });
    });
  });
  describe('fromBuffer()', function () {
    it('should create an instance from WKB', function () {
      [
        [ '00000000030000000100000004' +
        '3ff0000000000000' + //p1
        '4008000000000000' +
        '4008000000000000' + //p2
        '3ff0000000000000' +
        '4008000000000000' + //p3
        '4018000000000000' +
        '3ff0000000000000' + //p4
        '4008000000000000',
          [new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)]]
      ]
        .forEach(function (item) {
          var polygon = Polygon.fromBuffer(new Buffer(item[0], 'hex'));
          assert.strictEqual(polygon.rings.length, 1);
          polygon.rings[0].forEach(function (p, i) {
            assert.strictEqual(p.toString(), item[1][i].toString());
          });
        });
    });
  });
  describe('#toBuffer()', function () {
    it('should return WKB in a big-endian OS', function () {
      var BEPolygon = rewire(moduleName);
      BEPolygon.__set__('os', { endianness: function() { return 'BE';} });
      [
        [ [new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)],
          '00000000030000000100000004' +
          '3ff0000000000000' + //p1
          '4008000000000000' +
          '4008000000000000' + //p2
          '3ff0000000000000' +
          '4008000000000000' + //p3
          '4018000000000000' +
          '3ff0000000000000' + //p4
          '4008000000000000']
      ]
        .forEach(function (item) {
          var polygon = new BEPolygon(item[0]);
          var buffer = polygon.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
    it('should return WKB in a little-endian OS', function () {
      var LEPolygon = rewire(moduleName);
      LEPolygon.__set__('os', { endianness: function() { return 'LE';} });
      [
        [ [ new Point(0, 3), new Point(3, 1), new Point(3, 6), new Point(0, 3)],
          '01030000000100000004000000000000000000000000000000000008400000000000000840000000000000f03f0000000000000840000000000000184000000000000000000000000000000840'
        ]
      ]
        .forEach(function (item) {
          var polygon = new LEPolygon(item[0]);
          var buffer = polygon.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
  });
  describe('#toString()', function () {
    it('should return WKT of the object', function () {
      [
        [ new Polygon([new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)]),
          'POLYGON ((1 3, 3 1, 3 6, 1 3))'
        ],
        [ new Polygon(
            [new Point(1.1, 3.3), new Point(3, 0), new Point(3, 6), new Point(1, 3)],
            [new Point(2, 2), new Point(2, 1), new Point(1, 1), new Point(2, 2)]
          ),
          'POLYGON ((1.1 3.3, 3 0, 3 6, 1 3), (2 2, 2 1, 1 1, 2 2))'
        ]
      ]
        .forEach(function (item) {
          assert.strictEqual(item[0].toString(), item[1]);
        });
    });
  });
  describe('#toJSON()', function () {
    it('should return geo json of the object', function () {
      [
        [ new Polygon([new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)]),
          '{"type":"Polygon","coordinates":[[[1,3],[3,1],[3,6],[1,3]]]}'
        ],
        [ new Polygon(
            [new Point(1.1, 3.3), new Point(3, 0), new Point(3, 6), new Point(1, 3)],
            [new Point(2, 2), new Point(2, 1), new Point(1, 1), new Point(2, 2)]
          ),
          '{"type":"Polygon","coordinates":[[[1.1,3.3],[3,0],[3,6],[1,3]],[[2,2],[2,1],[1,1],[2,2]]]}'
        ]
      ]
        .forEach(function (item) {
          assert.strictEqual(JSON.stringify(item[0]), item[1]);
        });
    });
  });
});