/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');
const Point = require('../../../lib/geometry/point');
const moduleName = '../../../lib/geometry/polygon';
const Polygon = require(moduleName);

describe('Polygon', function () {
  describe('constructor', function () {
    it('should validate points provided', function () {
      assert.doesNotThrow(function () {
        // eslint-disable-next-line
        new Polygon(new Point(1, 2.312));
      });
      assert.doesNotThrow(function () {
        //empty polygons are valid
        // eslint-disable-next-line
        new Polygon();
      });
    });
    it('should set #rings property', function () {
      [
        [new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)],
        [new Point(0, 1), new Point(3, 4)]
      ]
        .forEach(function (ring) {
          const polygon = new Polygon(ring);
          assert.strictEqual(polygon.rings.length, 1);
          assert.strictEqual(polygon.rings[0].length, ring.length);
          assert.strictEqual(JSON.stringify(polygon.rings[0]), JSON.stringify(ring));
        });
    });
  });
  describe('fromBuffer()', function () {
    it('should create an instance from WKB', function () {
      [
        [
          '00000000030000000100000004' +
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
          const polygon = Polygon.fromBuffer(utils.allocBufferFromString(item[0], 'hex'));
          assert.strictEqual(polygon.rings.length, 1);
          polygon.rings[0].forEach(function (p, i) {
            assert.strictEqual(p.toString(), item[1][i].toString());
          });
        });
    });
  });
  describe('#toBuffer()', function () {
    it('should return WKB in a big-endian OS', function () {
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
          const polygon = new Polygon(item[0]);
          polygon.useBESerialization = function () {
            return true;
          };
          const buffer = polygon.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
    it('should return WKB in a little-endian OS', function () {
      [
        [ [ new Point(0, 3), new Point(3, 1), new Point(3, 6), new Point(0, 3)],
          '01030000000100000004000000000000000000000000000000000008400000000000000840000000000000f03f0000000000000840000000000000184000000000000000000000000000000840'
        ]
      ]
        .forEach(function (item) {
          const polygon = new Polygon(item[0]);
          polygon.useBESerialization = function () {
            return false;
          };
          const buffer = polygon.toBuffer();
          helper.assertInstanceOf(buffer, Buffer);
          assert.strictEqual(buffer.toString('hex'), item[1]);
        });
    });
  });
  describe('#toString()', function () {
    it('should return WKT of the object', function () {
      [
        [
          new Polygon([new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)]),
          'POLYGON ((1 3, 3 1, 3 6, 1 3))'
        ],
        [
          new Polygon(
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
        [
          new Polygon([new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)]),
          '{"type":"Polygon","coordinates":[[[1,3],[3,1],[3,6],[1,3]]]}'
        ],
        [
          new Polygon(
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
  describe('#fromString()', function () {
    it('should parse WKT representation', function () {
      [
        ['POLYGON ((10 20, 30 40, 30 -40, 10 20))', [[10, 20, 30, 40, 30, -40, 10, 20]]],
        ['POLYGON((11.9 20, 30 40, 30 -40, 11.9 20))', [[11.9, 20, 30, 40, 30, -40, 11.9, 20]]],
        [
          'POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30.1, 35 35, 30.1 20, 20 30.1))',
          [[35, 10, 45, 45, 15, 40, 10, 20, 35, 10], [20, 30.1, 35, 35, 30.1, 20, 20, 30.1]]
        ]
      ].forEach(function (item) {
        const shape = Polygon.fromString(item[0]);
        const rings = item[1];
        assert.strictEqual(shape.rings.length, rings.length);
        rings.forEach(function (ringPoints, ringIndex) {
          const shapeRing = shape.rings[ringIndex];
          assert.strictEqual(shapeRing.length, ringPoints.length / 2);
          for (let i = 0; i < ringPoints.length / 2; i++) {
            const p = shapeRing[i];
            assert.strictEqual(p.x, ringPoints[i*2]);
            assert.strictEqual(p.y, ringPoints[i*2+1]);
          }
        });
      });
    });
    it('should throw TypeError when WKT representation is invalid', function () {
      [
        'POLYGON (10 20, 30 40, -30 -10, 10 20)',
        'POLYGON ((10 20, 30 40, 30 -40, 10))',
        'POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),|(20 30.1, 35 35, 30.1 20, 20 30.1))'
      ].forEach(function (item) {
        assert.throws(function () {
          Polygon.fromString(item);
        }, TypeError);
      });
    });
  });
});