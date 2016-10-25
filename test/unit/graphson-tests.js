/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var assert = require('assert');
var cassandra = require('cassandra-driver');
var util = require('util');
var helper = require('../helper');
var GraphSONReader = require('../../lib/encoder-extensions').GraphSONReader;
var graphModule = require('../../lib/graph');
var types = cassandra.types;
var geometry = require('../../lib/geometry');

describe('GraphSONReader', function () {
  describe('#read()', function () {
    var reader = new GraphSONReader();
    (function defineObjectTest() {
      [
        [ 'g:UUID', types.Uuid, types.Uuid.random() ],
        [ 'g:Int64', types.Long, types.Long.fromString('123') ],
        [ 'gx:BigDecimal', types.BigDecimal, types.BigDecimal.fromString('123.32') ],
        [ 'gx:BigInteger', types.Integer, types.Integer.fromString('99901')],
        [ 'gx:InetAddress', types.InetAddress, types.InetAddress.fromString('123.123.123.201')],
        [ 'dse:Blob', Buffer, new Buffer('010203', 'hex')],
        [ 'dse:Point', geometry.Point, new geometry.Point(1, 2.1)],
        [ 'dse:LineString', geometry.LineString, geometry.LineString.fromString('LINESTRING (1 1, 2 2, 3 3)')],
        [ 'dse:Polygon', geometry.Polygon, new geometry.Polygon.fromString('POLYGON ((3 1, 4 4, 2 4, 1 2, 3 1))')]
      ].forEach(function (item) {
        it('should read ' + item[0], function () {
          var obj = {
            "@type": item[0],
            "@value": item[2]
          };
          var result = reader.read(obj);
          helper.assertInstanceOf(result, item[1]);
          assert.ok(result.equals(item[2]));
        });
      });
    })();
    it('should read double, float and int32 as Number', function () {
      [
        [{
          "@type": "g:Int32",
          "@value": 31
        }, 31],
        [{
          "@type": "g:Float",
          "@value": 31.3
        }, 31.3],
        [{
          "@type": "g:Double",
          "@value": 31.2
        }, 31.2]
      ].forEach(function (item) {
        var result = reader.read(item[0]);
        assert.strictEqual(result, item[1]);
        assert.strictEqual(typeof result, 'number');
      });
    });
    it('should read a Date', function () {
      var obj = {
        "@type": "gx:Instant",
        "@value": 123
      };
      var result = reader.read(obj);
      helper.assertInstanceOf(result, Date);
      assert.strictEqual(result.getTime(), 123);
    });
    it('should read Vertex with nested properties', function () {
      var obj = {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1}, "label":"person",
        "properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],
          "age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}};
      var result = reader.read(obj);
      assert.ok(result instanceof graphModule.Vertex);
      assert.strictEqual(result.label, 'person');
      assert.strictEqual(typeof result.id, 'number');
      assert.strictEqual(typeof result.properties, 'object');
      assert.strictEqual(typeof result.properties['name'], 'object');
      assert.strictEqual(util.isArray(result.properties['name']), true);
      assert.strictEqual(result.properties['name'].length, 1);
      helper.assertInstanceOf(result.properties['name'][0].id, types.Long);
      helper.assertInstanceOf(result.properties['age'][0].id, types.Long);
      assert.ok(result.properties['age'][0].id.equals(types.Long.fromString('1')));
      assert.strictEqual(result.properties['age'][0].value, 29);
    });
    it('should read a Path', function () {
      var obj = {"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[
        {"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},
        {"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":"name"}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":"lang"}}]}}},
        "lop"
      ]}};
      var result = reader.read(obj);
      assert.ok(result);
      assert.ok(result.objects);
      assert.ok(result.labels);
      assert.strictEqual(result.objects[2], 'lop');
      assert.ok(result.objects[0] instanceof graphModule.Vertex);
      assert.ok(result.objects[1] instanceof graphModule.Vertex);
      assert.strictEqual(result.objects[0].label, 'person');
      assert.strictEqual(result.objects[1].label, 'software');
    });
  });
});