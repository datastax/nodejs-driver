'use strict';
var assert = require('assert');
var util = require('util');
var async = require('async');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');
var vdescribe = helper.vdescribe;
var encoderExtensions = require('../../lib/encoder-extensions');
var Point = require('../../lib/types/point');
var Polygon = require('../../lib/types/polygon');
encoderExtensions.register(cassandra.Encoder);

vdescribe('5.0', 'Polygon', function () {
  this.timeout(120000);
  before(function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      function (next) {
        helper.ccm.startAll(1, {}, next);
      },
      client.connect.bind(client),
      function createAll(next) {
        var queries = [
          "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = false",
          "USE ks1",
          "CREATE TABLE polygons (id text, value 'PolygonType', PRIMARY KEY (id))",
          "INSERT INTO polygons (id, value) VALUES ('sample1', 'POLYGON ((1 3, 3 1, 3 6, 1 3))')",
          "INSERT INTO polygons (id, value) VALUES ('sample2', 'POLYGON((0 10, 10 0, 10 10, 0 10), (6 7,3 9,9 9,6 7))')",
          "INSERT INTO polygons (id, value) VALUES ('sample3', 'POLYGON EMPTY')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse polygons', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.polygons', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['sample1', new Polygon([new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)])],
            ['sample2', new Polygon(
              [new Point(0, 10), new Point(10, 0), new Point(10, 10), new Point(0, 10)],
              [new Point(6, 7), new Point(3, 9), new Point(9, 9), new Point(6, 7)]
            )],
            ['sample3', new Polygon()]
          ]
            .forEach(function (item) {
              var polygon = map[item[0]];
              helper.assertInstanceOf(polygon, Polygon);
              assert.strictEqual(polygon.rings.length, item[1].rings.length);
              polygon.rings.forEach(function (r1, i) {
                var r2 = item[1].rings[i];
                assert.strictEqual(r1.length, r2.length);
                assert.strictEqual(r1.join(', '), r2.join(', '));
                r1.forEach(function (p, j) {
                  p.equals(r2[j]);
                });
              });
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode polygons for %s queries', name), function (done) {
      var client = new cassandra.Client(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new Polygon([new Point(1, 3), new Point(3, 6.2), new Point(3, -11.2), new Point(1, 3)]),
            new Polygon(
              [new Point(-10, 10), new Point(10, 10), new Point(10, 0), new Point(-10, 10)],
              [new Point(6, 7), new Point(9, 9), new Point(3, 9), new Point(6, 7)]
            ),
            new Polygon()
          ];
          var insertQuery = 'INSERT INTO ks1.polygons (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.polygons WHERE id = ?';
          var counter = 0;
          async.each(values, function (polygon, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, polygon], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates || [], polygon.toJSON().coordinates);
                eachNext();
              });
            });
          }, next);
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  after(helper.ccm.remove.bind(helper.ccm));
});