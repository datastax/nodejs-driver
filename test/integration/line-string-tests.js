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
var LineString = require('../../lib/types/line-string');
encoderExtensions.register(cassandra.Encoder);

vdescribe('5.0', 'LineString', function () {
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
          "use ks1",
          "CREATE TABLE lines (id text, value 'LineStringType', PRIMARY KEY (id))",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (0 0, 1 1)', 'LINESTRING (0 0, 1 1)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (1 3, 2 6, 3 9)', 'LINESTRING (1 3, 2 6, 3 9)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING (-1.2 -100, 0.99 3)', 'LINESTRING (-1.2 -100, 0.99 3)')",
          "INSERT INTO lines (id, value) VALUES ('LINESTRING EMPTY', 'LINESTRING EMPTY')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse lines', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.lines', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['LINESTRING (0 0, 1 1)', new LineString(new Point(0, 0), new Point(1, 1))],
            ['LINESTRING (1 3, 2 6, 3 9)', new LineString(new Point(1, 3), new Point(2, 6), new Point(3, 9))],
            ['LINESTRING (-1.2 -100, 0.99 3)', new LineString(new Point(-1.2, -100), new Point(0.99, 3))],
            ['LINESTRING EMPTY', new LineString()]
          ]
            .forEach(function (item) {
              var l = map[item[0]];
              helper.assertInstanceOf(l, LineString);
              assert.strictEqual(l.points.length, item[1].points.length);
              l.points.forEach(function (p1, i) {
                var p2 = item[1].points[i];
                assert.strictEqual(p1.x, p2.x);
                assert.strictEqual(p1.y, p2.y);
              });
              assert.strictEqual(l.toString(), item[0]);
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode lines for %s queries', name), function (done) {
      var client = new cassandra.Client(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new LineString(new Point(1.2, 3.9), new Point(6.2, 18.9)),
            new LineString(new Point(-1.2, 1.9), new Point(111, 22)),
            new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9))
          ];
          var insertQuery = 'INSERT INTO ks1.lines (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.lines WHERE id = ?';
          var counter = 0;
          async.each(values, function (line, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, line], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates, line.points.map(function (p) {
                  return [p.x, p.y];
                }));
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