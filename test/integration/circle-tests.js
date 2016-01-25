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
var Circle = require('../../lib/types/circle');
encoderExtensions.register(cassandra.Encoder);

vdescribe('5.0', 'Circle', function () {
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
          "CREATE TABLE circles (id text, value 'CircleType', PRIMARY KEY (id))",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((0 0) 1)', 'CIRCLE((0 0) 1)')",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((1 3) 2.2)', 'CIRCLE((1 3) 2.2)')",
          "INSERT INTO circles (id, value) VALUES ('CIRCLE((-1.2 -100) 0.99)', 'CIRCLE((-1.2 -100) 0.99)')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse circles', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.circles', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['CIRCLE((0 0) 1)', new Circle(new Point(0, 0), 1)],
            ['CIRCLE((1 3) 2.2)', new Circle(new Point(1, 3), 2.2)],
            ['CIRCLE((-1.2 -100) 0.99)', new Circle(new Point(-1.2, -100), 0.99)]
          ]
            .forEach(function (item) {
              var c = map[item[0]];
              helper.assertInstanceOf(c, Circle);
              assert.strictEqual(c.radius, item[1].radius);
              assert.ok(c.equals(item[1]));
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  [0, 1].forEach(function (prepare) {
    var name = prepare ? 'prepared' : 'simple';
    it(util.format('should encode circles for %s queries', name), function (done) {
      var client = new cassandra.Client(helper.getOptions());
      async.series([
        client.connect.bind(client),
        function test(next) {
          var values = [
            new Circle(new Point(1.2, 3.9), 6.2),
            new Circle(new Point(-1.2, 1.9), 111),
            new Circle(new Point(0.21222, 32.9), 10.21222)
          ];
          var insertQuery = 'INSERT INTO ks1.circles (id, value) VALUES (?, ?)';
          var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.circles WHERE id = ?';
          var counter = 0;
          async.each(values, function (circle, eachNext) {
            var id = util.format('%s-%d', name, ++counter);
            client.execute(insertQuery, [id, circle], { prepare: prepare }, function (err) {
              assert.ifError(err);
              client.execute(selectQuery, [id], function (err, result) {
                assert.ifError(err);
                var row = result.first();
                assert.ok(row);
                //use json value to avoid decoding client side in this test
                var value = JSON.parse(row['json_value']);
                assert.deepEqual(value.coordinates, [ circle.center.x, circle.center.y ]);
                assert.strictEqual(value.radius, circle.radius);
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