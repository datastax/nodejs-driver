'use strict';
var assert = require('assert');
var util = require('util');
var async = require('async');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');
var vdescribe = helper.vdescribe;
var encoderExtensions = require('../../lib/encoder-extensions');
encoderExtensions.register(cassandra.Encoder);
var Point = require('../../lib/types/point');

vdescribe('5.0', 'Point', function () {
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
          "CREATE TABLE points (id text, value 'PointType', PRIMARY KEY (id))",
          "INSERT INTO points (id, value) VALUES ('POINT(0 0)', 'POINT(0 0)')",
          "INSERT INTO points (id, value) VALUES ('POINT(2 4)', 'POINT(2 4)')",
          "INSERT INTO points (id, value) VALUES ('POINT(-1.2 -100)', 'POINT(-1.2 -100)')"
        ];
        async.eachSeries(queries, function (q, eachNext) {
          client.execute(q, eachNext);
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should parse points', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        client.execute('SELECT * FROM ks1.points', function (err, result) {
          assert.ifError(err);
          var map = helper.keyedById(result);
          [
            ['POINT(0 0)', 0, 0],
            ['POINT(2 4)', 2, 4],
            ['POINT(-1.2 -100)', -1.2, -100]
          ]
            .forEach(function (item) {
              var p = map[item[0]];
              helper.assertInstanceOf(p, Point);
              assert.strictEqual(p.x, item[1]);
              assert.strictEqual(p.y, item[2]);
            });
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should encode points for simple queries', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        var values = [
          new Point(1.2, 3.9),
          new Point(-1.2, 1.9),
          new Point(0.21222, 3122.9)
        ];
        var insertQuery = 'INSERT INTO ks1.points (id, value) VALUES (?, ?)';
        var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.points WHERE id = ?';
        async.each(values, function (p, eachNext) {
          var id = util.format('simple[%d,%d]', p.x, p.y);
          client.execute(insertQuery, [id, p], { prepare: false }, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
              assert.ok(row);
              //use json value to avoid decoding client side in this test
              var value = JSON.parse(row['json_value']);
              assert.deepEqual(value.coordinates, [p.x, p.y]);
              eachNext();
            });
          });
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  it('should encode points for prepared queries', function (done) {
    var client = new cassandra.Client(helper.getOptions());
    async.series([
      client.connect.bind(client),
      function test(next) {
        var values = [
          new Point(-1.2, -3.9),
          new Point(1.2, -1.9),
          new Point(10.21222, 1622.9)
        ];
        var insertQuery = 'INSERT INTO ks1.points (id, value) VALUES (?, ?)';
        var selectQuery = 'SELECT toJSON(value) as json_value FROM ks1.points WHERE id = ?';
        async.each(values, function (p, eachNext) {
          var id = util.format('simple[%d,%d]', p.x, p.y);
          client.execute(insertQuery, [id, p], { prepare: true }, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
              assert.ok(row);
              //use json value to avoid decoding client side in this test
              var value = JSON.parse(row['json_value']);
              assert.deepEqual(value.coordinates, [p.x, p.y]);
              eachNext();
            });
          });
        }, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
});