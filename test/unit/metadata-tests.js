"use strict";
var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
var clientOptions = require('../../lib/client-options.js');
var Host = require('../../lib/host.js').Host;
var Metadata = require('../../lib/metadata');
var tokenizer = require('../../lib/tokenizer');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var Encoder = require('../../lib/encoder');

describe('Metadata', function () {
  describe('#getReplicas()', function () {
    it('should return depending on the rf and ring size with simple strategy', function () {
      var metadata = new Metadata(clientOptions.defaultOptions());
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.ring = [0, 1, 2, 3, 4, 5];
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'SimpleStrategy',
        'strategy_options': '{"replication_factor": 3}'
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //Primary replica plus the 2 next tokens
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '0');
      assert.strictEqual(replicas[1], '1');
      assert.strictEqual(replicas[2], '2');

      replicas = metadata.getReplicas('dummy', new Buffer([5]));
      assert.ok(replicas);
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '5');
      assert.strictEqual(replicas[1], '0');
      assert.strictEqual(replicas[2], '1');
    });
    it('should return depending on the dc rf with network topology', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var metadata = new Metadata(options);
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.datacenters = {'dc1': 4, 'dc2': 4};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7];
      //load even primary replicas
      metadata.primaryReplicas = {};
      for (var i = 0; i < metadata.ring.length; i ++) {
        var h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        metadata.primaryReplicas[i.toString()] = h;
      }
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "1"}'
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //3 replicas from dc1 and 1 replica from dc2
      assert.strictEqual(replicas.length, 4);
      assert.strictEqual(replicas[0].address, '0');
      assert.strictEqual(replicas[1].address, '1');
      assert.strictEqual(replicas[2].address, '2');
      assert.strictEqual(replicas[3].address, '4');
    });
  });
  describe('#clearPrepared()', function () {
    it('should clear the internal state', function () {
      var metadata = new Metadata(clientOptions.defaultOptions());
      metadata.getPreparedInfo('QUERY1');
      metadata.getPreparedInfo('QUERY2');
      assert.strictEqual(metadata.preparedQueries['__length'], 2);
      metadata.clearPrepared();
      assert.strictEqual(metadata.preparedQueries['__length'], 0);
    });
  });
  describe('#getUdt()', function () {
    it('should retrieve the udt information', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.ok(udtInfo);
        assert.strictEqual(udtInfo.name, 'udt1');
        assert.ok(udtInfo.fields);
        assert.strictEqual(udtInfo.fields.length, 2);
        done();
      });
    });
    it('should callback in err when there is an error', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(new Error('Test error'));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
    });
    it('should be null when it is not found', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: []}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should be null when keyspace does not exists', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = {};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      async.times(50, function (n, next) {
        metadata.getUdt('ks1', 'udt5', function (err, udtInfo) {
          if (err) return next(err);
          assert.ok(udtInfo);
          assert.ok(util.isArray(udtInfo.fields));
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 1);
        done();
      });
    });
    it('should query once and cache when called serially', function (done) {
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.BooleanType']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      async.timesSeries(50, function (n, next) {
        metadata.getUdt('ks1', 'udt10', function (err, udtInfo) {
          if (err) return next(err);
          assert.ok(udtInfo);
          assert.ok(util.isArray(udtInfo.fields));
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 1);
        done();
      });
    });
    it('should query the following times if it was null', function (done) {
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          queried++;
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: []}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      async.timesSeries(20, function (n, next) {
        metadata.getUdt('ks1', 'udt20', function (err, udtInfo) {
          if (err) return next(err);
          assert.strictEqual(udtInfo, null);
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 20);
        done();
      });
    });
  });
});