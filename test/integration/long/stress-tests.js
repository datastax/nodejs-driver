"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(180000);
  afterEach(helper.ccmHelper.remove);
  it('should handle parallel insert and select', function (done) {
    var client = newInstance({encoding: { copyBuffer: false}});
    //var client = newInstance();
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('tbl');
    var selectQuery = 'SELECT * FROM ' + table;
    var insertQuery = util.format('INSERT INTO %s (id, text_sample, timestamp_sample) VALUES (?, ?, ?)', table);
    var times = 2000;
    utils.series([
      helper.ccmHelper.start(3),
      function createKs(next) {
        client.execute(helper.createKeyspaceCql(keyspace, 3), [], helper.waitSchema(client, next));
      },
      function createTable(next) {
        client.execute(helper.createTableCql(table), [], helper.waitSchema(client, next));
      },
      function testCase(next) {
        utils.parallel([insert, select], next);
      }
    ], done);
    function insert(callback) {
      utils.timesLimit(times, 500, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      var resultCount = 0;
      utils.timesLimit(Math.floor(times / 5), 200, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.one};
        client.execute(selectQuery, [], options, function (err, result) {
          assert.ifError(err);
          assert.ok(result.rows);
          resultCount += result.rows.length;
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.ok(resultCount > times, 'it should have selected the rows inserted in parallel');
        callback();
      });
    }
  });
  it('should handle parallel insert and select with nodes failing', function (done) {
    var client = newInstance();
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('tbl');
    var selectQuery = 'SELECT * FROM ' + table;
    var insertQuery = util.format('INSERT INTO %s (id, text_sample, timestamp_sample) VALUES (?, ?, ?)', table);
    var times = 2000;
    utils.series([
      helper.ccmHelper.start(3),
      function createKs(next) {
        client.execute(helper.createKeyspaceCql(keyspace, 3), [], helper.waitSchema(client, next));
      },
      function createTable(next) {
        client.execute(helper.createTableCql(table), [], helper.waitSchema(client, next));
      },
      function testCase(next) {
        utils.parallel([insert, select, killANode], next);
      }
    ], done);
    function insert(callback) {
      utils.timesLimit(times, 500, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      var resultCount = 0;
      utils.timesLimit(Math.floor(times / 5), 100, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.one};
        client.execute(selectQuery, [], options, function (err, result) {
          assert.ifError(err);
          assert.ok(result.rows);
          resultCount += result.rows.length;
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.ok(resultCount > times, 'it should have selected the rows inserted in parallel');
        callback();
      });
    }
    function killANode(callback) {
      setTimeout(function () {
        helper.ccmHelper.exec(['node2', 'stop'], callback);
      }, 500);
    }
  });
  it('should handle parallel insert and select of large blobs', function (done) {
    var client = newInstance();
    var keyspace = helper.getRandomName('ks');
    var table = helper.getRandomName('tbl');
    var clientInsert = newInstance({keyspace: keyspace});
    var clientSelect = newInstance({keyspace: keyspace});
    var selectQuery = 'SELECT * FROM ' + table + ' LIMIT 10';
    var insertQuery = util.format('INSERT INTO %s (id, double_sample, blob_sample) VALUES (?, ?, ?)', table);
    var times = 500;
    utils.series([
      helper.ccmHelper.start(3),
      function createKs(next) {
        client.execute(helper.createKeyspaceCql(keyspace, 3), [], helper.waitSchema(client, next));
      },
      function createTable(next) {
        client.execute(helper.createTableCql(keyspace + '.' + table), [], helper.waitSchema(client, next));
      },
      function testCase(next) {
        utils.parallel([insert, select], next);
      }
    ], done);
    function insert(callback) {
      var n = 0;
      utils.timesLimit(times, 100, function (i, next) {
        i = ++n;
        var options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        var buf = new Buffer(i * 1024);
        buf.write(i + ' dummy values ' + (new Array(100)).join(i.toString()));
        clientInsert.execute(insertQuery, [types.uuid(), buf.length, buf], options, next);
      }, callback);
    }
    function select(callback) {
      var resultCount = 0;
      utils.timesLimit(times*10, 2, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.one,
          autoPage: true};
        clientSelect.execute(selectQuery, [], options, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          if (result.rows) {
            result.rows.forEach(function (row) {
              assert.strictEqual(row['blob_sample'].length, row['double_sample']);
              resultCount++;
            });
          }
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.ok(resultCount > times, 'it should have selected the rows inserted in parallel: ' + resultCount);
        callback();
      });
    }
  });
});


/**
 * @returns {Client}
 */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
