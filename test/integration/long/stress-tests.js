var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(120000);
  afterEach(helper.ccmHelper.remove);
  it('should handle parallel insert and select', function (done) {
    var client = newInstance();
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('tbl');
    var selectQuery = 'SELECT * FROM ' + table;
    var insertQuery = util.format('INSERT INTO %s (id, text_sample, timestamp_sample) VALUES (?, ?, ?)', table);
    var times = 2000;
    async.series([
      helper.ccmHelper.start(3),
      function createKs(next) {
        client.execute(helper.createKeyspaceCql(keyspace, 3), [], helper.waitSchema(client, next));
      },
      function createTable(next) {
        client.execute(helper.createTableCql(table), [], helper.waitSchema(client, next));
      },
      function testCase(next) {
        async.parallel([insert, select], next);
      }
    ], done);
    function insert(callback) {
      async.eachLimit(new Array(times), 500, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      var resultCount = 0;
      async.eachLimit(new Array(times), 200, function (i, next) {
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
    async.series([
      helper.ccmHelper.start(3),
      function createKs(next) {
        client.execute(helper.createKeyspaceCql(keyspace, 3), [], helper.waitSchema(client, next));
      },
      function createTable(next) {
        client.execute(helper.createTableCql(table), [], helper.waitSchema(client, next));
      },
      function testCase(next) {
        async.parallel([insert, select, killANode], next);
      }
    ], done);
    function insert(callback) {
      async.eachLimit(new Array(times), 500, function (i, next) {
        var options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      var resultCount = 0;
      async.eachLimit(new Array(times), 200, function (i, next) {
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
});
/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}
