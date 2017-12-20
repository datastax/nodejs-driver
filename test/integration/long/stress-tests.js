"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(180000);
  afterEach(helper.ccmHelper.remove);
  it('should handle parallel insert and select', function (done) {
    const client = newInstance({encoding: { copyBuffer: false}});
    //const client = newInstance();
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('tbl');
    const selectQuery = 'SELECT * FROM ' + table;
    const insertQuery = util.format('INSERT INTO %s (id, text_sample, timestamp_sample) VALUES (?, ?, ?)', table);
    const times = 2000;
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
        const options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      let resultCount = 0;
      utils.timesLimit(Math.floor(times / 5), 200, function (i, next) {
        const options = {
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
    const client = newInstance();
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('tbl');
    const selectQuery = 'SELECT * FROM ' + table;
    const insertQuery = util.format('INSERT INTO %s (id, text_sample, timestamp_sample) VALUES (?, ?, ?)', table);
    const times = 2000;
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
        const options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        client.execute(insertQuery, [types.uuid(), 'text' + i, new Date()], options, next);
      }, callback);
    }
    function select(callback) {
      let resultCount = 0;
      utils.timesLimit(Math.floor(times / 5), 100, function (i, next) {
        const options = {
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
    const client = newInstance();
    const keyspace = helper.getRandomName('ks');
    const table = helper.getRandomName('tbl');
    const clientInsert = newInstance({keyspace: keyspace});
    const clientSelect = newInstance({keyspace: keyspace});
    const selectQuery = 'SELECT * FROM ' + table + ' LIMIT 10';
    const insertQuery = util.format('INSERT INTO %s (id, double_sample, blob_sample) VALUES (?, ?, ?)', table);
    const times = 500;
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
      let n = 0;
      utils.timesLimit(times, 100, function (i, next) {
        i = ++n;
        const options = {
          prepare: 1,
          consistency: types.consistencies.quorum};
        const buf = utils.allocBuffer(i * 1024);
        buf.write(i + ' dummy values ' + (new Array(100)).join(i.toString()));
        clientInsert.execute(insertQuery, [types.uuid(), buf.length, buf], options, next);
      }, callback);
    }
    function select(callback) {
      let resultCount = 0;
      utils.timesLimit(times*10, 2, function (i, next) {
        const options = {
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
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}
