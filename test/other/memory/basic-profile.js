var assert = require('assert');
var async = require('async');
var util = require('util');
var fs = require('fs');
var heapdump;
var heapdumpPath = '/var/log/nodejs-driver';
try {
  heapdump = require('heapdump');
}
catch (e) {
  console.log(e);
}

var helper = require('../../test-helper.js');
var cassandra = require('../../../index.js');
var Client = cassandra.Client;
var types = cassandra.types;
var utils = require('../../../lib/utils.js');

var client = new Client(helper.baseOptions);
var keyspace = helper.getRandomName('ks');
var table = keyspace + '.' + helper.getRandomName('tbl');

if (!global.gc) {
  console.log('You must run this test exposing the GC');
  return
}

async.series([
  helper.ccmHelper.removeIfAny,
  helper.ccmHelper.start(2),
  client.connect.bind(client),
  function (next) {
    client.execute(helper.createKeyspaceCql(keyspace, 2), helper.waitSchema(client, next));
  },
  function (next) {
    client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
  },
  function insertData(next) {
    console.log('Starting to insert data...');
    var query = util.format('INSERT INTO %s (id, int_sample, blob_sample) VALUES (?, ?, ?)', table);
    var counter = 0;
    var callbackCounter = 0;
    global.gc();
    var heapUsed = process.memoryUsage().heapUsed;
    async.eachLimit(new Array(50000), 500, function (v, timesNext) {
      var n = counter++;
      client.execute(query, [types.uuid(), n, new Buffer(1024)], {prepare: 1}, function (err) {
        if ((callbackCounter++) % 1000 === 0) {
          console.log('Inserted', callbackCounter);
        }
        assert.ifError(err);
        timesNext();
      });
    }, function (err) {
      if (err) return next(err);
      global.gc();
      var diff = process.memoryUsage().heapUsed - heapUsed;
      console.log('Heap used difference', formatLength(diff));
      if (diff > 2024 * 1024) {
        //not even a 2Mb
        return next(new Error('Difference between starting heap and finish heap used size is too large ' + formatLength(diff)));
      }
      if (heapdump) heapdump.writeSnapshot(heapdumpPath + '/' + Date.now() + '.heapsnapshot');
      next();
    });
  },
  function selectData(next) {
    console.log('Retrieving data...');
    var query = util.format('SELECT * FROM %s', table);
    var totalByteLength = 0;
    global.gc();
    var heapUsed = process.memoryUsage().heapUsed;
    var rowCount = 0;
    client.eachRow(query, [], {prepare: true, autoPage: true}, function (n, row) {
      //Buffer + int + uuid
      totalByteLength += row['blob_sample'].length + 4 + 16;
      rowCount++;
    }, function (err, result) {
      if (err) return next(err);
      global.gc();
      var diff = process.memoryUsage().heapUsed - heapUsed;
      assert.strictEqual(rowCount, result.rowLength);
      console.log('Heap used difference', formatLength(diff));
      console.log(util.format('Retrieved %d rows and around %s', result.rowLength, formatLength(totalByteLength)));
      next();
    });
  },
  client.shutdown.bind(client)
], function (err) {
  helper.ccmHelper.removeIfAny();
  assert.ifError(err);
});

function formatLength(value) {
  return Math.floor(value / 1024) + 'KiB';
}