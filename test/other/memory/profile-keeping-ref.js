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

var client = new Client(utils.extend({ encoding: { copyBuffer: true}}, helper.baseOptions));
var keyspace = helper.getRandomName('ks');
var table = keyspace + '.' + helper.getRandomName('tbl');

if (!global.gc) {
  console.log('You must run this test exposing the GC');
  return
}

var totalLength = 100;
var heapUsed = process.memoryUsage().heapUsed;
var totalByteLength = 0;
var values = [];

async.series([
  helper.ccmHelper.removeIfAny,
  helper.ccmHelper.start(2),
  client.connect.bind(client),
  function (next) {
    client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
  },
  function (next) {
    client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
  },
  function insertData(next) {
    console.log('------------Starting to insert data...');
    var query = util.format('INSERT INTO %s (id, int_sample, bigint_sample, blob_sample) VALUES (?, ?, ?, ?)', table);
    var counter = 0;
    var callbackCounter = 0;
    global.gc();
    async.eachLimit(new Array(totalLength), 500, function (v, timesNext) {
      var n = counter++;
      client.execute(query, [types.uuid(), n, types.Long.fromNumber(n), new Buffer(generateAsciiString(1024))], {prepare: 1}, function (err) {
        if ((callbackCounter++) % 1000 === 0) {
          console.log('Inserted', callbackCounter);
        }
        assert.ifError(err);
        setImmediate(timesNext);
      });
    }, function (err) {
      next(err);
    });
  },
  function selectData(next) {
    console.log('------------Retrieving data...');
    var query = util.format('SELECT id, int_sample, bigint_sample, blob_sample FROM %s', table);
    //var query = util.format('SELECT blob_sample FROM %s', table);
    global.gc();
    client.eachRow(query, [], {prepare: true, autoPage: true}, function (n, row) {
      //Buffer length + uuid + int + bigint
      totalByteLength += row['blob_sample'].length + 4 + 16 + 8;
      values.push(row.values());
    }, function (err) {
      next(err);
    });
  }
], function (err) {
  assert.ifError(err);
  client.shutdown(function (err) {
    setImmediate(function () {
      client = null;
      global.gc();
      var diff = process.memoryUsage().heapUsed - heapUsed;
      console.log('Byte length %s in %d values', formatLength(totalByteLength), values.length);
      console.log('Heap used difference', formatLength(diff));
      if (heapdump) heapdump.writeSnapshot(heapdumpPath + '/' + Date.now() + '.heapsnapshot');
      helper.ccmHelper.removeIfAny();
      assert.ifError(err);
    });
  });
});

function formatLength(value) {
  var kbValues = Math.floor(value / 1024);
  if (kbValues > 1024) {
    return (kbValues / 1024).toFixed(2) + 'MiB';
  }
  return kbValues + 'KiB';
}

function generateAsciiString(length) {
  var text = '';
  var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for( var i=0; i < length; i++ ){
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}