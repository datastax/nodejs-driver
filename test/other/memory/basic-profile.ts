/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
import assert from "assert";
import util from "util";
import helper from "../../test-helper";
import cassandra from "../../../index";
import utils from "../../../lib/utils";

let heapdump;
const heapdumpPath = '/var/log/nodejs-driver';
try {
  // eslint-disable-next-line global-require
  heapdump = require('heapdump');
}
/* eslint-disable no-console, no-undef */
catch (e) {
  console.error('There was an error while trying to import heapdump', e);
}
const Client = cassandra.Client;
const types = cassandra.types;
let client = new Client(utils.extend({ encoding: { copyBuffer: true}}, helper.baseOptions));
const keyspace = helper.getRandomName('ks');
const table = keyspace + '.' + helper.getRandomName('tbl');

if (!global.gc) {
  console.log('You must run this test exposing the GC');
  return;
}

const insertOnly = process.argv.indexOf('--insert-only') > 0;
const heapUsed = process.memoryUsage().heapUsed;

utils.series([
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
    const query = util.format('INSERT INTO %s (id, int_sample, blob_sample) VALUES (?, ?, ?)', table);
    let counter = 0;
    let callbackCounter = 0;
    global.gc();
    utils.timesLimit(10000, 500, function (v, timesNext) {
      const n = counter++;
      const buffer = utils.allocBufferFromString(generateAsciiString(1024), 'utf8');
      client.execute(query, [types.Uuid.random(), n, buffer], {prepare: true}, function (err) {
        if ((callbackCounter++) % 1000 === 0) {
          console.log('Inserted', callbackCounter);
        }
        assert.ifError(err);
        setImmediate(timesNext);
      });
    }, function (err) {
      if (err) {
        return next(err);
      }
      next();
    });
  }
  ,
  function selectData(next) {
    if (insertOnly) {
      return next();
    }
    console.log('Retrieving data...');
    const query = util.format('SELECT * FROM %s', table);
    let totalByteLength = 0;
    global.gc();
    let rowCount = 0;
    client.eachRow(query, [], {prepare: true, autoPage: true}, function (n, row) {
      //Buffer + int + uuid
      totalByteLength += row['blob_sample'].length + 4 + 16;
      rowCount++;
    }, function (err, result) {
      if (err) {
        return next(err);
      }
      assert.strictEqual(rowCount, result.rowLength);
      console.log(util.format('Retrieved %d rows and around %s', result.rowLength, formatLength(totalByteLength)));
      next();
    });
  }
], function (err) {
  assert.ifError(err);
  client.shutdown(function (err) {
    setImmediate(function () {
      client = null;
      global.gc();
      const diff = process.memoryUsage().heapUsed - heapUsed;
      console.log('Heap used difference', formatLength(diff));
      if (heapdump) {
        heapdump.writeSnapshot(heapdumpPath + '/' + Date.now() + '.heapsnapshot');
      }
      helper.ccmHelper.removeIfAny();
      assert.ifError(err);
    });
  });
});

function formatLength(value) {
  return Math.floor(value / 1024) + 'KiB';
}

function generateAsciiString(length) {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

  for( let i=0; i < length; i++ ){
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}