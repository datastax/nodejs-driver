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
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper.js');
const vit = helper.vit;
const Client = require('../../../lib/client.js');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils.js');
const errors = require('../../../lib/errors.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#stream(query, params, {prepare: 0})', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should emit end when no rows', function (done) {
      const client = newInstance();
      const stream = client.stream(helper.queries.basicNoResults, [], {prepare: false});
      stream
        .on('end', done)
        .on('readable', function () {
          //Node.js 0.10, readable is never called
          //Node.js 0.12, readable is called with null
          const chunk = stream.read();
          assert.strictEqual(chunk, null);
        })
        .on('error', done);
    });
    it('should end when VOID result', function (done) {
      const client = newInstance();
      const keyspace = helper.getRandomName('ks');
      const query = helper.createKeyspaceCql(keyspace, 1);
      let counter = 0;
      client.stream(query, [], {prepare: false})
        .on('end', function () {
          assert.strictEqual(counter, 0);
          done();
        })
        .on('readable', function () {
          let row;
          while ((row = this.read())) {
            assert.ok(row);
            counter++;
          }
        })
        .on('error', done);
    });
    it('should be readable once when there is one row', function (done) {
      const client = newInstance();
      const stream = client.stream(helper.queries.basic, []);
      let counter = 0;
      stream
        .on('end', function () {
          assert.strictEqual(counter, 1);
          done();
        })
        .on('readable', function () {
          let row;
          while ((row = this.read())) {
            assert.ok(row);
            assert.strictEqual(row.key, 'local');
            counter++;
          }
        })
        .on('error', done);
    });
    it('should emit response errors', function (done) {
      const client = newInstance();
      const stream = client.stream('SELECT WILL FAIL', []);
      let errorCalled = false;
      stream
        .on('end', function () {
          assert.strictEqual(errorCalled, true);
          done();
        })
        .on('readable', function () {
          //Node.js 0.10, never emits readable
          //Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ok(err, 'It should yield an error');
          helper.assertInstanceOf(err, errors.ResponseError);
          errorCalled = true;
        });
    });
    it('should not fail with autoPage when there isn\'t any data', function (done) {
      const client = newInstance({keyspace: 'system'});
      const stream = client.stream(helper.queries.basicNoResults, [], {autoPage: true});
      stream
        .on('end', function () {
          done();
        })
        .on('readable', function () {
          //Node.js 0.10, never emits readable
          //Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ifError(err);
        });
    });
    it('should emit error if non-existent profile provided', function (done) {
      const client = newInstance();
      const stream = client.stream(helper.queries.basicNoResults, [], {executionProfile: 'none'});
      let errorCalled = false;
      stream
        .on('end', function () {
          assert.strictEqual(errorCalled, true);
          done();
        })
        .on('readable', function () {
          //Node.js 0.10, never emits readable
          //Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ok(err);
          helper.assertInstanceOf(err, errors.ArgumentError);
          errorCalled = true;
        });
    });
  });
  describe('#stream(query, params, {prepare: 1})', function () {
    const commonKs = helper.getRandomName('ks');
    const commonTable = commonKs + '.' + helper.getRandomName('table');
    before(function (done) {
      const client = newInstance();
      utils.series([
        helper.ccmHelper.start(3),
        client.connect.bind(client),
        helper.toTask(client.execute, client, helper.createKeyspaceCql(commonKs, 3)),
        helper.toTask(client.execute, client, helper.createTableWithClusteringKeyCql(commonTable)),
        client.shutdown.bind(client)
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should prepare and emit end when no rows', function (done) {
      const client = newInstance();
      const stream = client.stream(helper.queries.basicNoResults, [], { prepare: true });
      stream
        .on('end', function () {
          done();
        })
        .on('readable', function () {
          //Node.js 0.10, never emits readable
          //Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ifError(err);
        });
    });
    it('should prepare and emit the exact amount of rows', function (done) {
      const client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      const keyspace = helper.getRandomName('ks');
      const table = keyspace + '.' + helper.getRandomName('table');
      const length = 1000;
      utils.series([
        client.connect.bind(client),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
        },
        function (next) {
          utils.timesLimit(length, 100, function (n, timesNext) {
            let query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.Uuid.random(), n, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          const query = util.format('SELECT * FROM %s LIMIT 10000', table);
          let counter = 0;
          client.stream(query, [], {prepare: 1})
            .on('end', function () {
              assert.strictEqual(counter, length);
              next();
            })
            .on('readable', function () {
              let row;
              while ((row = this.read())) {
                assert.ok(row);
                assert.strictEqual(typeof row.int_sample, 'number');
                counter++;
              }
            })
            .on('error', function (err) {
              assert.ifError(err);
            });
        }
      ], done);
    });
    it('should prepare and fetch paging the exact amount of rows', function (done) {
      const client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      const keyspace = helper.getRandomName('ks');
      const table = keyspace + '.' + helper.getRandomName('table');
      const length = 350;
      utils.series([
        client.connect.bind(client),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
        },
        function (next) {
          utils.timesLimit(length, 100, function (n, timesNext) {
            let query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.Uuid.random(), n + 1, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          const query = util.format('SELECT * FROM %s LIMIT 10000', table);
          let counter = 0;
          client.stream(query, [], {autoPage: true, fetchSize: 100, prepare: 1})
            .on('end', function () {
              assert.strictEqual(counter, length);
              next();
            })
            .on('readable', function () {
              let row;
              while ((row = this.read())) {
                assert.ok(row);
                assert.ok(row.int_sample);
                counter++;
              }
            })
            .on('error', function (err) {
              assert.ifError(err);
            });
        }
      ], done);
    });
    it('should emit argument parsing errors', function (done) {
      const client = newInstance();
      const stream = client.stream(helper.queries.basic + ' WHERE key = ?', [{}], {prepare: 1});
      let errCalled = false;
      stream
        .on('error', function (err) {
          assert.ok(err);
          assert.ok(err instanceof TypeError, 'Error should be an instance of TypeError');
          errCalled = true;
        })
        .on('readable', function () {
          assert.strictEqual(stream.read(), null);
        })
        .on('end', function () {
          assert.strictEqual(errCalled, true);
          done();
        });
    });
    it('should emit other ResponseErrors', function (done) {
      const client = newInstance();
      //Invalid amount of parameters
      const stream = client.stream(helper.queries.basic, ['param1'], {prepare: 1});
      let errCalled = false;
      stream
        .on('readable', function () {
          //Node.js 0.10, never emits readable
          //Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ok(err);
          assert.ok(err instanceof errors.ResponseError, 'Error should be an instance of ResponseError');
          assert.ok(err.code === types.responseErrorCodes.invalid || err.code === types.responseErrorCodes.protocolError, 'Obtained err code ' + err.code);
          errCalled = true;
        })
        .on('end', function () {
          assert.strictEqual(errCalled, true);
          done();
        });
    });
    it('should wait buffer until read', function (done) {
      const client = newInstance();
      let allRead = false;
      const stream = client.stream(helper.queries.basic, null, {prepare: 1});
      stream.
        on('end', function () {
          assert.strictEqual(allRead, true);
          done();
        })
        .on('error', helper.throwop)
        .on('readable', function () {
          const streamContext = this;
          setTimeout(function () {
            //delay all reading
            let row;
            while ((row = streamContext.read())) {
              assert.ok(row);
            }
            allRead = true;
          }, 2000);
        });
    });
    vit('2.0', 'should not buffer more than fetchSize', function (done) {
      const client = newInstance();
      const id = types.Uuid.random();
      const consistency = types.consistencies.quorum;
      const rowsLength = 1000;
      const fetchSize = 100;
      utils.series([
        function insert(next) {
          const query = util.format('INSERT INTO %s (id1, id2, text_sample) VALUES (?, ?, ?)', commonTable);
          utils.timesLimit(rowsLength, 50, function (n, timesNext) {
            client.execute(query, [id, types.TimeUuid.now(), n.toString()], { prepare: true, consistency: consistency}, timesNext);
          }, next);
        },
        function testBuffering(next) {
          const query = util.format('SELECT id2, text_sample from %s WHERE id1 = ?', commonTable);
          const stream = client.stream(query, [id], { prepare: true, fetchSize: fetchSize, consistency: consistency});
          let rowsRead = 0;
          stream.
            on('end', function () {
              setTimeout(function onEndTimeout() {
                assert.strictEqual(rowsRead, rowsLength);
                next();
              }, 400);
            })
            .on('error', helper.throwop)
            .on('readable', function () {
              let row;
              const self = this;
              utils.whilst(function condition() {
                assert.ok(self.buffer.length <= fetchSize);
                return (row = self.read());
              }, function iterator(whilstNext) {
                assert.ok(self.buffer.length <= fetchSize);
                assert.ok(row);
                rowsRead++;
                if (rowsRead % 55 === 0) {
                  //delay from time to time
                  return setTimeout(whilstNext, 100);
                }
                whilstNext();
              }, helper.noop);
            });
        }
      ], done);
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  return helper.shutdownAfterThisTest(new Client(utils.deepExtend({}, helper.baseOptions, options)));
}