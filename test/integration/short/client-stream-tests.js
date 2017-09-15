/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var vit = helper.vit;
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#stream(query, params, {prepare: 0})', function () {
    before(helper.ccmHelper.start(1));
    after(helper.ccmHelper.remove);
    it('should emit end when no rows', function (done) {
      var client = newInstance();
      var stream = client.stream(helper.queries.basicNoResults, [], {prepare: false});
      stream
        .on('end', done)
        .on('readable', function () {
          //Node.js 0.10, readable is never called
          //Node.js 0.12, readable is called with null
          var chunk = stream.read();
          assert.strictEqual(chunk, null);
        })
        .on('error', done);
    });
    it('should end when VOID result', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var query = helper.createKeyspaceCql(keyspace, 1);
      var counter = 0;
      client.stream(query, [], {prepare: false})
        .on('end', function () {
          assert.strictEqual(counter, 0);
          done();
        })
        .on('readable', function () {
          var row;
          while ((row = this.read())) {
            assert.ok(row);
            counter++;
          }
        })
        .on('error', done);
    });
    it('should be readable once when there is one row', function (done) {
      var client = newInstance();
      var stream = client.stream(helper.queries.basic, []);
      var counter = 0;
      stream
        .on('end', function () {
          assert.strictEqual(counter, 1);
          done();
        })
        .on('readable', function () {
          var row;
          while ((row = this.read())) {
            assert.ok(row);
            assert.strictEqual(row.key, 'local');
            counter++;
          }
        })
        .on('error', done);
    });
    it('should emit response errors', function (done) {
      var client = newInstance();
      var stream = client.stream('SELECT WILL FAIL', []);
      var errorCalled = false;
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
          assert.ok(err instanceof errors.ResponseError);
          errorCalled = true;
        });
    });
    it('should not fail with autoPage when there isn\'t any data', function (done) {
      var client = newInstance({keyspace: 'system'});
      var stream = client.stream(helper.queries.basicNoResults, [], {autoPage: true});
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
      var client = newInstance();
      var stream = client.stream(helper.queries.basicNoResults, [], {executionProfile: 'none'});
      var errorCalled = false;
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
    var commonKs = helper.getRandomName('ks');
    var commonTable = commonKs + '.' + helper.getRandomName('table');
    before(function (done) {
      var client = newInstance();
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
      var client = newInstance();
      var stream = client.stream(helper.queries.basicNoResults, [], { prepare: true });
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 1000;
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
            var query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.Uuid.random(), n, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          var query = util.format('SELECT * FROM %s LIMIT 10000', table);
          var counter = 0;
          client.stream(query, [], {prepare: 1})
            .on('end', function () {
              assert.strictEqual(counter, length);
              next();
            })
            .on('readable', function () {
              var row;
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
      var client = newInstance({queryOptions: {consistency: types.consistencies.quorum}});
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 350;
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
            var query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.Uuid.random(), n + 1, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          var query = util.format('SELECT * FROM %s LIMIT 10000', table);
          var counter = 0;
          client.stream(query, [], {autoPage: true, fetchSize: 100, prepare: 1})
            .on('end', function () {
              assert.strictEqual(counter, length);
              next();
            })
            .on('readable', function () {
              var row;
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
      var client = newInstance();
      var stream = client.stream(helper.queries.basic + ' WHERE key = ?', [{}], {prepare: 1});
      var errCalled = false;
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
      var client = newInstance();
      //Invalid amount of parameters
      var stream = client.stream(helper.queries.basic, ['param1'], {prepare: 1});
      var errCalled = false;
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
      var client = newInstance();
      var allRead = false;
      var stream = client.stream(helper.queries.basic, null, {prepare: 1});
      stream.
        on('end', function () {
          assert.strictEqual(allRead, true);
          done();
        })
        .on('error', helper.throwop)
        .on('readable', function () {
          var streamContext = this;
          setTimeout(function () {
            //delay all reading
            var row;
            while ((row = streamContext.read())) {
              assert.ok(row);
            }
            allRead = true;
          }, 2000);
        });
    });
    vit('2.0', 'should not buffer more than fetchSize', function (done) {
      var client = newInstance();
      var id = types.Uuid.random();
      var consistency = types.consistencies.quorum;
      var rowsLength = 1000;
      var fetchSize = 100;
      utils.series([
        function insert(next) {
          var query = util.format('INSERT INTO %s (id1, id2, text_sample) VALUES (?, ?, ?)', commonTable);
          utils.timesLimit(rowsLength, 50, function (n, timesNext) {
            client.execute(query, [id, types.TimeUuid.now(), n.toString()], { prepare: true, consistency: consistency}, timesNext);
          }, next);
        },
        function testBuffering(next) {
          var query = util.format('SELECT id2, text_sample from %s WHERE id1 = ?', commonTable);
          var stream = client.stream(query, [id], { prepare: true, fetchSize: fetchSize, consistency: consistency});
          var rowsRead = 0;
          stream.
            on('end', function () {
              setTimeout(function onEndTimeout() {
                assert.strictEqual(rowsRead, rowsLength);
                next();
              }, 400);
            })
            .on('error', helper.throwop)
            .on('readable', function () {
              var row;
              var self = this;
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
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}