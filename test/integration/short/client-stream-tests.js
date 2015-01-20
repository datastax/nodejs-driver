var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
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
      client._getPrepared = function () { throw new Error('Query should not be prepared')};
      var stream = client.stream('SELECT * FROM system.schema_keyspaces where keyspace_name = \'___notexists\'', [], {prepare: false});
      stream
        .on('end', done)
        .on('readable', function () {
          assert.ok(false, 'Readable event should not be fired');
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
          while (row = this.read()) {
            counter++;
          }
        })
        .on('error', done);
    });
    it('should be readable once when there is one row', function (done) {
      var client = newInstance();
      var stream = client.stream('SELECT * FROM system.schema_keyspaces where keyspace_name = \'system\'', []);
      var counter = 0;
      stream
        .on('end', function () {
          assert.strictEqual(counter, 1);
          done();
        })
        .on('readable', function () {
          var row;
          while (row = this.read()) {
            assert.ok(row);
            assert.strictEqual(row.keyspace_name, 'system');
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
          assert.ok(false, 'It should not be emit readable');
        })
        .on('error', function (err) {
          assert.ok(err, 'It should yield an error');
          assert.ok(err instanceof errors.ResponseError);
          errorCalled = true;
        });
    });
    it('should not fail with autoPage when there isnt any data', function (done) {
      var client = newInstance({keyspace: 'system'});
      var stream = client.stream('SELECT * from schema_keyspaces WHERE keyspace_name = \'KS_NOT_EXISTS\'', [], {autoPage: true});
      var errorCalled = false;
      stream
        .on('end', function () {
          done();
        })
        .on('readable', function () {
          assert.ok(false, 'It should not be emit readable');
        })
        .on('error', function (err) {
          assert.ifError(err);
        });
    });
  });
  describe('#stream(query, params, {prepare: 1})', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should prepare and emit end when no rows', function (done) {
      var client = newInstance();
      var originalGetPrepared = client._getPrepared;
      var calledPrepare = true;
      client._getPrepared = function () {
        calledPrepare = true;
        originalGetPrepared.apply(client, arguments);
      };
      var stream = client.stream('SELECT * FROM system.schema_columnfamilies where keyspace_name = \'___notexists\'', [], {prepare: 1});
      stream
        .on('end', function () {
          assert.strictEqual(calledPrepare, true);
          done();
        })
        .on('readable', function () {
          assert.ok(false, 'Readable event should not be fired');
        })
        .on('error', function (err) {
          assert.ifError(err);
        });
    });
    it('should prepare and emit the exact amount of rows', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 1000;
      async.series([
        client.connect.bind(client),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
        },
        function (next) {
          async.times(length, function (n, timesNext) {
            var query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.uuid(), n, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          var query = util.format('SELECT * FROM %s LIMIT 10000', table);
          var counter = 0;
          var stream = client.stream(query, [], {prepare: 1, consistency: types.consistencies.quorum})
            .on('end', function () {
              assert.strictEqual(counter, length);
              done();
            })
            .on('readable', function () {
              var row;
              while (row = this.read()) {
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
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var length = 350;
      async.series([
        client.connect.bind(client),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
        },
        function (next) {
          async.times(length, function (n, timesNext) {
            var query = 'INSERT INTO %s (id, int_sample, bigint_sample) VALUES (%s, %d, %s)';
            query = util.format(query, table, types.uuid(), n + 1, new types.Long(n, 0x090807).toString());
            client.execute(query, timesNext);
          }, next);
        },
        function (next) {
          var query = util.format('SELECT * FROM %s LIMIT 10000', table);
          var counter = 0;
          var stream = client.stream(query, [], {autoPage: true, fetchSize: 100, prepare: 1, consistency: types.consistencies.quorum})
            .on('end', function () {
              assert.strictEqual(counter, length);
              done();
            })
            .on('readable', function () {
              var row;
              while (row = this.read()) {
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
      var stream = client.stream('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [{}], {prepare: 1});
      var errCalled = false;
      stream
        .on('error', function (err) {
          assert.ok(err);
          assert.ok(err instanceof TypeError, 'Error should be an instance of TypeError');
          errCalled = true;
        })
        .on('end', function () {
          assert.strictEqual(errCalled, true);
          done();
        });
    });
    it('should emit other ResponseErrors', function (done) {
      var client = newInstance();
      //Invalid amount of parameters
      var stream = client.stream('SELECT * FROM system.schema_keyspaces', ['param1'], {prepare: 1});
      var errCalled = false;
      stream
        .on('readable', function () {
          assert.ifError(new Error('It should not be readable'));
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
      var stream = client.stream('SELECT * FROM system.schema_keyspaces', null, {prepare: 1});
      stream.
        on('end', function () {
          assert.strictEqual(allRead, true);
          done();
        })
        .on('error', helper.throwop)
        .on('readable', function () {
          var row;
          var streamContext = this;
          setTimeout(function () {
            //delay all reading
            var row;
            while (row = streamContext.read()) {
              assert.ok(row);
            }
            allRead = true;
          }, 2000);
        });
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}