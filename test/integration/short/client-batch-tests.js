var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');
describe('Client', function () {
  this.timeout(120000);
  describe('#batch() @c2_0', function () {
    var keyspace = helper.getRandomName('ks');
    var table1 = keyspace + '.' + helper.getRandomName('tblA');
    var table2 = keyspace + '.' + helper.getRandomName('tblB');
    before(function (done) {
      var client = newInstance();
      async.series([
        helper.ccmHelper.start(1),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table2), next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should execute a batch of queries with no params', function (done) {
      var insertQuery = 'INSERT INTO %s (id, text_sample) VALUES (%s, \'%s\')';
      var selectQuery = 'SELECT * FROM %s WHERE id = %s';
      var id1 = types.uuid();
      var id2 = types.uuid();
      var client = newInstance();
      var queries = [
        util.format(insertQuery, table1, id1, 'one'),
        util.format(insertQuery, table2, id2, 'two')
      ];
      async.series([
        function (next) {
          client.batch(queries, next);
        },
        function assertValue1(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'one');
            next();
          });
        },
        function assertValue2(next) {
          client.execute(util.format(selectQuery, table2, id2), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].text_sample, 'two');
            next();
          });
        }
      ], done);
    });
    it('should execute a batch of queries with params', function (done) {
      var insertQuery = 'INSERT INTO %s (id, double_sample) VALUES (?, ?)';
      var selectQuery = 'SELECT * FROM %s WHERE id = %s';
      var id1 = types.uuid();
      var id2 = types.uuid();
      var client = newInstance();
      var queries = [
        {query: util.format(insertQuery, table1), params: [id1, 1000]},
        {query: util.format(insertQuery, table2), params: [id2, 2000.2]}
      ];
      async.series([
        function (next) {
          client.batch(queries, next);
        },
        function assertValue1(next) {
          client.execute(util.format(selectQuery, table1, id1), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.equal(result.rows[0].text_sample, null);
            assert.equal(result.rows[0].double_sample, 1000);
            next();
          });
        },
        function assertValue2(next) {
          client.execute(util.format(selectQuery, table2, id2), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rows);
            assert.strictEqual(result.rows[0].double_sample, 2000.2);
            next();
          });
        }
      ], done);
    });
    it('should callback with error when there is a ResponseError', function (done) {
      var client = newInstance();
      client.batch(['INSERT WILL FAIL'], function (err) {
        assert.ok(err);
        assert.ok(err instanceof errors.ResponseError);
        done();
      });
    });
    it('should validate the arguments are valid', function (done) {
      var client = newInstance();
      assert.throws(function () {
          client.batch();
        },
        null,
        'It should throw an Error when executeBatch is called with less than 2 arguments'
      );
      assert.throws(function () {
          client.batch(['SELECT'], {});
        },
        null,
        'It should throw an Error when the callback is not specified'
      );
      assert.throws(function () {
          client.batch({}, {}, function () {});
        },
        null,
        'It should throw an Error when queries argument is not an Array'
      );

      //it should not throw an error with the following arguments
      var query = util.format('INSERT INTO %s (id, int_sample) VALUES (?, ?)', table1);
      async.series([
        function (next) {
          client.batch([{query: query, params: [types.uuid(), null]}], next);
        },
        function (next) {
          client.batch(
            [{query: query, params: [types.uuid(), null]}],
            {logged: false, consistency: types.consistencies.quorum},
            next);
        }
      ], done);
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}