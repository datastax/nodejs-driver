var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');


/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 0}, callback)', function () {
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    before(function (done) {
      var client = newInstance();
      async.series([
        helper.ccmHelper.start(1),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table), next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should guess known types', function (done) {
      var client = newInstance();
      var columns = 'id, text_sample, double_sample, timestamp_sample, blob_sample, list_sample';
      //a precision a float32 can represent
      var values = [types.uuid(), 'text sample 1', 133, new Date(121212211), new Buffer(100), ['one', 'two']];
      //no hint
      insertSelectTest(client, table, columns, values, null, done);
    });
    it('should use parameter hints as number for simple types', function (done) {
      var client = newInstance();
      var columns = 'id, text_sample, float_sample, int_sample';
      //a precision a float32 can represent
      var values = [types.uuid(), 'text sample', 1000.0999755859375, -12];
      var hints = [types.dataTypes.uuid, types.dataTypes.text, types.dataTypes.float, types.dataTypes.int];
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for simple types', function (done) {
      var columns = 'id, text_sample, float_sample, int_sample';
      var values = [types.uuid(), 'text sample', -9, 1];
      var hints = [null, 'text', 'float', 'int'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for complex types partial', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.uuid(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      var hints = [null, 'map', 'list', 'set'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    it('should use parameter hints as string for complex types complete', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.uuid(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      //complete info
      var hints = [null, 'map<text, text>', 'list<text>', 'set<text>'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
  });
});

function insertSelectTest(client, table, columns, values, hints, done) {
  async.series([
    function (next) {
      var markers = '?';
      var columnsSplit = columns.split(',');
      for (var i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      var query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 0, hints: hints}, next);
    },
    function (next) {
      var query = util.format('SELECT %s FROM %s WHERE id = %s', columns, table, values[0]);
      client.execute(query, null, {prepare: 0}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        var row = result.rows[0];
        for (var i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}