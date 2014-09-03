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
    it('should use parameter hints');
//    , function (done) {
//      var query = util.format('INSERT INTO %s (id, text_sample, float_sample) VALUES (?, ?, ?)', table);
//      var args = [types.uuid(), 'text sample', 1000.1];
//      var hints = [types.dataTypes.uuid, types.dataTypes.text, types.dataTypes.float];
//      var client = newInstance();
//      client.execute(query, args, {hints: hints}, done);
//    });
  });
});