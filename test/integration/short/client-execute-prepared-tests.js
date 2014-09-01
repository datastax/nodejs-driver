var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepared: 1}, callback)', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should execute a basic query with parameters on all hosts', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = ?';
      async.timesSeries(3, function (n, next) {
        client.execute(query, ['system'], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 3);
          assert.notEqual(result, null);
          assert.notEqual(result.rows, null);
          next();
        });
      }, done);
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance() {
  return new Client(helper.baseOptions);
}