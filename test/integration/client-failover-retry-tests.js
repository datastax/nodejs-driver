var assert = require('assert');

var Client = require('../../index.js').Client;

describe('Client', function () {
  describe('#execute()', function () {
    //CMM 2
    it('should execute a basic query', function (done) {
      var options = {contactPoints: ['127.0.0.1']};
      var client = new Client(options);
      client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
        assert.equal(ok, null);
        assert.notEqual(result, null);
      });
    });
  });
});