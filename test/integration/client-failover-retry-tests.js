var assert = require('assert');
var async = require('async');

var Client = require('../../index.js').Client;
var ip = '127.0.0.1';

describe('Client', function () {
  describe('#connect()', function () {
    it('should allow multiple parallel calls', function (done) {
      async.times(1, function (n, next) {
        var options = {contactPoints: [ip]};
        var client = new Client(options);
        client.connect(next);
      }, done);
    });
  });
  describe('#execute()', function () {
    //CMM 2
    it('should execute a basic query', function (done) {
      var options = {contactPoints: [ip]};
      var client = new Client(options);
      client.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
        assert.equal(err, null);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        done();
      });
    });
    it('should callback with syntax error', function (done) {
      var options = {contactPoints: [ip]};
      var client = new Client(options);
      client.execute('SELECT WILL FAIL', function (err, result) {
        assert.notEqual(err, null);
        assert.equal(result, null);
        done();
      });
    });
  });
});