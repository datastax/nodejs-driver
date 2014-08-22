var assert = require('assert');

var helper = require('../test-helper.js');
var ControlConnection = require('../../lib/control-connection.js');

describe('ControlConnection', function () {
  describe('#init()', function () {
    //CMM 2
    it('should retrieve local host and peers', function (done) {
      var cc = new ControlConnection(helper.baseOptions);
      cc.init(function () {
        assert.equal(cc.hosts.length, 2);
        cc.hosts.forEach(function (h) {
          assert.ok(h.datacenter);
          assert.ok(h.rack);
        });
        done();
      });
    });
  });
});