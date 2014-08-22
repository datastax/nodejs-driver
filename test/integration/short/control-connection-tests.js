var assert = require('assert');

var helper = require('../../test-helper.js');
var ControlConnection = require('../../../lib/control-connection.js');

describe('ControlConnection', function () {
  this.timeout(120000);
  describe('#init()', function () {
    before(helper.ccmHelper.start(2));
    after(helper.ccmHelper.remove);
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