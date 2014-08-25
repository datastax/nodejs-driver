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
    it('should subscribe to STATUS_CHANGE events', function (done) {
      //This test brings nodes down
      var cc = new ControlConnection(helper.baseOptions);
      cc.init(function () {
        helper.ccmHelper.exec(['node2', 'stop'], function (err) {
          if (err) return done(err);
          //setting host as down
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 2);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 1);
            done();
          }, 3000);
        });
      });
    });
  });
});