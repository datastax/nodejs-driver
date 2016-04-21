var assert = require('assert');
var util = require('util');
var events = require('events');
var async = require('neo-async');
var dns = require('dns');

var addressResolution = require('../../lib/policies/address-resolution');
var EC2MultiRegionTranslator = addressResolution.EC2MultiRegionTranslator;

describe('EC2MultiRegionTranslator', function () {
  this.timeout(5000);
  describe('#translate()', function () {
    it('should return the same address when it could not be resolved', function (done) {
      var t = new EC2MultiRegionTranslator();
      t.translate('127.100.100.1', 9042, function (endPoint) {
        assert.strictEqual(endPoint, '127.100.100.1:9042');
        done();
      });
    });
    it('should do a reverse and a forward dns lookup', function (done) {
      var t = new EC2MultiRegionTranslator();
      dns.lookup('datastax.com', function (err, address) {
        assert.ifError(err);
        assert.ok(address);
        t.translate(address, 9001, function (endPoint) {
          assert.strictEqual(endPoint, address + ':9001');
          done();
        });
      });
    });
  });
});