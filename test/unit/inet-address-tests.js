var assert = require('assert');
var util = require('util');
var events = require('events');

var utils = require('../../lib/utils');
var helper = require('../test-helper');
var InetAddress = require('../../lib/types').InetAddress;

describe('InetAddress', function () {
  describe('constructor', function () {
    it('should validate the Buffer length', function () {
      assert.throws(function () {
        new InetAddress(new Buffer(10));
      });
      assert.throws(function () {
        new InetAddress(null);
      });
      assert.throws(function () {
        new InetAddress();
      });
      assert.doesNotThrow(function () {
        new InetAddress(new Buffer(16));
      });
      assert.doesNotThrow(function () {
        new InetAddress(new Buffer(4));
      });
    });
  });
  describe('#toString()', function () {
    it('should convert IPv6 to string representation', function () {
      var val = new InetAddress(new Buffer('aabb0000eeff00112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabb::eeff:0011:2233:4455:6677:8899');
      val = new InetAddress(new Buffer('aabbccddeeff00112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabb:ccdd:eeff:0011:2233:4455:6677:8899');
      val = new InetAddress(new Buffer('aabb0000000000112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabb::0011:2233:4455:6677:8899');
      val = new InetAddress(new Buffer('aabb0001000100112233445500000000', 'hex'));
      assert.strictEqual(val.toString(), 'aabb:0001:0001:0011:2233:4455::');
      val = new InetAddress(new Buffer('00000000000100112233445500aa00bb', 'hex'));
      assert.strictEqual(val.toString(), '::0001:0011:2233:4455:00aa:00bb');
      val = new InetAddress(new Buffer('000000000000000022330000000000bb', 'hex'));
      assert.strictEqual(val.toString(), '::2233:0000:0000:00bb');
    });
    it('should convert IPv4 to string representation', function () {
      var val = new InetAddress(new Buffer([127, 0, 0, 1]));
      assert.strictEqual(val.toString(), '127.0.0.1');
      val = new InetAddress(new Buffer([198, 168, 1, 1]));
      assert.strictEqual(val.toString(), '198.168.1.1');
      val = new InetAddress(new Buffer([10, 12, 254, 32]));
      assert.strictEqual(val.toString(), '10.12.254.32');
    });
  });
  describe('#equals()', function () {
    it('should return true when the bytes are the same', function () {
      var hex1 = 'aabb0000eeff00112233445566778899';
      var hex2 = 'ffff0000eeff00112233445566778899';
      var buf1 = new Buffer(hex1, 'hex');
      var val1 = new InetAddress(buf1);
      var val2 = new InetAddress(new Buffer(hex2, 'hex'));
      assert.ok(val1.equals(new InetAddress(buf1)));
      assert.ok(val1.equals(new InetAddress(new Buffer(hex1, 'hex'))));
      assert.ok(!val1.equals(val2));
    });
  });
});