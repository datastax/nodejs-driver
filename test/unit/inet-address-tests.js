'use strict';
const assert = require('assert');
const helper = require('../test-helper');
const utils = require('../../lib/utils');
const InetAddress = require('../../lib/types').InetAddress;

describe('InetAddress', function () {
  describe('constructor', function () {
    it('should validate the Buffer length', function () {
      assert.throws(function () {
        return new InetAddress(utils.allocBufferUnsafe(10));
      }, TypeError);
      assert.throws(function () {
        return new InetAddress(null);
      }, TypeError);
      assert.throws(function () {
        return new InetAddress();
      }, TypeError);
      assert.doesNotThrow(function () {
        return new InetAddress(utils.allocBufferUnsafe(16));
      }, TypeError);
      assert.doesNotThrow(function () {
        return new InetAddress(utils.allocBufferUnsafe(4));
      }, TypeError);
    });
  });
  describe('#toString()', function () {
    it('should convert IPv6 to string representation', function () {
      let val = new InetAddress(utils.allocBufferFromString('aabb0000eeff00112233445566778899', 'hex'));
      assert.strictEqual(val.version, 6);
      assert.strictEqual(val.toString(), 'aabb::eeff:11:2233:4455:6677:8899');
      val = new InetAddress(utils.allocBufferFromString('aabbccddeeff00112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabb:ccdd:eeff:11:2233:4455:6677:8899');
      val = new InetAddress(utils.allocBufferFromString('aabb0000000000112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabb::11:2233:4455:6677:8899');
      val = new InetAddress(utils.allocBufferFromString('aabb0001000100112233445500000000', 'hex'));
      assert.strictEqual(val.toString(), 'aabb:1:1:11:2233:4455::');
      val = new InetAddress(utils.allocBufferFromString('00000000000100112233445500aa00bb', 'hex'));
      assert.strictEqual(val.toString(), '::1:11:2233:4455:aa:bb');
      val = new InetAddress(utils.allocBufferFromString('000000000000000022330000000000bb', 'hex'));
      assert.strictEqual(val.toString(), '::2233:0:0:bb');
      val = new InetAddress(utils.allocBufferFromString('00000000000000000000000000000001', 'hex'));
      assert.strictEqual(val.toString(), '::1');
    });
    it('should convert IPv4 to string representation', function () {
      let val = new InetAddress(utils.allocBufferFromArray([127, 0, 0, 1]));
      assert.strictEqual(val.version, 4);
      assert.strictEqual(val.toString(), '127.0.0.1');
      val = new InetAddress(utils.allocBufferFromArray([198, 168, 1, 1]));
      assert.strictEqual(val.toString(), '198.168.1.1');
      val = new InetAddress(utils.allocBufferFromArray([10, 12, 254, 32]));
      assert.strictEqual(val.toString(), '10.12.254.32');
    });
  });
  describe('#equals()', function () {
    it('should return true when the bytes are the same', function () {
      const hex1 = 'aabb0000eeff00112233445566778899';
      const hex2 = 'ffff0000eeff00112233445566778899';
      const buf1 = utils.allocBufferFromString(hex1, 'hex');
      const val1 = new InetAddress(buf1);
      const val2 = new InetAddress(utils.allocBufferFromString(hex2, 'hex'));
      assert.ok(val1.equals(new InetAddress(buf1)));
      assert.ok(val1.equals(new InetAddress(utils.allocBufferFromString(hex1, 'hex'))));
      assert.ok(!val1.equals(val2));
    });
  });
  describe('fromString()', function () {
    it('should parse IPv6 string representation', function () {
      [
        'aabb::eeff:11:2233:4455:6677:8899',
        'aabb:1:eeff:11:2233:4455:6677:8899',
        'aabb:1:eeff:11:2233:4455:6677:8899',
        '::1:11:2233:4455:aa:bb',
        '::2233:0:0:bb',
        '::1234',
        '10fa::1',
        '3ffe:b00::1:0:0:a', // ensure furthest group is not truncated when preceding groups are longest segment of 0-bytes (NODEJS-255)
        '3ffe:0:0:1::a', // ensure furthest groups of 0-bytes are compressed when longest, but last non-0 bytes group is not truncated.
        '3ffe:0:0:1::' // ensure final groups of 0-bytes are compressed when longest and last group is also 0-bytes.
      ].forEach(function (item) {
        const val = InetAddress.fromString(item);
        helper.assertInstanceOf(val, InetAddress);
        assert.strictEqual(val.toString(), item);
      });
    });
    it('should parse IPv4 string representation', function () {
      let val = InetAddress.fromString('127.0.0.1');
      helper.assertInstanceOf(val, InetAddress);
      assert.strictEqual(val.toString(), '127.0.0.1');
      val = InetAddress.fromString('198.168.1.1');
      helper.assertInstanceOf(val, InetAddress);
      assert.strictEqual(val.toString(), '198.168.1.1');
      val = InetAddress.fromString('10.11.12.13');
      helper.assertInstanceOf(val, InetAddress);
      assert.strictEqual(val.toString(), '10.11.12.13');
    });
    it('should parse IPv4-Mapped IPv6 addresses', function () {
      /* eslint-disable no-multi-spaces */
      [
        ['0:0:0:0:0:FFFF:129.144.52.38',    '00000000000000000000ffff81903426'],
        ['::ffff:129.144.52.38',            '00000000000000000000ffff81903426'],
        ['::ffff:254.255.52.32',            '00000000000000000000fffffeff3420']
      ].forEach(function (item) {
        const ip = InetAddress.fromString(item[0]);
        assert.strictEqual(ip.toString('hex'), item[1]);
      });
      /* eslint-enable no-multi-spaces */
    });
    it('should throw TypeError for invalid IPv6 address with embedded IPv4 address', function () {
      [
        ['0:0:0:0:0:0:129.144.52.38'],
        ['::13.1.68.3'],
        ['fe::13.1.68.3']
      ].forEach(function (address) {
        assert.throws(function () {
          InetAddress.fromString(address);
        }, TypeError);
      });
    });
    it('should throw TypeError when the string is not a valid address', function () {
      assert.throws(function () {
        InetAddress.fromString('127.0.0.1.10');
      }, TypeError);
      assert.throws(function () {
        InetAddress.fromString('127.0.');
      }, TypeError);
      assert.throws(function () {
        InetAddress.fromString(' 127.10.0.1');
      }, TypeError);
      assert.throws(function () {
        InetAddress.fromString('z');
      }, TypeError);
      assert.throws(function () {
        InetAddress.fromString('::Z:11:2233:4455:aa:bb');
      }, TypeError);
      assert.throws(function () {
        InetAddress.fromString(' ::a:11');
      }, TypeError);
    });
  });
});