var assert = require('assert');
var util = require('util');
var events = require('events');
var utils = require('../../lib/utils');
var helper = require('../test-helper');

var Uuid = require('../../lib/types').Uuid;
var TimeUuid = require('../../lib/types').TimeUuid;

describe('Uuid', function () {
  describe('constructor', function () {
    it('should validate the Buffer length', function () {
      assert.throws(function () {
        new Uuid(new Buffer(10));
      });
      assert.throws(function () {
        new Uuid(null);
      });
      assert.throws(function () {
        new Uuid();
      });
      assert.doesNotThrow(function () {
        new Uuid(new Buffer(16));
      });
    });
  });
  describe('#toString()', function () {
    it('should convert to string representation', function () {
      var val = new Uuid(new Buffer('aabbccddeeff00112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'aabbccdd-eeff-0011-2233-445566778899');
      val = new Uuid(new Buffer('a1b1ccddeeff00112233445566778899', 'hex'));
      assert.strictEqual(val.toString(), 'a1b1ccdd-eeff-0011-2233-445566778899');
      val = new Uuid(new Buffer('ffb1ccddeeff00112233445566778800', 'hex'));
      assert.strictEqual(val.toString(), 'ffb1ccdd-eeff-0011-2233-445566778800');
    });
  });
  describe('#equals', function () {
    it('should return true only when the values are equal', function () {
      var val = new Uuid(new Buffer('aabbccddeeff00112233445566778899', 'hex'));
      var val2 = new Uuid(new Buffer('ffffffffffff00000000000000000000', 'hex'));
      var val3 = new Uuid(new Buffer('ffffffffffff00000000000000000001', 'hex'));
      assert.strictEqual(val.equals(val), true);
      assert.strictEqual(val.equals(new Uuid(new Buffer('aabbccddeeff00112233445566778899', 'hex'))), true);
      assert.strictEqual(val.equals(val2), false);
      assert.strictEqual(val.equals(val3), false);
      assert.strictEqual(val2.equals(val3), false);
      assert.strictEqual(val3.equals(val2), false);
    });
  });
  describe('fromString()', function () {
    it('should validate that the string', function () {
      assert.throws(function () {
        Uuid.fromString('22');
      });
      assert.throws(function () {
        Uuid.fromString(null);
      });
      assert.throws(function () {
        Uuid.fromString();
      });
      assert.throws(function () {
        Uuid.fromString('zzb1ccdd-eeff-0011-2233-445566778800');
      });
      assert.doesNotThrow(function () {
        Uuid.fromString('acb1ccdd-eeff-0011-2233-445566778813');
      });
    });
    it('should contain a valid internal representation', function () {
      var val = Uuid.fromString('acb1ccdd-eeff-0011-2233-445566778813');
      assert.strictEqual(val.buffer.toString('hex'), 'acb1ccddeeff00112233445566778813');
      val = Uuid.fromString('ffffccdd-eeff-0022-2233-445566778813');
      assert.strictEqual(val.buffer.toString('hex'), 'ffffccddeeff00222233445566778813');
    });
  });
  describe('toBuffer()', function () {
    it('should return the Buffer representation', function () {
      var buf = new Buffer(16);
      var val = new Uuid(buf);
      assert.strictEqual(Uuid.toBuffer(val).toString('hex'), buf.toString('hex'));
      buf = new Buffer('ffffccddeeff00222233445566778813', 'hex');
      val = new Uuid(buf);
      assert.strictEqual(Uuid.toBuffer(val).toString('hex'), buf.toString('hex'));
    });
  });
  describe('random()', function () {
    it('should return a Uuid instance', function () {
      helper.assertInstanceOf(Uuid.random(), Uuid);
    });
    it('should contain the version bits and IETF variant', function () {
      var val = Uuid.random();
      assert.strictEqual(val.toString().charAt(14), '4');
      assert.ok(['8', '9', 'a', 'b'].indexOf(val.toString().charAt(19)) >= 0);
      val = Uuid.random();
      assert.strictEqual(val.toString().charAt(14), '4');
      assert.ok(['8', '9', 'a', 'b'].indexOf(val.toString().charAt(19)) >= 0);
      val = Uuid.random();
      assert.strictEqual(val.toString().charAt(14), '4');
      assert.ok(['8', '9', 'a', 'b'].indexOf(val.toString().charAt(19)) >= 0);
    });
    it('should generate v4 Uuid that do not collide', function () {
      var values = {};
      var length = 100000;
      for (var i = 0; i < length; i++) {
        values[Uuid.random().toString()] = true;
      }
      assert.strictEqual(Object.keys(values).length, length);
    });
  });
});
describe('TimeUuid', function () {
  describe('constructor()', function () {
    it('should generate based on the parameters', function () {
      //Gregorian calendar epoch
      var val = new TimeUuid(new Date(-12219292800000), 0, new Buffer([0,0,0,0,0,0]), new Buffer([0,0]));
      assert.strictEqual(val.toString(), '00000000-0000-1000-8000-000000000000');
      val = new TimeUuid(new Date(-12219292800000 + 1000), 0, new Buffer([0,0,0,0,0,0]), new Buffer([0,0]));
      assert.strictEqual(val.toString(), '00989680-0000-1000-8000-000000000000');
      //unix  epoch
      val = new TimeUuid(new Date(0), 0, new Buffer([0,0,0,0,0,0]), new Buffer([0,0]));
      assert.strictEqual(val.toString(), '13814000-1dd2-11b2-8000-000000000000');
    });
  });
});