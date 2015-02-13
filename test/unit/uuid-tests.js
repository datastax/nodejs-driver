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
    it('should generate v4 uuids that do not collide', function () {
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
      val = new TimeUuid(new Date(0), 0, new Buffer([255,255,255,255,255,255]), new Buffer([255,255]));
      assert.strictEqual(val.toString(), '13814000-1dd2-11b2-bfff-ffffffffffff');
      val = new TimeUuid(new Date(0), 0, new Buffer([1,1,1,1,1,1]), new Buffer([1,1]));
      assert.strictEqual(val.toString(), '13814000-1dd2-11b2-8101-010101010101');

      val = new TimeUuid(new Date('2015-01-10 5:05:05 GMT+0000'), 0, new Buffer([1,1,1,1,1,1]), new Buffer([1,1]));
      assert.strictEqual(val.toString(), '3d555680-9886-11e4-8101-010101010101');
    });
  });
  describe('#getDatePrecision()', function () {
    it('should get the Date and ticks of the Uuid representation', function () {
      var date = new Date();
      var val = new TimeUuid(date, 1);
      assert.strictEqual(val.getDatePrecision().ticks, 1);
      assert.strictEqual(val.getDatePrecision().date.getTime(), date.getTime());

      date = new Date('2015-02-13 06:07:08.450');
      val = new TimeUuid(date, 699);
      assert.strictEqual(val.getDatePrecision().ticks, 699);
      assert.strictEqual(val.getDatePrecision().date.getTime(), date.getTime());
      assert.strictEqual(val.getDate().getTime(), date.getTime());
    });
  });
  describe('#getNodeId()', function () {
    it('should get the node id of the Uuid representation', function () {
      var val = new TimeUuid(new Date(), 0, new Buffer([1, 2, 3, 4, 5, 6]));
      helper.assertInstanceOf(val.getNodeId(), Buffer);
      assert.strictEqual(val.getNodeId().toString('hex'), '010203040506');
      val = new TimeUuid(new Date(), 0, 'host01');
      assert.strictEqual(val.getNodeIdString(), 'host01');
      val = new TimeUuid(new Date(), 0, 'h12288');
      assert.strictEqual(val.getNodeIdString(), 'h12288');
    });
  });
  describe('fromDate()', function () {
    it('should generate v1 uuids that do not collide', function () {
      var values = {};
      var length = 50000;
      var date = new Date();
      for (var i = 0; i < length; i++) {
        values[TimeUuid.fromDate(date).toString()] = true;
      }
      assert.strictEqual(Object.keys(values).length, length);
    });
    it('should collide exactly at 10001 if date but not the ticks are specified', function () {
      var values = {};
      var length = 10000;
      var date = new Date();
      for (var i = 0; i < length; i++) {
        values[TimeUuid.fromDate(date, null, 'host01', 'AA').toString()] = true;
      }
      assert.strictEqual(Object.keys(values).length, length);
      //next should collide
      assert.strictEqual(values[TimeUuid.fromDate(date, null, 'host01', 'AA').toString()], true);
    });
  });
  describe('now()', function () {
    it('should pass the nodeId when provided', function () {
      var val = TimeUuid.now('h12345');
      assert.strictEqual(val.getNodeIdString(), 'h12345');
    });
    it('should use current date', function () {
      var date = new Date();
      var val = TimeUuid.now();
      assert.strictEqual(val.getDate().getTime(), date.getTime());
    });
  });
});