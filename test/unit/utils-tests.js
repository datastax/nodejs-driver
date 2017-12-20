"use strict";
const assert = require('assert');
const utils = require('../../lib/utils');
const helper = require('../test-helper');

describe('utils', function () {
  describe('timesLimit()', function () {
    it('should handle sync and async functions', function (done) {
      utils.timesLimit(5, 10, function (i, next) {
        if (i === 0) {
          return setImmediate(next);
        }
        next();
      }, done);
    });
  });
  describe('allocBuffer', function () {
    it('should return a buffer filled with zeros', function () {
      var b = utils.allocBuffer(256);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.length, 256);
      for (let i = 0; i < b.length; i++) {
        assert.strictEqual(b[i], 0);
      }
    });
  });
  describe('allocBufferUnsafe', function () {
    it('should return a Buffer with the correct length', function () {
      var b = utils.allocBuffer(256);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.length, 256);
    });
  });
  describe('allocBufferFromString', function () {
    it('should throw TypeError when the first parameter is not a string', function () {
      if (typeof Buffer.from === 'undefined') {
        // Our method validates for a string
        assert.throws(function () {
          utils.allocBufferFromString([]);
        }, TypeError);
      }
      assert.throws(function () {
        utils.allocBufferFromString(null);
      }, TypeError);
      assert.throws(function () {
        utils.allocBufferFromString(undefined);
      }, TypeError);
      assert.throws(function () {
        utils.allocBufferFromString(100);
      }, TypeError);
    });
    it('should return a Buffer representing the string value', function () {
      var text = 'Hello safe buffer';
      var b = utils.allocBufferFromString(text);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.toString(), text);
    });
  });
  describe('allocBufferFromArray', function () {
    it('should throw TypeError when the first parameter is not a string', function () {
      if (typeof Buffer.from === 'undefined') {
        // Our method validates for an Array instance
        assert.throws(function () {
          utils.allocBufferFromArray('hello');
        }, TypeError);
      }
      assert.throws(function () {
        utils.allocBufferFromArray(null);
      }, TypeError);
      assert.throws(function () {
        utils.allocBufferFromArray(undefined);
      }, TypeError);
      assert.throws(function () {
        utils.allocBufferFromArray(100);
      }, TypeError);
    });
    it('should return a Buffer representing the string value', function () {
      var arr = [ 0xff, 0, 0x1a ];
      var b = utils.allocBufferFromArray(arr);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.length, arr.length);
      for (let i = 0; i < b.length; i++) {
        assert.strictEqual(b[i], arr[i]);
      }
    });
  });
});