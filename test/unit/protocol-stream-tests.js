/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');

var Protocol = require('../../lib/streams').Protocol;
var types = require('../../lib/types');

describe('Protocol', function () {
  it('should emit a single frame with 0-length body', function () {
    var p = newInstance();
    var items = [];
    p.on('readable', function () {
      var item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    var buffer = generateBuffer(4, [ 0 ]);
    p.readItems(buffer);
    assert.strictEqual(items.length, 1);
    assert.strictEqual(items[0].header.bodyLength, 0);
  });
  it('should emit a single frame with 0-length body chunked', function () {
    var p = newInstance();
    var items = [];
    p.on('readable', function () {
      var item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    var buffer = generateBuffer(4, [ 0 ]);
    p.readItems(buffer.slice(0, 2));
    p.readItems(buffer.slice(2));
    assert.strictEqual(items.length, 1);
    assert.strictEqual(items[0].header.bodyLength, 0);
  });
  it('should emit multiple frames from a single chunk', function () {
    var p = newInstance();
    var items = [];
    p.on('readable', function () {
      var item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    var bodyLengths = [ 0, 10, 0, 20, 30, 0];
    var buffer = generateBuffer(4, bodyLengths);
    p.readItems(buffer);
    assert.strictEqual(items.length, bodyLengths.length);
    bodyLengths.forEach(function (length, index) {
      assert.strictEqual(items[index].header.bodyLength, length);
    });
  });
  it('should emit multiple frames from multiples chunks', function () {
    var p = newInstance();
    var items = {};
    p.on('readable', function () {
      var item;
      while ((item = p.read())) {
        items[item.header.streamId] = items[item.header.streamId] || [];
        items[item.header.streamId].push(item);
      }
    });
    var bodyLengths = [ 0, 10, 0, 20, 30, 0];
    var buffer = generateBuffer(4, bodyLengths);
    p.readItems(buffer.slice(0, 9));
    p.readItems(buffer.slice(9, 31));
    p.readItems(buffer.slice(31, 33));
    p.readItems(buffer.slice(33, 45));
    p.readItems(buffer.slice(45, 65));
    p.readItems(buffer.slice(65));
    bodyLengths.forEach(function (length, index) {
      var item = items[index];
      assert.ok(item);
      assert.ok(item.length);
      var sumLength = item.reduce(function (previousValue, subItem) {
        return previousValue + subItem.chunk.length - subItem.offset;
      }, 0);
      assert.ok(sumLength >= length, sumLength + ' >= ' + length + ' failed');
    });
  });
  it('should emit multiple frames from multiples small chunks', function () {
    var p = newInstance();
    var items = {};
    p.on('readable', function () {
      var item;
      while ((item = p.read())) {
        items[item.header.streamId] = items[item.header.streamId] || [];
        items[item.header.streamId].push(item);
      }
    });
    var bodyLengths = [ 0, 10, 15, 15, 20, 12, 0, 6];
    var buffer = generateBuffer(4, bodyLengths);
    for (var i = 0; i < buffer.length; i = i + 2) {
      if (i + 2 > buffer.length) {
        p.readItems(buffer.slice(i));
        break;
      }
      p.readItems(buffer.slice(i, i + 2));
    }
    bodyLengths.forEach(function (length, index) {
      var item = items[index];
      assert.ok(item);
      assert.ok(item.length);
      var sumLength = item.reduce(function (previousValue, subItem) {
        return previousValue + subItem.chunk.length - subItem.offset;
      }, 0);
      assert.ok(sumLength >= length, sumLength + ' >= ' + length + ' failed');
    });
  });
});

/**
 * @param {Number} version
 * @param {Array.<Number>} frameBodyLengths
 * @returns {Buffer}
 */
function generateBuffer(version, frameBodyLengths) {
  var buffers = frameBodyLengths.map(function (bodyLength, index) {
    var header = new types.FrameHeader(version, 0, index, 0, bodyLength);
    return Buffer.concat([ header.toBuffer(), new Buffer(bodyLength) ]);
  });
  return Buffer.concat(buffers);
}

/** @returns {Protocol} */
function newInstance() {
  return new Protocol({ objectMode: true });
}