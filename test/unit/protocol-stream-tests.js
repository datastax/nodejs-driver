/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');

const Protocol = require('../../lib/streams').Protocol;
const types = require('../../lib/types');
const utils = require('../../lib/utils');

describe('Protocol', function () {
  it('should emit a single frame with 0-length body', function (done) {
    const p = newInstance();
    const items = [];
    p.on('readable', function () {
      let item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    const buffer = generateBuffer(4, [ 0 ]);
    p.readItems(buffer);
    process.nextTick(() => {
      assert.strictEqual(items.length, 1);
      assert.strictEqual(items[0].header.bodyLength, 0);
      done();
    });
  });
  it('should emit a single frame with 0-length body chunked', function (done) {
    const p = newInstance();
    const items = [];
    p.on('readable', function () {
      let item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    const buffer = generateBuffer(4, [ 0 ]);
    p.readItems(buffer.slice(0, 2));
    p.readItems(buffer.slice(2));
    process.nextTick(() => {
      assert.strictEqual(items.length, 1);
      assert.strictEqual(items[0].header.bodyLength, 0);
      done();
    });
  });
  it('should emit multiple frames from a single chunk', function (done) {
    const p = newInstance();
    const items = [];
    p.on('readable', function () {
      let item;
      while ((item = p.read())) {
        items.push(item);
      }
    });
    const bodyLengths = [ 0, 10, 0, 20, 30, 0];
    const buffer = generateBuffer(4, bodyLengths);
    p.readItems(buffer);
    process.nextTick(() => {
      assert.strictEqual(items.length, bodyLengths.length);
      bodyLengths.forEach(function (length, index) {
        assert.strictEqual(items[index].header.bodyLength, length);
      });
      done();
    });
  });
  it('should emit multiple frames from multiples chunks', function (done) {
    const p = newInstance();
    const items = {};
    p.on('readable', function () {
      let item;
      while ((item = p.read())) {
        items[item.header.streamId] = items[item.header.streamId] || [];
        items[item.header.streamId].push(item);
      }
    });
    const bodyLengths = [ 0, 10, 0, 20, 30, 0];
    const buffer = generateBuffer(4, bodyLengths);
    p.readItems(buffer.slice(0, 9));
    p.readItems(buffer.slice(9, 31));
    p.readItems(buffer.slice(31, 33));
    p.readItems(buffer.slice(33, 45));
    p.readItems(buffer.slice(45, 65));
    p.readItems(buffer.slice(65));
    process.nextTick(() => {
      bodyLengths.forEach(function (length, index) {
        const item = items[index];
        assert.ok(item);
        assert.ok(item.length);
        const sumLength = item.reduce(function (previousValue, subItem) {
          return previousValue + subItem.chunk.length - subItem.offset;
        }, 0);
        assert.ok(sumLength >= length, sumLength + ' >= ' + length + ' failed');
      });
      done();
    });
  });
  it('should emit multiple frames from multiples small chunks', function (done) {
    const p = newInstance();
    const items = {};
    p.on('readable', function () {
      let item;
      while ((item = p.read())) {
        items[item.header.streamId] = items[item.header.streamId] || [];
        items[item.header.streamId].push(item);
      }
    });
    const bodyLengths = [ 0, 10, 15, 15, 20, 12, 0, 6];
    const buffer = generateBuffer(4, bodyLengths);
    for (let i = 0; i < buffer.length; i = i + 2) {
      if (i + 2 > buffer.length) {
        p.readItems(buffer.slice(i));
        break;
      }
      p.readItems(buffer.slice(i, i + 2));
    }
    process.nextTick(() => {
      bodyLengths.forEach(function (length, index) {
        const item = items[index];
        assert.ok(item);
        assert.ok(item.length);
        const sumLength = item.reduce(function (previousValue, subItem) {
          return previousValue + subItem.chunk.length - subItem.offset;
        }, 0);
        assert.ok(sumLength >= length, sumLength + ' >= ' + length + ' failed');
      });
      done();
    });
  });
});

/**
 * @param {Number} version
 * @param {Array.<Number>} frameBodyLengths
 * @returns {Buffer}
 */
function generateBuffer(version, frameBodyLengths) {
  const buffers = frameBodyLengths.map(function (bodyLength, index) {
    const header = new types.FrameHeader(version, 0, index, 0, bodyLength);
    return Buffer.concat([ header.toBuffer(), utils.allocBuffer(bodyLength) ]);
  });
  return Buffer.concat(buffers);
}

/** @returns {Protocol} */
function newInstance() {
  return new Protocol({ objectMode: true });
}