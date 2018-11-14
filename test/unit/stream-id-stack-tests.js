/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');
const rewire = require('rewire');

describe('StreamIdStack', function () {
  this.timeout(2000);
  const osPrecision = 30;
  it('should pop and push', function () {
    const stack = newInstance();
    assert.strictEqual(stack.pop(), 0);
    assert.strictEqual(stack.pop(), 1);
    assert.strictEqual(stack.pop(), 2);
    assert.strictEqual(stack.inUse, 3);
    //1 becomes available again
    stack.push(1);
    assert.strictEqual(stack.inUse, 2);
    assert.strictEqual(stack.pop(), 1);
    stack.clear();
  });
  it('should not use more than allowed by the protocol version', function () {
    [
      [1, 128],
      [2, 128],
      [3, Math.pow(2, 15)]
    ].forEach(function (value) {
      const version = value[0];
      const maxSize = value[1];
      const stack = newInstance(version);
      const ids = pop(stack, maxSize + 20);
      assert.strictEqual(ids.length, maxSize + 20);
      for (let i = 0; i < maxSize + 20; i++) {
        if (i < maxSize) {
          assert.strictEqual(ids[i], i);
        }
        else {
          assert.strictEqual(ids[i], null);
        }
      }
      stack.push(5);
      assert.strictEqual(stack.inUse, maxSize - 1);
      stack.clear();
    });
  });
  it('should yield the lowest available id', function () {
    const stack = newInstance(3);
    const ids = pop(stack, 128 * 3 - 1);
    assert.strictEqual(ids.length, 128 * 3 - 1);
    for (let i = 0; i < ids.length; i++) {
      assert.strictEqual(ids[i], i);
    }
    //popped all from the first 3 groups
    assert.strictEqual(stack.pop(), 128 * 3 - 1);
    assert.strictEqual(stack.groups.length, 3);
    stack.push(10); //group 0
    stack.push(200); //group 1
    assert.strictEqual(stack.pop(), 10);
    assert.strictEqual(stack.pop(), 200);
    assert.strictEqual(stack.pop(), 128 * 3);
    stack.push(200);
    assert.strictEqual(stack.pop(), 200);
    assert.strictEqual(stack.groups.length, 4);
    stack.clear();
  });
  it('should release unused groups', function (done) {
    const releaseDelay = 100;
    const stack = newInstance(3, releaseDelay);
    //6 groups,
    const length = 128 * 5 + 2;

    // Verify that the events are emitted
    let inFlight = 0;
    stack.on('inFlightIncrease', () => inFlight++);
    stack.on('inFlightDecrease', n => inFlight -= n);

    pop(stack, length);

    assert.strictEqual(stack.inUse, length);
    assert.strictEqual(inFlight, length);

    // return just 1 to the last group
    stack.push(128 * 5);
    assert.strictEqual(stack.groups.length, 6);

    //the last group is completed and can be released
    stack.push(128 * 5 + 1);

    // push 10 more
    push(stack, 0, 10);

    assert.strictEqual(stack.inUse, inFlight);

    assert.strictEqual(stack.groups.length, 6);
    setTimeout(function () {
      //there should be 5 groups now
      assert.strictEqual(stack.groups.length, 5);

      assert.strictEqual(stack.inUse, length - 12);
      assert.strictEqual(inFlight, length - 12);

      stack.clear();
      done();
    }, releaseDelay + osPrecision);
  });
  it('should not release the current group', function (done) {
    const releaseDelay = 100;
    const stack = newInstance(3, releaseDelay);

    let inFlight = 0;
    stack.on('inFlightIncrease', () => inFlight++);
    stack.on('inFlightDecrease', n => inFlight -= n);

    //6 groups,
    pop(stack, 128 * 5 + 2);
    //return just 1 to the last group
    stack.push(128 * 5);
    assert.strictEqual(stack.groups.length, 6);
    //the last group is completed and but can not be released as is the current one
    stack.push(128 * 5 + 1);
    assert.strictEqual(stack.groups.length, 6);
    setTimeout(function () {
      //there should be 5 groups now
      assert.strictEqual(stack.groups.length, 6);
      assert.strictEqual(stack.inUse, inFlight);
      stack.clear();
      done();
    }, releaseDelay + osPrecision);
  });
  it('should not release more than release size per time', function (done) {
    const releaseDelay = 100;
    const stack = newInstance(3, releaseDelay);
    //12 groups,
    pop(stack, 128 * 11 + 2);
    assert.strictEqual(stack.groups.length, 12);
    //return
    push(stack, 0, 128 * 11 + 2);
    assert.strictEqual(stack.groups.length, 12);
    assert.strictEqual(stack.groupIndex, 0);
    assert.strictEqual(stack.inUse, 0);
    assert.strictEqual(stack.pop(), 127); //last from the first group
    assert.strictEqual(stack.inUse, 1);
    setTimeout(function () {
      //there should be 12 - 4 groups now
      assert.strictEqual(stack.groups.length, 8);
    }, releaseDelay + osPrecision);
    setTimeout(function () {
      //there should be 12 - 8 groups now
      assert.strictEqual(stack.groups.length, 4);
      stack.clear();
      done();
    }, releaseDelay * 2 + osPrecision);
  });
});

/** @returns {StreamIdStack}  */
function newInstance(version, releaseDelay) {
  const StreamIdStack = rewire('../../lib/stream-id-stack');
  StreamIdStack.__set__("releaseDelay", releaseDelay || 100);
  return new StreamIdStack(version || 3);
}

function pop(stack, n) {
  const arr = [];
  for (let i = 0; i < n; i++) {
    arr.push(stack.pop());
  }
  return arr;
}

function push(stack, initialValue, length) {
  if (util.isArray(initialValue)) {
    initialValue.forEach(stack.push.bind(stack));
    return;
  }
  for (let i = 0; i < length; i++) {
    stack.push(initialValue + i);
  }
}