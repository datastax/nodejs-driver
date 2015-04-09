var assert = require('assert');
var util = require('util');
var rewire = require('rewire');

var utils = require('../../lib/utils');
var helper = require('../test-helper');

describe('StreamIdStack', function () {
  this.timeout(2000);
  it('should pop and push', function () {
    var stack = newInstance();
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
        var version = value[0];
        var maxSize = value[1];
        var stack = newInstance(version);
        var ids = pop(stack, maxSize + 20);
        assert.strictEqual(ids.length, maxSize + 20);
        for (var i = 0; i < maxSize + 20; i++) {
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
    var stack = newInstance(3);
    var ids = pop(stack, 128 * 3 - 1);
    assert.strictEqual(ids.length, 128 * 3 - 1);
    for (var i = 0; i < ids.length; i++) {
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
    var releaseDelay = 50;
    var stack = newInstance(3, releaseDelay - 10);
    //6 groups,
    pop(stack, 128 * 5 + 2);
    //return just 1 to the last group
    stack.push(128 * 5);
    assert.strictEqual(stack.groups.length, 6);

    //the last group is completed and can be released
    stack.push(128 * 5 + 1);
    push(stack, 0, 10);
    assert.strictEqual(stack.groups.length, 6);
    setTimeout(function () {
      //there should be 5 groups now
      assert.strictEqual(stack.groups.length, 5);
      stack.clear();
      done();
    }, releaseDelay);
  });
  it('should not release the current group', function (done) {
    var releaseDelay = 50;
    var stack = newInstance(3, releaseDelay - 10);
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
      stack.clear();
      done();
    }, releaseDelay);
  });
  it('should not release more than release size per time', function (done) {
    var releaseDelay = 50;
    var stack = newInstance(3, releaseDelay - 10);
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
    }, releaseDelay);
    setTimeout(function () {
      //there should be 12 - 8 groups now
      assert.strictEqual(stack.groups.length, 4);
      stack.clear();
      done();
    }, releaseDelay * 2);
  });
});

/** @returns {StreamIdStack}  */
function newInstance(version, releaseDelay) {
  var StreamIdStack = rewire('../../lib/stream-id-stack');
  StreamIdStack.__set__("releaseDelay", releaseDelay || 100);
  return new StreamIdStack(version || 3);
}

function pop(stack, n) {
  var arr = [];
  for (var i = 0; i < n; i++) {
    arr.push(stack.pop());
  }
  return arr;
}

function push(stack, initialValue, length) {
  if (util.isArray(initialValue)) {
    initialValue.forEach(stack.push.bind(stack));
    return;
  }
  for (var i = 0; i < length; i++) {
    stack.push(initialValue + i);
  }
}