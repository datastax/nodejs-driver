/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const assert = require('assert');

const helper = require('../test-helper');
const EventDebouncer = require('../../lib/metadata/event-debouncer');

describe('EventDebouncer', function () {
  describe('timeoutElapsed()', function () {
    it('should set the queue to null', function (done) {
      const debouncer = newInstance(1);
      debouncer._queue = {
        mainEvent: { handler: helper.callbackNoop },
        callbacks: [ helper.noop ]
      };
      debouncer._slideDelay(1);
      setTimeout(function () {
        assert.strictEqual(debouncer._queue, null);
        done();
      }, 40);
    });
    it('should process the main event and invoke all the callbacks', function (done) {
      const debouncer = newInstance(1);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      debouncer._queue = {
        mainEvent: { handler: helper.callbackNoop },
        callbacks: helper.fillArray(10, increaseCounter)
      };
      debouncer._slideDelay(1);
      setTimeout(function () {
        assert.strictEqual(callbackCounter, 10);
        done();
      }, 40);
    });
    it('should process each keyspace the main event and invoke all child the callbacks', function (done) {
      const debouncer = newInstance(1);
      let callbackCounter = 0;
      let ksMainEventCalled = 0;
      function increaseCounter() { callbackCounter++; }
      debouncer._queue = {
        callbacks: [ assert.fail ],
        keyspaces: {
          'ks1': {
            mainEvent: {
              handler: function (cb) { ksMainEventCalled++; cb();},
              callback: increaseCounter
            },
            events: [ { callback: increaseCounter }, { callback: increaseCounter }]
          }
        }
      };
      debouncer._slideDelay(1);
      setTimeout(function () {
        assert.strictEqual(callbackCounter, 2);
        assert.strictEqual(ksMainEventCalled, 1);
        done();
      }, 40);
    });
    it('should process each keyspace and invoke handlers and callbacks', function (done) {
      const debouncer = newInstance(1);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      const handlersCalled = [];
      function getHandler(name) {
        return (function() { handlersCalled.push(name); });
      }
      debouncer._queue = {
        callbacks: [ assert.fail ],
        keyspaces: {
          'ks1': {
            events: [
              { callback: increaseCounter, handler: getHandler('A') },
              { callback: increaseCounter, handler: getHandler('B') }
            ]
          }
        }
      };
      debouncer._slideDelay(1);
      setTimeout(function () {
        assert.strictEqual(callbackCounter, 2);
        assert.deepEqual(handlersCalled, [ 'A', 'B' ]);
        done();
      }, 40);
    });
  });
  describe('#eventReceived()', function () {
    it('should invoke 1 handler and all the callbacks when one event is flagged as `all`', function (done) {
      const debouncer = newInstance(20);
      let callbackCounter = 0;
      let mainEventHandlerCalled = 0;
      function increaseCounter() { callbackCounter++; }
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks1' }, false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks1', cqlObject: 'abc' },
        false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({
        handler: function (cb) { mainEventHandlerCalled++; cb(); },
        all: true,
        callback: increaseCounter
      }, false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks2' }, false);
      setTimeout(function () {
        assert.strictEqual(callbackCounter, 5);
        assert.strictEqual(mainEventHandlerCalled, 1);
        assert.strictEqual(debouncer._timeout, null);
        done();
      }, 40);
    });
    it('should invoke 1 keyspace handler and all the callbacks when cqlObject is undefined', function (done) {
      const debouncer = newInstance(30);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      const handlersCalled = [];
      function getHandler(name) {
        return (function(cb) {
          handlersCalled.push(name);
          if (cb) {
            cb();
          }
        });
      }
      debouncer.eventReceived({ handler: getHandler('A'), callback: increaseCounter, keyspace: 'ks1' }, false);
      debouncer.eventReceived({ handler: getHandler('B'), callback: increaseCounter, keyspace: 'ks1', cqlObject: '1a' },
        false);
      debouncer.eventReceived({ handler: getHandler('C'), callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({ handler: getHandler('D'), callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({ handler: getHandler('E'), callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({ handler: getHandler('F'), callback: increaseCounter, keyspace: 'ks3', cqlObject: '3a' },
        false);
      debouncer.eventReceived({ handler: getHandler('H'), callback: increaseCounter, keyspace: 'ks3', cqlObject: '3b' },
        false);
      setTimeout(function checkNotCalledImmediately() {
        // should not be called yet
        assert.strictEqual(callbackCounter, 0);
      }, 10);
      setTimeout(function checkAfterTimeElapsed() {
        assert.strictEqual(callbackCounter, 7);
        assert.deepEqual(handlersCalled, ['A', 'E', 'F', 'H']);
        assert.strictEqual(debouncer._timeout, null);
        done();
      }, 50);
    });
    it('should not invoke handlers before time elapses', function (done) {
      const debouncer = newInstance(200);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      const handlersCalled = [];
      function getHandler(name) {
        return (function(cb) {
          handlersCalled.push(name);
          if (cb) {
            cb();
          }
        });
      }
      debouncer.eventReceived({ handler: getHandler('A'), callback: increaseCounter, keyspace: 'ks1' }, false);
      setTimeout(function checkNotCalledImmediately() {
        // should not be called yet
        assert.strictEqual(callbackCounter, 0);
        debouncer.shutdown();
        done();
      }, 30);
    });
    it('should process queue immediately when processNow is true', function (done) {
      const debouncer = newInstance(40);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      const handlersCalled = [];
      function getHandler(name) {
        return (function(cb) {
          handlersCalled.push(name);
          if (cb) {
            cb();
          }
        });
      }
      debouncer.eventReceived({ handler: getHandler('A'), callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({ handler: getHandler('B'), callback: increaseCounter, keyspace: 'ks1' }, false);
      // set with process now to true
      debouncer.eventReceived({ handler: getHandler('C'), callback: increaseCounter, keyspace: 'ks1' }, true);
      debouncer.eventReceived({ handler: getHandler('D'), callback: increaseCounter, keyspace: 'ks1' }, false);
      setTimeout(function () {
        // the first ones should be processed by now (before delay!)
        assert.strictEqual(callbackCounter, 3);
        // still flattens the amount of requests
        assert.deepEqual(handlersCalled, [ 'A', 'C' ]);
      }, 20);
      setTimeout(function () {
        // all should be called by now: immediate ones and the delayed one
        assert.strictEqual(callbackCounter, 4);
        // still flattens the amount of requests
        assert.deepEqual(handlersCalled, [ 'A', 'C', 'D' ]);
        debouncer.shutdown();
        done();
      }, 60);
    });
  });
  describe('#shutdown()', function () {
    it('should invoke all callbacks', function (done) {
      const debouncer = newInstance(20);
      let callbackCounter = 0;
      function increaseCounter() { callbackCounter++; }
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks1' }, false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks1', cqlObject: '1a' },
        false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, keyspace: 'ks2' }, false);
      debouncer.eventReceived({ handler: helper.failop, callback: increaseCounter, all: true }, false);
      debouncer.shutdown();
      assert.strictEqual(callbackCounter, 4);
      setTimeout(function () {
        assert.strictEqual(callbackCounter, 4);
        assert.strictEqual(debouncer._timeout, null);
        done();
      }, 40);
    });
  });
});

/** @returns {EventDebouncer} */
function newInstance(delay) {
  return new EventDebouncer(delay, helper.noop);
}