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

const { assert } = require('chai');
const sinon = require('sinon');

const helper = require('../test-helper');
const EventDebouncer = require('../../lib/metadata/event-debouncer');

describe('EventDebouncer', function () {
  describe('timeoutElapsed()', function () {
    it('should set the queue to null', function (done) {
      const debouncer = newInstance(1);
      debouncer._queue = {
        mainEvent: { handler: () => Promise.resolve() },
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
        mainEvent: { handler: () => Promise.resolve() },
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

      function increaseCounter() {
        callbackCounter++;
        return Promise.resolve();
      }

      debouncer._queue = {
        callbacks: [ assert.fail ],
        keyspaces: {
          'ks1': {
            mainEvent: {
              handler: () => { ksMainEventCalled++; return Promise.resolve();},
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
    it('should invoke 1 handler and all the callbacks when one event is flagged as `all`', async () => {
      const debouncer = newInstance(20);
      let mainEventHandlerCalled = 0;

      await Promise.all([
        debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks1' }, false),
        debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks1', cqlObject: 'abc' }, false),
        debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks2' }, false),

        // Send an event with `all: true`
        debouncer.eventReceived({
          handler: () => { mainEventHandlerCalled++; return Promise.resolve(); },
          all: true
        }, false),

        // Another one with `all: false`
        debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks2' }, false)
      ]);

      assert.strictEqual(mainEventHandlerCalled, 1);
      assert.strictEqual(debouncer._timeout, null);
    });

    it('should invoke 1 keyspace handler and all the callbacks when cqlObject is undefined', async () => {
      const debouncer = newInstance(30);
      const handlersCalled = [];
      function getHandler(name) {
        return () => {
          handlersCalled.push(name);
          return Promise.resolve();
        };
      }

      const promise = Promise.all([
        debouncer.eventReceived({ handler: getHandler('A'), keyspace: 'ks1' }, false),
        debouncer.eventReceived({ handler: getHandler('B'), keyspace: 'ks1', cqlObject: '1a' }, false),
        debouncer.eventReceived({ handler: getHandler('C'), keyspace: 'ks2' }, false),
        debouncer.eventReceived({ handler: getHandler('D'), keyspace: 'ks2' }, false),
        debouncer.eventReceived({ handler: getHandler('E'), keyspace: 'ks2' }, false),
        debouncer.eventReceived({ handler: getHandler('F'), keyspace: 'ks3', cqlObject: '3a' }, false),
        debouncer.eventReceived({ handler: getHandler('H'), keyspace: 'ks3', cqlObject: '3b' }, false)
      ]);

      await helper.delayAsync(5);

      // Should not be called yet
      assert.deepStrictEqual(handlersCalled, []);

      await promise;

      assert.deepEqual(handlersCalled, ['A', 'E', 'F', 'H']);
      assert.strictEqual(debouncer._timeout, null);
    });

    it('should not invoke handlers before time elapses', async () => {
      const debouncer = newInstance(200);
      const handlersCalled = [];
      function getHandler(name) {
        return () => {
          handlersCalled.push(name);
          return Promise.resolve();
        };
      }

      debouncer.eventReceived({ handler: getHandler('A'), keyspace: 'ks1' }, false);

      await helper.delayAsync(20);

      // should not be called yet
      assert.lengthOf(handlersCalled, 0);
      debouncer.shutdown();

    });

    it('should process queue immediately when processNow is true', async () => {
      const debouncer = newInstance(40);
      const handlersCalled = [];
      function getHandler(name) {
        return () => {
          handlersCalled.push(name);
          return Promise.resolve();
        };
      }

      const spy = sinon.spy(() => {});

      const promise = Promise.all([
        debouncer.eventReceived({ handler: getHandler('A'), keyspace: 'ks2' }, false).then(spy),
        debouncer.eventReceived({ handler: getHandler('B'), keyspace: 'ks1' }, false).then(spy),
        // set with process now to true
        debouncer.eventReceived({ handler: getHandler('C'), keyspace: 'ks1' }, true).then(spy),
        debouncer.eventReceived({ handler: getHandler('D'), keyspace: 'ks1' }, false).then(spy)
      ]);

      await helper.delayAsync(20);

      // The first three should be resolved by now
      assert.strictEqual(spy.callCount, 3);
      // Flattens the amount of requests
      assert.deepEqual(handlersCalled, [ 'A', 'C' ]);

      await promise;

      assert.deepEqual(handlersCalled, [ 'A', 'C', 'D' ]);
    });
  });
  describe('#shutdown()', () => {
    it('should invoke all callbacks', async () => {
      const debouncer = newInstance(20);
      const spy = sinon.spy(() => {});

      debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks1' }, false).then(spy);
      debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks1', cqlObject: '1a' }, false).then(spy);
      debouncer.eventReceived({ handler: helper.failop, keyspace: 'ks2' }, false).then(spy);
      debouncer.eventReceived({ handler: helper.failop, all: true }, false).then(spy);

      debouncer.shutdown();

      await helper.delayAsync(5);
      assert.strictEqual(spy.callCount, 4);

      // Check that timer shouldn't elapse
      await helper.delayAsync(30);
      assert.strictEqual(spy.callCount, 4);
    });
  });
});

/** @returns {EventDebouncer} */
function newInstance(delay) {
  return new EventDebouncer(delay, helper.noop);
}