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
const utils = require('../../lib/utils');
const helper = require('../test-helper');
const AddressResolver = utils.AddressResolver;

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
      const b = utils.allocBuffer(256);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.length, 256);
      for (let i = 0; i < b.length; i++) {
        assert.strictEqual(b[i], 0);
      }
    });
  });
  describe('allocBufferUnsafe', function () {
    it('should return a Buffer with the correct length', function () {
      const b = utils.allocBuffer(256);
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
      const text = 'Hello safe buffer';
      const b = utils.allocBufferFromString(text);
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
      const arr = [ 0xff, 0, 0x1a ];
      const b = utils.allocBufferFromArray(arr);
      helper.assertInstanceOf(b, Buffer);
      assert.strictEqual(b.length, arr.length);
      for (let i = 0; i < b.length; i++) {
        assert.strictEqual(b[i], arr[i]);
      }
    });
  });

  describe('AddressResolver', () => {

    describe('#getIp()', () => {

      it('should return the resolved addresses in a round-robin fashion', async () => {
        const addresses = ['10.10.10.1', '10.10.10.2'];
        const resolver = new AddressResolver({ nameOrIp: 'dummy-host', dns: getDnsMock(addresses) });

        await resolver.init();

        const result = new Map();

        for (let i = 0; i < 10; i++) {
          const ip = resolver.getIp();
          result.set(ip, (result.get(ip) || 0) + 1);
        }

        assert.deepStrictEqual(Array.from(result.keys()).sort(), addresses);
        assert.strictEqual(result.get(addresses[0]), 5);
        assert.strictEqual(result.get(addresses[1]), 5);
      });
    });

    describe('#init()', () => {

      it('should callback in error when resolution fails', async () => {
        const error = new Error('dummy error');
        const resolver = new AddressResolver({ nameOrIp: 'dummy-host', dns: getDnsMock(error) });

        let err;

        try {
          await resolver.init();
        } catch (e) {
          err = e;
        }

        assert.strictEqual(err, error);
      });

      it('should callback in error when resolution returns empty', async () => {
        const resolver = new AddressResolver({ nameOrIp: 'dummy-host', dns: getDnsMock([]) });

        let err;

        try {
          await resolver.init();
        } catch (e) {
          err = e;
        }

        helper.assertInstanceOf(err, Error);
        helper.assertContains(err.message, 'could not be resolved');
      });

      it('should support a IP address as parameter', async () => {
        const address = '10.10.10.255';
        const resolver = new AddressResolver({ nameOrIp: address, dns: getDnsMock(new Error('ip must be used')) });

        await resolver.init();

        for (let i = 0; i < 10; i++) {
          assert.strictEqual(resolver.getIp(), address);
        }
      });
    });

    describe('#refresh()', () => {

      it('should ignore failures', async () => {
        let initialized = false;
        const address = '10.10.10.1';

        const dnsMock = {
          resolve4: (name, cb) => {
            if (!initialized) {
              cb(null, [ address ]);
            } else {
              cb(new Error('this error should be ignored'));
            }
          }
        };

        const resolver = new AddressResolver({ nameOrIp: 'dummy-host', dns: dnsMock });

        await resolver.init();

        assert.strictEqual(resolver.getIp(), address);
        initialized = true;

        await resolver.refresh();

        // getIp() should work as usual
        assert.strictEqual(resolver.getIp(), address);
      });

      it('should update the ips returned by getIp() methods', async () => {
        const addresses = ['10.10.10.1'];
        const resolver = new AddressResolver({ nameOrIp: 'dummy-host', dns: getDnsMock(addresses) });

        await resolver.init();

        // Update the addresses that are going to be resolved
        const initialAddresses = addresses.slice(0);
        addresses.splice(0, 1);
        addresses.push('10.10.10.1', '10.10.10.2');

        // Validate that get ip uses a cached value
        for (let i = 0; i < 10; i++) {
          assert.strictEqual(resolver.getIp(), initialAddresses[0]);
        }

        await resolver.refresh();

        const result = new Map();

        // Should have resolved
        for (let i = 0; i < 10; i++) {
          const ip = resolver.getIp();
          result.set(ip, (result.get(ip) || 0) + 1);
        }

        assert.deepStrictEqual(Array.from(result.keys()).sort(), addresses);
        assert.strictEqual(result.get(addresses[0]), 5);
        assert.strictEqual(result.get(addresses[1]), 5);
      });

      //TODO
      it('should return the promise instance');
    });
  });
});

function getDnsMock(addressesOrErr) {
  return {
    resolve4: (name, cb) => {
      if (Array.isArray(addressesOrErr)) {
        // Use a copy of the addresses
        process.nextTick(() => cb(null, Array.from(addressesOrErr)));
      } else {
        process.nextTick(() => cb(addressesOrErr));
      }
    }
  };
}