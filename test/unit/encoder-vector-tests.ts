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
import { assert, util } from "chai";
import Encoder from "../../lib/encoder";
import { types } from "../../index";
import Vector from "../../lib/types/vector";
import helper from "../test-helper";

'use strict';
describe('Vector tests', function () {
  const encoder = new Encoder(4, {});

  helper.dataProviderWithCollections.forEach((data) => {
    it(`should encode and decode vector of ${data.subtypeString}`, function () {
      const vector = new Vector(data.value, data.subtypeString);
      const encoded = encoder.encode(vector, data.typeInfo);
      const decoded = encoder.decode(encoded, data.typeInfo);
      assert.strictEqual(util.inspect(decoded), util.inspect(vector));
    });

    it(`should encode and decode vector of ${data.subtypeString} while guessing data type`, function (){
      if (data.subtypeString === 'my_udt'){
        // cannot guess udt type
        this.skip();
      }
      const vector = new Vector(data.value, data.subtypeString);
      const guessedType = Encoder.guessDataType(vector);
      if (!guessedType) {
        throw new Error('Can not guess type');
      }
      const encoded = encoder.encode(vector, guessedType);
      const decoded = encoder.decode(encoded, guessedType);
      assert.strictEqual(util.inspect(decoded), util.inspect(vector));
    });

    it(`should throw when providing less or more elements/bytes when encoding/decoding vector of ${data.subtypeString}`, function () {
      const vector = new Vector(data.value, data.subtypeString);
      const encoded = encoder.encode(vector, data.typeInfo);
      const encodedBuffer = Buffer.from(encoded);
      const encodedBufferShort = encodedBuffer.slice(0, encodedBuffer.length - 1);
      const encodedBufferLong = Buffer.concat([encodedBuffer, Buffer.alloc(1)]);
      assert.throws(() => encoder.decode(encodedBufferShort, data.typeInfo), 'Not enough bytes to decode the vector');
      assert.throws(() => encoder.decode(encodedBufferLong, data.typeInfo), 'Extra bytes found after decoding the vector');

      const shortVector = new Vector(data.value.slice(0, data.value.length - 1), data.subtypeString);
      const longVector = new Vector(data.value.concat(data.value), data.subtypeString);
      assert.throws(() => encoder.encode(shortVector, data.typeInfo), 'Expected vector with 3 dimensions, observed size of 2');
      assert.throws(() => encoder.encode(longVector, data.typeInfo), 'Expected vector with 3 dimensions, observed size of 6');
    });
  });

  it('should encode and decode vector of float', function(){
    const vector = new Float32Array([1.1, 2.2, 3.3]);
    const typeObj = { code: types.dataTypes.custom, info: [{code: types.dataTypes.float}, 3], customTypeName: 'vector' };
    const encoded = encoder.encode(vector, typeObj);
    const decoded = encoder.decode(encoded, typeObj);
    for (let i = 0; i < vector.length; i++) {
      assert.strictEqual(decoded[i], vector[i]);
    }
  });
});