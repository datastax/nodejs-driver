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
const { assert, util } = require('chai');
const Encoder = require('../../lib/encoder');
const { types } = require('../../index');
const Vector = require('../../lib/types/vector');
const Long = require('long');

describe('Vector tests', function () {
  const encoder = new Encoder(4, {});
  /**
     * @type {Array<{subtypeString : string, typeInfo: import('../../lib/encoder').VectorColumnInfo, value: Array}>}
     */
  const dataProvider = [
    {
      subtypeString : 'float',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.float,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [1.1122000217437744, 2.212209939956665, 3.3999900817871094]
    },
    {
      subtypeString : 'double',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.double,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [1.1, 2.2, 3.3]
    },
    {
      subtypeString : 'text',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.text,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: ['a', 'bc', 'cde']
    },
    {
      subtypeString : 'int',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.int,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [1, 2, 3]
    },
    {
      subtypeString : 'bigint',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.bigint,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [Long.fromInt(1), Long.fromInt(2), Long.fromInt(3)]
    },
    {
      subtypeString : 'uuid',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.uuid,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.Uuid.random(), types.Uuid.random(), types.Uuid.random()]
    },
    {
      subtypeString : 'timeuuid',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.timeuuid,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.TimeUuid.now(), types.TimeUuid.now(), types.TimeUuid.now()]
    },
    {
      subtypeString : 'decimal',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.decimal,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.BigDecimal.fromString('1.1'), types.BigDecimal.fromString('2.2'), types.BigDecimal.fromString('3.3')]
    }
  ];

  dataProvider.forEach((data) => {
    it(`should encode and decode vector of ${data.subtypeString}`, function () {
      const vector = new Vector(data.value);
      const encoded = encoder.encode(vector, data.typeInfo);
      const decoded = encoder.decode(encoded, data.typeInfo);
      assert.strictEqual(util.inspect(decoded), util.inspect(vector));
    });
  });

  it('should encode and decode vector of float', function(){
    const vector = new Float32Array([1.1, 2.2, 3.3]);
    const typeObj = { code: types.dataTypes.custom, info: {code: types.dataTypes.float}, dimension: 3, customTypeName: 'vector' };
    const encoded = encoder.encode(vector, typeObj);
    const decoded = encoder.decode(encoded, typeObj);
    for (let i = 0; i < vector.length; i++) {
      assert.strictEqual(decoded[i], vector[i]);
    }
  });
});