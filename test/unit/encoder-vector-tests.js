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

describe('Vector tests', function () {
  const encoder = new Encoder(4, {});
  /**
 * @type {Array<{subtypeString : string, typeInfo: import('../../../lib/encoder').VectorColumnInfo, value: Array}>}
 */
  const dataProvider = [
    {
      subtypeString: 'float',
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
      subtypeString: 'double',
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
      subtypeString: 'varchar',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.text,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: ['ab', 'b', 'cde']
    },
    {
      subtypeString: 'bigint',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.bigint,
        },
        customTypeName: 'vector',
        dimension: 3,
      },
      value: [new types.Long(1), new types.Long(2), new types.Long(3)]
    },
    {
      subtypeString: 'blob',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.blob,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [Buffer.from([1, 2, 3]), Buffer.from([4, 5, 6]), Buffer.from([7, 8, 9])]
    },
    {
      subtypeString: 'boolean',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.boolean,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [true, false, true]
    },
    {
      subtypeString: 'decimal',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.decimal,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.BigDecimal.fromString('1.1'), types.BigDecimal.fromString('2.2'), types.BigDecimal.fromString('3.3')]
    },
    {
      subtypeString: 'inet',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.inet,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.InetAddress.fromString('127.0.0.1'), types.InetAddress.fromString('0.0.0.0'), types.InetAddress.fromString('34.12.10.19')]
    },
    {
      subtypeString: 'tinyint',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.tinyint,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [1, 2, 3]
    },
    {
      subtypeString: 'smallint',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.smallint,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [1, 2, 3]
    },
    {
      subtypeString: 'int',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.int,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [-1, 0, -3]
    },
    // TODO: what do we want to do with Duration?
    // {
    //   subtypeString: 'duration',
    //   typeInfo: {
    //     code: types.dataTypes.custom,
    //     info: {
    //       code: types.dataTypes.custom,
    //       info: 'duration'
    //     },
    //     customTypeName: 'vector',
    //     dimension: 3
    //   },
    //   value: [new types.Duration(1, 2, 3), new types.Duration(4, 5, 6), new types.Duration(7, 8, 9)]
    // },
    {
      subtypeString: 'date',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.date,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [new types.LocalDate(2020, 1, 1), new types.LocalDate(2020, 2, 1), new types.LocalDate(2020, 3, 1)]
    },
    {
      subtypeString: 'time',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.time,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [new types.LocalTime(types.Long.fromString('6331999999911')), new types.LocalTime(types.Long.fromString('6331999999911')), new types.LocalTime(types.Long.fromString('6331999999911'))]
    },
    {
      subtypeString: 'timestamp',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.timestamp,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [new Date(2020, 1, 1, 1, 1, 1, 1), new Date(2020, 2, 1, 1, 1, 1, 1), new Date(2020, 3, 1, 1, 1, 1, 1)]
    },
    {
      subtypeString: 'uuid',
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
      subtypeString: 'timeuuid',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.timeuuid,
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [types.TimeUuid.now(), types.TimeUuid.now(), types.TimeUuid.now()]
    }
  ];

  const dataProviderWithCollections = dataProvider.flatMap(data => [
    data,
    // vector<list<subtype>, 3>
    {
      subtypeString: 'list<' + data.subtypeString + '>',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.list,
          info: {
            code: data.typeInfo.info.code,
            info: data.typeInfo.info.info
          }
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: data.value.map(value => [value, value, value])
    },
    // vector<map<int, subtype>, 3>
    {
      subtypeString: 'map<int, ' + data.subtypeString + '>',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.map,
          info: [
            { code: types.dataTypes.int },
            { code: data.typeInfo.info.code, info: data.typeInfo.info.info }
          ]
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: data.value.map((value) => ({ 1: value, 2: value, 3: value }))
    },
    // vector<set<subtype>, 3>
    {
      subtypeString: 'set<' + data.subtypeString + '>',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.set,
          info: {
            code: data.typeInfo.info.code,
            info: data.typeInfo.info.info
          }
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: data.value.map(value => [value, value, value])
    },
    // vector<tuple<subtype, subtype>, 3>
    {
      subtypeString: 'tuple<' + data.subtypeString + ', ' + data.subtypeString + '>',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.tuple,
          info: [
            { code: data.typeInfo.info.code, info: data.typeInfo.info.info },
            { code: data.typeInfo.info.code, info: data.typeInfo.info.info }
          ]
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: data.value.map(value => new types.Tuple(value, value))
    },
    // vector<vector<subtype, 3>, 3>
    {
      subtypeString: 'vector<' + data.subtypeString + ', 3>',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.custom,
          info: {
            code: data.typeInfo.info.code,
            info: data.typeInfo.info.info
          },
          customTypeName: 'vector',
          dimension: 3
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: data.value.map(value => new Vector([value, value, value], data.subtypeString))
    }
  ]).concat([
  // vector<my_udt, 3>
    {
      subtypeString: 'my_udt',
      typeInfo: {
        code: types.dataTypes.custom,
        info: {
          code: types.dataTypes.udt,
          info: {
            name: 'my_udt',
            fields: [{ name: 'f1', type: { code: types.dataTypes.text } }],
          }
        },
        customTypeName: 'vector',
        dimension: 3
      },
      value: [{ f1: 'a' }, { f1: 'b' }, { f1: 'c' }]
    }
  ]);

  dataProviderWithCollections.forEach((data) => {
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
    const typeObj = { code: types.dataTypes.custom, info: {code: types.dataTypes.float}, dimension: 3, customTypeName: 'vector' };
    const encoded = encoder.encode(vector, typeObj);
    const decoded = encoder.decode(encoded, typeObj);
    for (let i = 0; i < vector.length; i++) {
      assert.strictEqual(decoded[i], vector[i]);
    }
  });
});