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
const helper = require('../../test-helper.js');

const { types } = require('../../../index.js');
const Vector = require('../../../lib/types/vector.js');
const util = require('node:util');
const vdescribe = helper.vdescribe;

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
    subtypeString: 'list<float>',
    typeInfo: {
      code: types.dataTypes.custom,
      info: {
        code: types.dataTypes.list,
        subTypes: [{ code: types.dataTypes.float }]
      },
      customTypeName: 'vector',
      dimension: 3
    },
    value: [[1.1122000217437744, 2.212209939956665], [2.212209939956665, 2.212209939956665], [1.1122000217437744, 1.1122000217437744]]
  }
];


vdescribe('5.0.0', 'Vector tests', function () {
  this.timeout(120000);

  describe('#execute with vectors', function () {
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('table');
    let createTableCql = `CREATE TABLE ${table} (id uuid PRIMARY KEY`;
    dataProvider.forEach(data => {
      createTableCql += `, ${subtypeStringToColumnName(data.subtypeString)} vector<${data.subtypeString}, 3>`;
    });
    createTableCql += ');';

    const setupInfo = helper.setup(1, {
      keyspace: keyspace,
      queries: [createTableCql]
    });

    const client = setupInfo.client;
    if (!client) { throw new Error('client setup failed'); }

    dataProvider.forEach(data => {
      it('should insert and select vector of subtype ' + data.subtypeString, function (done) {
        const id = types.Uuid.random();
        const vector = new Vector(data.value, data.subtypeString);
        const query = `INSERT INTO ${table} (id, ${subtypeStringToColumnName(data.subtypeString)}) VALUES (?, ?)`;
        client.execute(query, [id, vector], { prepare: true }, function (err) {
          if (err) { return done(err); }
          client.execute(`SELECT ${subtypeStringToColumnName(data.subtypeString)} FROM ${table} WHERE id = ?`, [id], { prepare: true }, function (err, result) {
            if (err) { return done(err); }
            assert.strictEqual(result.rows.length, 1);
            assert.strictEqual(util.inspect(result.rows[0][subtypeStringToColumnName(data.subtypeString)]), util.inspect(vector));
            done();
          });
        });
      });

      it('should insert and select vector of subtype ' + data.subtypeString + ' while guessing data type', function (done) {
        const id = types.Uuid.random();
        const vector = new Vector(data.value, data.subtypeString);
        const query = `INSERT INTO ${table} (id, ${subtypeStringToColumnName(data.subtypeString)}) VALUES (?, ?)`;
        client.execute(query, [id, vector], { prepare: true }, function (err) {
          if (err) { return done(err); }
          client.execute(`SELECT ${subtypeStringToColumnName(data.subtypeString)} FROM ${table} WHERE id = ?`, [id], { prepare: true }, function (err, result) {
            if (err) { return done(err); }
            assert.strictEqual(result.rows.length, 1);
            assert.strictEqual(util.inspect(result.rows[0][subtypeStringToColumnName(data.subtypeString)]), util.inspect(vector));

            done();
          });
        });
      });
    });
  });
});

/**
 * 
 * @param {string} subtypeString 
 * @returns 
 */
function subtypeStringToColumnName(subtypeString) {
  return "v" + subtypeString.replace('<', '_').replace('>', '_');
}