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
import assert from "assert";
import helper from "../../test-helper";
import { types } from "../../../index";
import Vector from "../../../lib/types/vector";
import util from "node:util";


const vdescribe = helper.vdescribe;
vdescribe('5.0.0', 'Vector tests', function () {
  this.timeout(120000);

  describe('#execute with vectors', function () {
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('table');
    let createTableCql = `CREATE TABLE ${table} (id uuid PRIMARY KEY`;
    helper.dataProviderWithCollections.forEach(data => {
      // skip set<duration> because it's not allowed in C* 5.0
      if (data.subtypeString === 'set<duration>') {
        return;
      }
      createTableCql += `, ${subtypeStringToColumnName(data.subtypeString)} vector<${data.subtypeString}, 3>`;
    });
    createTableCql += ');';
    const createUdtCql = `CREATE TYPE ${keyspace}.my_udt (f1 text);`;
    const setupInfo = helper.setup(1, {
      keyspace: keyspace,
      queries: [createUdtCql, createTableCql]
    });

    const client = setupInfo.client;
    if (!client) { throw new Error('client setup failed'); }

    helper.dataProviderWithCollections.forEach(data => {
      // skip set<duration> because it's not allowed in C* 5.0
      if (data.subtypeString === 'set<duration>') {
        return;
      }
      it('should insert, select, and update vector of subtype ' + data.subtypeString, function (done) {
        const id = types.Uuid.random();
        const vector = new Vector(data.value, data.subtypeString);
        const query = `INSERT INTO ${table} (id, ${subtypeStringToColumnName(data.subtypeString)}) VALUES (?, ?)`;
        client.execute(query, [id, vector], { prepare: true }, function (err) {
          if (err) { return done(err); }
          client.execute(`SELECT ${subtypeStringToColumnName(data.subtypeString)} FROM ${table} WHERE id = ?`, [id], { prepare: true }, function (err, result) {
            if (err) { return done(err); }
            assert.strictEqual(result.rows.length, 1);
            assert.strictEqual(util.inspect(result.rows[0][subtypeStringToColumnName(data.subtypeString)]), util.inspect(vector));

            const updatedValues = data.value.slice();
            updatedValues[0] = updatedValues[1];
            client.execute(`UPDATE ${table} SET ${subtypeStringToColumnName(data.subtypeString)} = ? WHERE id = ?`, [new Vector(updatedValues, data.subtypeString), id], { prepare: true }, function (err, _result) {
              if (err) { return done(err); }
              done();
            });
          });
        });
      });

      it('should insert and select vector of subtype ' + data.subtypeString + ' with prepare to be false', function (done) {
        if (data.subtypeString === 'my_udt') {
          this.skip();
        }
        const id = types.Uuid.random();
        const vector = new Vector(data.value, data.subtypeString);
        const query = `INSERT INTO ${table} (id, ${subtypeStringToColumnName(data.subtypeString)}) VALUES (?, ?)`;
        client.execute(query, [id, vector], { prepare: false }, function (err) {
          if (err) { return done(err); }
          client.execute(`SELECT ${subtypeStringToColumnName(data.subtypeString)} FROM ${table} WHERE id = ?`, [id], { prepare: false }, function (err, result) {
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
  return "v" + subtypeString.replace(/<|>|,| /g, '_');
}