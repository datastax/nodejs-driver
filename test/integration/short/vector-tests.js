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
const vdescribe = helper.vdescribe;

vdescribe('5.0.0', 'Vector tests', function () {
  this.timeout(120000);
  describe('#execute with vectors', function () {
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('table');
    const createTableCql = `CREATE TABLE ${table} (id uuid PRIMARY KEY, v1 vector<float, 3>, v2 vector<text,3>);`;
    
    const setupInfo = helper.setup(1, {
      keyspace: keyspace,
      queries: [ createTableCql ]
    });
    it('should insert and select vectors', function(done){
      const client = setupInfo.client;
      // if client undefined, raise error
      if(!client) {return done(new Error('client is not defined'));}
      const id = types.Uuid.random();
      const v1 = new Float32Array([1.1, 2.2, 3.3]);
      const query = `INSERT INTO ${table} (id, v1) VALUES (?, ?)`;
      client.execute(query, [id, v1], {prepare : true}, function(err){
        if (err) {return done(err);}
        client.execute(`SELECT v1 FROM ${table} WHERE id = ?`, [id], { prepare: true }, function(err, result){
          if (err) {return done(err);}
          assert.strictEqual(result.rows.length, 1);
          assert.strictEqual(result.rows[0].v1.length, 3);
          assert.strictEqual(result.rows[0].v1[0], v1[0]); 
          assert.strictEqual(result.rows[0].v1[1], v1[1]);
          assert.strictEqual(result.rows[0].v1[2], v1[2]);
          done();
        });
      });
    });


    it('should insert and select vector of text', function(done){
      const client = setupInfo.client;
      // if client undefined, raise error
      if(!client) {return done(new Error('client is not defined'));}
      const id = types.Uuid.random();
      const v1 = new Vector(['ab', 'b', 'cde'], 'text');
      const query = `INSERT INTO ${table} (id, v2) VALUES (?, ?)`;
      client.execute(query, [id, v1], {prepare : true}, function(err){
        if (err) {return done(err);}
        client.execute(`SELECT v2 FROM ${table} WHERE id = ?`, [id], { prepare: true }, function(err, result){
          if (err) {return done(err);}
          assert.strictEqual(result.rows.length, 1);
          assert.strictEqual(result.rows[0].v1.length, 3);
          assert.strictEqual(result.rows[0].v1[0], v1[0]); 
          assert.strictEqual(result.rows[0].v1[1], v1[1]);
          assert.strictEqual(result.rows[0].v1[2], v1[2]);
          done();
        });
      });
    });
  });
});