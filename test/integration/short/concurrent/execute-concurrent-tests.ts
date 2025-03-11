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
import assert from "assert";
import fs from "fs";
import types from "../../../../lib/types/index";
import errors from "../../../../lib/errors";
import helper from "../../../test-helper";
import { Transform } from "stream";
import {executeConcurrent} from "../../../../lib/concurrent";

const Uuid = types.Uuid;

const insertQuery1 = 'INSERT INTO table1 (key1, key2, value) VALUES (?, ?, ?)';
const insertQuery2 = 'INSERT INTO table2 (id, value) VALUES (?, ?)';
const countQuery1 = 'SELECT COUNT(*) as total FROM table1 WHERE key1 = ?';
const selectQuery2 = 'SELECT * FROM table2 WHERE id = ?';

describe('executeConcurrent()', function () {
  this.timeout(120000);

  const setupInfo = helper.setup(1, {
    queries: [
      'CREATE TABLE table1 (key1 uuid, key2 int, value text, PRIMARY KEY (key1, key2))',
      'CREATE TABLE table2 (id uuid PRIMARY KEY, value text)',
    ]
  });

  const client = setupInfo.client;

  describe('with fixed query and parameters', () => {
    it('should insert hundreds of rows', () => {
      const id = Uuid.random();
      const values = Array.from(new Array(600).keys()).map(x => [ id, x, x.toString() ]);

      return executeConcurrent(client, insertQuery1, values)
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors, []);
        })
        .then(() => validateInserted(client, id, values.length));
    });

    it('should not store individual results', () => {
      const id = Uuid.random();
      const values = getParameterValues(id, 10);

      return executeConcurrent(client, insertQuery1, values)
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors, []);
          assert.throws(() => result.resultItems, /can not be accessed when collectResults is set to false/);
        })
        .then(() => validateInserted(client, id, values.length));
    });

    it('should store individual results when collectResults is set true', () => {
      const id = Uuid.random();
      const values = getParameterValues(id, 20);

      return executeConcurrent(client, insertQuery1, values, { concurrencyLevel: 5, collectResults: true })
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors, []);
          assert.strictEqual(result.resultItems.length, values.length);
          result.resultItems.forEach(rs => helper.assertInstanceOf(rs, types.ResultSet));
        })
        .then(() => validateInserted(client, id, values.length));
    });

    it('should store individual results and errors when collectResults is set true', () => {
      const id = Uuid.random();
      const values = getParameterValues(id, 10);
      const options = { concurrencyLevel: 5, raiseOnFirstError: false, collectResults: true };

      // Set invalid parameters for one of the items
      values[7] = [];

      return executeConcurrent(client, insertQuery1, values, options)
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors.length, 1);
          assert.strictEqual(result.resultItems.length, values.length);
          result.resultItems.forEach((rs, index) =>
            helper.assertInstanceOf(rs, index === 7 ? errors.ResponseError : types.ResultSet));
        })
        .then(() => validateInserted(client, id, values.length - 1));
    });

    it('should throw when there is an error', () => {
      const values = [[ Uuid.random(), 1, 'one' ], []];
      let error;

      return executeConcurrent(client, insertQuery1, values)
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, errors.ResponseError);
          assert.strictEqual(error.code, types.responseErrorCodes.invalid);
        });
    });

    it('should resolve when there is an error and raiseOnFirstError is false', () => {
      const id = Uuid.random();
      const values = getParameterValues(id, 50);

      // Set invalid parameters for two of the values
      values[19] = values[33] = [];

      return executeConcurrent(client, insertQuery1, values, { raiseOnFirstError: false, concurrencyLevel: 10 })
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.strictEqual(result.errors.length, 2);
          result.errors.forEach(err => helper.assertInstanceOf(err, errors.ResponseError));
        })
        .then(() => validateInserted(client, id, values.length - 2));
    });
  });

  describe('with fixed query and a stream', () => {
    it('should support a transformed text file as input', () => {
      const id = Uuid.random();
      const fsStream = fs.createReadStream(__filename, { highWaterMark: 512 });
      const transformStream = fsStream.pipe(new LineTransform(id));

      return executeConcurrent(client, insertQuery1, transformStream, { concurrencyLevel: 10 })
        .then(result => {
          assert.strictEqual(result.totalExecuted, transformStream.index);
          // It should have paused the stream while reading
          assert.ok(transformStream.pauseCounter > 1);
          assert.deepStrictEqual(result.errors, []);
          assert.throws(() => result.resultItems, /resultItems/);
        })
        .then(() => validateInserted(client, id, transformStream.index));
    });

    it('should reject the promise when there is an execution error', () => {
      const fsStream = fs.createReadStream(__filename);
      const fixedValues = new Map([[15, []], [30, []]]);
      const transformStream = fsStream.pipe(new LineTransform(Uuid.random(), { fixedValues }));
      let error;

      return executeConcurrent(client, insertQuery1, transformStream, { concurrencyLevel: 10 })
        .catch(err => error = err)
        .then(() => helper.assertInstanceOf(error, errors.ResponseError));
    });

    it('should resolve the promise when there is an execution error and raiseOnFirstError is false', () => {
      const id = Uuid.random();
      const fsStream = fs.createReadStream(__filename);
      const fixedValues = new Map([[15, []], [30, []]]);
      const transformStream = fsStream.pipe(new LineTransform(id, { fixedValues }));

      return executeConcurrent(client, insertQuery1, transformStream, { concurrencyLevel: 10, raiseOnFirstError: false })
        .then(result => {
          assert.strictEqual(result.totalExecuted, transformStream.index);
          assert.strictEqual(result.errors.length, 2);
          result.errors.forEach(err => helper.assertInstanceOf(err, errors.ResponseError));
        })
        .then(() => validateInserted(client, id, transformStream.index - 2));
    });

    it('should reject the promise when there is a read error', () =>
      // Regardless of the raiseOnFirstError setting
      Promise.all([ true, false ].map(raiseOnFirstError => {
        const fsStream = fs.createReadStream(__filename);
        const transformStream = fsStream.pipe(new LineTransform(Uuid.random(), { failAtIndex: 15 }));
        let error;

        return executeConcurrent(client, insertQuery1, transformStream, { concurrencyLevel: 10, raiseOnFirstError })
          .catch(err => error = err)
          .then(() => {
            helper.assertInstanceOf(error, Error);
            assert.strictEqual(error.message, 'Test transform error');
          });
      })));
  });

  describe('with different queries and parameters', () => {
    it('should execute the different queries', () => {
      const id = Uuid.random();
      const queryAndParameters = [
        { query: insertQuery1, params: [ id, 0, 'one on table1'] },
        { query: insertQuery2, params: [ id, 'one on table2' ]},
        { query: insertQuery1, params: [ id, 1, 'second on table1'] }
      ];

      return executeConcurrent(client, queryAndParameters)
        .then(result => {
          assert.strictEqual(result.totalExecuted, queryAndParameters.length);
          assert.strictEqual(result.errors.length, 0);
          assert.throws(() => result.resultItems, /resultItems can not be accessed/);
        })
        .then(() => validateInserted(client, id, 2))
        .then(() => client.execute(selectQuery2, [ id ], { prepare: true }))
        .then(rs2 => assert.ok(rs2.first()));
    });

    it('should reject the promise when there is an error', () => {
      const id = Uuid.random();
      const queryAndParameters = [
        { query: insertQuery1, params: [ id, 0, 'one on table1'] },
        { query: 'INSERT FAIL', params: []},
        { query: insertQuery1, params: [ id, 1, 'second on table1'] }
      ];
      let error;

      return executeConcurrent(client, queryAndParameters)
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, errors.ResponseError);
          assert.strictEqual(error.code, types.responseErrorCodes.syntaxError);
        });
    });

    it('should resolve the promise when there is an error and raiseOnFirstError is false', () => {
      const id = Uuid.random();
      const queryAndParameters = [
        { query: insertQuery1, params: [ id, 0, 'one on table1'] },
        { query: 'INSERT FAIL', params: []},
        { query: insertQuery1, params: [ id, 1, 'second on table1'] }
      ];

      return executeConcurrent(client, queryAndParameters, { raiseOnFirstError: false })
        .then(result => {
          assert.strictEqual(result.totalExecuted, queryAndParameters.length);
          assert.strictEqual(result.errors.length, 1);
          helper.assertInstanceOf(result.errors[0], errors.ResponseError);
        })
        .then(() => validateInserted(client, id, 2));
    });
  });
});

function getParameterValues(id, length) {
  return Array.from(new Array(length).keys()).map(x => [ id, x, x.toString() ]);
}

function validateInserted(client, id, totalExpected) {
  return client.execute(countQuery1, [ id ])
    .then(rs => assert.equal(rs.first()['total'].toString(), totalExpected));
}

/**
 * Simplified line reader for testing purposes
 */
class LineTransform extends Transform {

  constructor(id, options) {
    super({ writableObjectMode: true, readableObjectMode: true });

    this._id = id;
    options = options || {};
    this._failAtIndex = options.failAtIndex;
    this._fixedValues = options.fixedValues || new Map();
    this.index = 0;
    this.pauseCounter = 0;
  }

  _transform(chunk, encoding, callback) {
    const text = chunk.toString();
    const parts = text.split('\n');

    for (let i = 0; i < parts.length; i++) {
      const index = this.index++;
      const values = this._fixedValues.get(index) || [ this._id, index, parts[i] ];
      this.push(values);

      if (this.index === this._failAtIndex) {
        return callback(new Error('Test transform error'));
      }
    }

    callback();
  }

  pause() {
    this.pauseCounter++;
    super.pause();
  }
}