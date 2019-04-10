'use strict';

const assert = require('assert');
const fs = require('fs');
const types = require('../../../../lib/types');
const errors = require('../../../../lib/errors');
const helper = require('../../../test-helper');
const Transform = require('stream').Transform;
const Uuid = types.Uuid;
const executeConcurrent = require('../../../../lib/concurrent').executeConcurrent;

const insertQuery = 'INSERT INTO table1 (key1, key2, value) VALUES (?, ?, ?)';
const countQuery = 'SELECT COUNT(*) as total FROM table1 WHERE key1 = ?';

describe('executeConcurrent()', function () {
  this.timeout(120000);

  const setupInfo = helper.setup(1, {
    queries: [
      'CREATE TABLE table1 (key1 uuid, key2 int, value text, PRIMARY KEY (key1, key2))',
      'CREATE TABLE table2 (id text PRIMARY KEY, value text)',
    ]
  });

  const client = setupInfo.client;

  describe('with fixed query and parameters', () => {
    it('should insert hundreds of rows', () => {
      const id = Uuid.random();
      const values = Array.from(new Array(600).keys()).map(x => [ id, x, x.toString() ]);

      return executeConcurrent(client, insertQuery, values)
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors, []);
        })
        .then(() => validateInserted(client, id, values.length));
    });

    it('should not store individual results', () => {
      const id = Uuid.random();
      const values = getParameterValues(id, 10);

      return executeConcurrent(client, insertQuery, values)
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

      return executeConcurrent(client, insertQuery, values, { concurrencyLevel: 5, collectResults: true })
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

      return executeConcurrent(client, insertQuery, values, options)
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

      return executeConcurrent(client, insertQuery, values)
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

      return executeConcurrent(client, insertQuery, values, { raiseOnFirstError: false, concurrencyLevel: 10 })
        .then(result => {
          assert.strictEqual(result.totalExecuted, values.length);
          assert.deepStrictEqual(result.errors.length, 2);
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

      return executeConcurrent(client, insertQuery, transformStream, { concurrencyLevel: 10 })
        .then(result => {
          assert.strictEqual(result.totalExecuted, transformStream.index);
          assert.deepStrictEqual(result.errors, []);
        })
        .then(() => validateInserted(client, id, transformStream.index));
    });
  });
});

function getParameterValues(id, length) {
  return Array.from(new Array(length).keys()).map(x => [ id, x, x.toString() ]);
}

function validateInserted(client, id, totalExpected) {
  return client.execute(countQuery, [ id ])
    .then(rs => assert.equal(rs.first()['total'].toString(), totalExpected));
}

/**
 * Simplified line reader for testing purposes
 */
class LineTransform extends Transform {
  constructor(id) {
    super({ writableObjectMode: true, readableObjectMode: true });

    this._id = id;
    this.index = 0;
  }

  _transform(chunk, encoding, callback) {
    const text = chunk.toString();
    const parts = text.split('\n');

    for (let i = 0; i < parts.length; i++) {
      this.push([ this._id, this.index++, parts[i] ]);
    }

    callback();
  }
}