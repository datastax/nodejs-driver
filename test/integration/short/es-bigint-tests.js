'use strict';

const assert = require('assert');
const Client = require('../../../lib/client');
const helper = require('../../test-helper');

// Exported to be called on other fixtures to take advantage from existing setups
module.exports = function (keyspace, prepare) {
  if (typeof BigInt === 'undefined') {
    return;
  }

  context('when BigInt is supported by the engine', () => {
    const client = new Client({
      contactPoints: helper.baseOptions.contactPoints,
      localDataCenter: helper.baseOptions.localDataCenter,
      keyspace,
      encoding: { useBigIntAsVarint: true, useBigIntAsLong: true, set: Set }
    });

    before(() => client.connect());
    before(() => client.execute(
      `CREATE TABLE tbl_integers (id1 text, id2 bigint, varint1 varint, list1 list<bigint>, set1 set<bigint>,
         set2 set<varint>, PRIMARY KEY ((id1, id2)))`));
    after(() => client.shutdown());

    const int64TextValues = [
      '1', '0', '-1', '-128', '-256', '281474976710655', '109951162777712341', '-281474976710655',
      '-9223372036854775808', '9223372036854775807', '-73372036854775999', '4294967295', '-2147483648', '2147483647'
    ];

    const varintTextValues = [
      '309485009821345068724781055', '-309485009821345068724781055', '10071880159625853916579273765154',
      '-10071880159625853916579273765154', '10151108322140118254172817715490', '-10151108322140118254172817715490'
    ];

    it('should insert and retrieve BigInt type values', () =>
      Promise.all(int64TextValues.map(textValue => {
        const insertQuery = 'INSERT INTO tbl_integers (id1, id2, varint1, list1, set1, set2)' +
          ' VALUES (?, ?, ?, ?, ?, ?)';
        const selectQuery = 'SELECT * FROM tbl_integers WHERE id1 = ? AND id2 = ?';
        const hints = !prepare ? ['text', 'bigint', 'varint', 'list<bigint>', 'set<bigint>', 'set<varint>'] : null;
        const value = BigInt(textValue);
        const params = [textValue, value, value, [value, value], new Set([value]), new Set([value])];

        return client.execute(insertQuery, params, { hints, prepare })
          .then(() => client.execute(selectQuery, [ textValue, value ], { hints, prepare }))
          .then(rs => {
            const row = rs.first();
            assert.strictEqual(row['id1'], value.toString());
            assert.strictEqual(row['id2'], value);
            assert.strictEqual(row['varint1'], value);
            assert.deepStrictEqual(row['list1'], [value, value]);
            assert.deepStrictEqual(row['set1'], new Set([value]));
            assert.deepStrictEqual(row['set2'], new Set([value]));
          });
      })));

    it('should insert and retrieve varint values larger than 64bits as BigInt', () =>
      Promise.all(varintTextValues.map(textValue => {
        const insertQuery = 'INSERT INTO tbl_integers (id1, id2, varint1, set2) VALUES (?, ?, ?, ?)';
        const selectQuery = 'SELECT * FROM tbl_integers WHERE id1 = ? AND id2 = ?';
        const hints = !prepare ? ['text', 'bigint', 'varint', 'set<varint>'] : null;
        const value = BigInt(textValue);
        const params = [textValue, BigInt(0), value, new Set([value])];

        return client.execute(insertQuery, params, { hints, prepare })
          .then(() => client.execute(selectQuery, [ textValue, BigInt(0) ], { hints, prepare }))
          .then(rs => {
            const row = rs.first();
            assert.strictEqual(row['id1'], value.toString());
            assert.strictEqual(row['varint1'], value);
            assert.deepStrictEqual(row['set2'], new Set([value]));
          });
      })));
  });
};