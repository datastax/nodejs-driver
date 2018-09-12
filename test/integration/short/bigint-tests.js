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
      keyspace,
      encoding: { useBigIntAsVarint: true, useBigIntAsLong: true, set: Set }
    });

    before(() => client.connect());
    before(() => client.execute(
      `CREATE TABLE tbl_integers (id1 text, id2 bigint, varint1 varint, list1 list<bigint>, set1 set<bigint>,
         set2 set<varint>, PRIMARY KEY ((id1, id2)))`));
    after(() => client.shutdown());

    const textValues = [
      '1', '0', '-1', '-256', '281474976710655', '109951162777712341', '-281474976710655', '-9223372036854775808',
      '9223372036854775807', '-73372036854775999', '4294967295', '-2147483648', '2147483647'
    ];

    it('should insert and retrieve BigInt type values', () =>
      // Test with different values
      Promise.all(textValues.map(textValue => {
        const insertQuery = 'INSERT INTO tbl_integers (id1, id2, varint1, list1, set1, set2)' +
          ' VALUES (?, ?, ?, ?, ?, ?)';
        const selectQuery = 'SELECT * FROM tbl_integers WHERE id1 = ? AND id2 = ?';
        const hints = !prepare ? ['text', 'bigint', 'varint', 'list<bigint>', 'set<bigint>', 'set<varint>'] : null;
        const value = BigInt(textValue);
        const params = [textValue, value, null, [value, value], new Set([value]), null];

        return client.execute(insertQuery, params, { hints, prepare })
          .then(() => client.execute(selectQuery, [ textValue, value ], { hints, prepare }))
          .then(rs => {
            const row = rs.first();
            assert.strictEqual(row['id1'], value.toString());
            assert.strictEqual(row['id2'], value);
            assert.deepStrictEqual(row['list1'], [value, value]);
            assert.deepStrictEqual(row['set1'], new Set([value]));
          });
      })));
  });
};