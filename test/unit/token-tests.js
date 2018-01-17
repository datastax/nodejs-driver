'use strict';

const tokenizer = require('../../lib/tokenizer');
const token = require('../../lib/token');
const assert = require('assert');

class TokenTester {
  constructor(tokenizer) {
    this.tokenizer = tokenizer;
  }

  rangeSplitTester(start, end, numberOfSplits, expectedRanges) {
    return () => {
      const startToken = this.tokenizer.parse(start);
      const endToken = this.tokenizer.parse(end);
      const range = new token.TokenRange(startToken, endToken, this.tokenizer);
      const splits = range.splitEvenly(numberOfSplits);
      assert.strictEqual(splits.length, numberOfSplits);
      expectedRanges.forEach((split, index) => {
        const eStart = this.tokenizer.parse(split[0]);
        const eEnd = this.tokenizer.parse(split[1]);
        const eRange = new token.TokenRange(eStart, eEnd, this.tokenizer);
        assert.ok(splits[index].equals(eRange));
      });
    };
  }
}

describe('Murmur3Token', () => {
  const _tokenizer = new tokenizer.Murmur3Tokenizer();
  const tester = new TokenTester(_tokenizer);
  describe('#splitEvenly()', () => {
    it('should split range', tester.rangeSplitTester('-9223372036854775808', '4611686018427387904', 3, [
      ['-9223372036854775808', '-4611686018427387904'], 
      ['-4611686018427387904', '0'],
      ['0', '4611686018427387904']
    ]));
    it('should split range that wraps around the ring', tester.rangeSplitTester('4611686018427387904', '0', 3, [
      ['4611686018427387904', '-9223372036854775807'],
      ['-9223372036854775807', '-4611686018427387903'],
      ['-4611686018427387903', '0']
    ]));
    it('should split range when division not integral', tester.rangeSplitTester('0', '11', 3, [
      ['0', '4'],
      ['4', '8'],
      ['8', '11']
    ]));
    it('should split range producing empty splits', tester.rangeSplitTester('0', '2', 5, [
      ['0', '1'],
      ['1', '2'],
      ['2', '2'],
      ['2', '2'],
      ['2', '2']
    ]));
    it('should split range producing empty splits near ring end', tester.rangeSplitTester('9223372036854775807', '-9223372036854775808', 3, [
      ['9223372036854775807', '9223372036854775807'],
      ['9223372036854775807', '9223372036854775807'],
      ['9223372036854775807', '-9223372036854775808']
    ]));
    it('should split whole ring', tester.rangeSplitTester('-9223372036854775808', '-9223372036854775808', 3, [
      ['-9223372036854775808', '-3074457345618258603'],
      ['-3074457345618258603', '3074457345618258602'],
      ['3074457345618258602', '-9223372036854775808']
    ]));
  });
});

describe('RandomToken', () => {
  const _tokenizer = new tokenizer.RandomTokenizer();
  const tester = new TokenTester(_tokenizer);
  describe('#splitEvenly()', () => {
    it('should split range', tester.rangeSplitTester('0', '127605887595351923798765477786913079296', 3, [
      ['0', '42535295865117307932921825928971026432'],
      ['42535295865117307932921825928971026432', '85070591730234615865843651857942052864'],
      ['85070591730234615865843651857942052864', '127605887595351923798765477786913079296']
    ]));
  });
});