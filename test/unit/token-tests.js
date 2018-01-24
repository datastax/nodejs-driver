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
        assert.deepEqual(splits[index], eRange);
        // validate equals method
        assert.ok(splits[index].equals(eRange));
      });
    };
  }
}

describe('TokenRange', () => {
  const _tokenizer = new tokenizer.Murmur3Tokenizer();
  describe('with Murmur3Token', () => {
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
      it('should split whole ring using minToken', tester.rangeSplitTester('-9223372036854775808', '-9223372036854775808', 3, [
        ['-9223372036854775808', '-3074457345618258603'],
        ['-3074457345618258603', '3074457345618258602'],
        ['3074457345618258602', '-9223372036854775808']
      ]));
    });
    describe('#compare()', () => {
      const token0 = _tokenizer.parse('0');
      const token1 = _tokenizer.parse('-4611686018427387903');
      const token2 = _tokenizer.parse('-4611686018427387902');
      const range1 = new token.TokenRange(token1, token0, _tokenizer);
      const range2 = new token.TokenRange(token2, token0, _tokenizer);
      const range3 = new token.TokenRange(token0, token1, _tokenizer);
      const range4 = new token.TokenRange(token0, token2, _tokenizer);
      it('should return -1 if start less than other.start', () => {
        assert.strictEqual(range1.compare(range2), -1);
      });
      it('should return 1 if start greater than other.start', () => {
        assert.strictEqual(range2.compare(range1), 1);
      });
      it('should return -1 if start the same and end less than other.end', () => {
        assert.strictEqual(range3.compare(range4), -1);
      });
      it('should return 1 if start the same and end greater than other.end', () => {
        assert.strictEqual(range4.compare(range3), 1);
      });
      it('should return 0 if start and end the same', () => {
        assert.strictEqual(range1.compare(range1), 0);
      });
    });
  });

  describe('with RandomToken', () => {
    const _tokenizer = new tokenizer.RandomTokenizer();
    const tester = new TokenTester(_tokenizer);
    describe('#splitEvenly()', () => {
      it('should split range', tester.rangeSplitTester('0', '127605887595351923798765477786913079296', 3, [
        ['0', '42535295865117307932921825928971026432'],
        ['42535295865117307932921825928971026432', '85070591730234615865843651857942052864'],
        ['85070591730234615865843651857942052864', '127605887595351923798765477786913079296']
      ]));
      it('should split whole ring using minToken', tester.rangeSplitTester('-1', '-1', 3, [
        ['-1', '56713727820156410577229101238628035242'],
        ['56713727820156410577229101238628035242', '113427455640312821154458202477256070485'],
        ['113427455640312821154458202477256070485', '-1'] 
      ]));
    });
    describe('#compare()', () => {
      const token0 = _tokenizer.parse('0');
      const token1 = _tokenizer.parse('-1');
      const token2 = _tokenizer.parse('113427455640312821154458202477256070485');
      const range1 = new token.TokenRange(token1, token0, _tokenizer);
      const range2 = new token.TokenRange(token2, token0, _tokenizer);
      const range3 = new token.TokenRange(token0, token1, _tokenizer);
      const range4 = new token.TokenRange(token0, token2, _tokenizer);
      it('should return -1 if start less than other.start', () => {
        assert.strictEqual(range1.compare(range2), -1);
      });
      it('should return 1 if start greater than other.start', () => {
        assert.strictEqual(range2.compare(range1), 1);
      });
      it('should return -1 if start the same and end less than other.end', () => {
        assert.strictEqual(range3.compare(range4), -1);
      });
      it('should return 1 if start the same and end greater than other.end', () => {
        assert.strictEqual(range4.compare(range3), 1);
      });
      it('should return 0 if start and end the same', () => {
        assert.strictEqual(range1.compare(range1), 0);
      });
    });
  });

  describe('with ByteOrderedToken', () => {
    const _tokenizer = new tokenizer.ByteOrderedTokenizer();
    const tester = new TokenTester(_tokenizer);

    function hex(str) {
      return Buffer.from(str, 'utf8').toString('hex');
    }

    describe('#splitEvenly()', () => {
      it('should split range', tester.rangeSplitTester(hex('a'), hex('d'), 3, [
        [ hex('a'), hex('b') ],
        [ hex('b'), hex('c') ],
        [ hex('c'), hex('d') ]
      ]));
      // ring start should be 0x64 (d), end should be 0x61 (a) spanned over a length of 253 (2^8 - 3 byte distance between d and a)
      // Each range should be 256 / 3 = 84 tokens, with 1 remainder assigned to first range.
      // Expect 3 ranges, 85, 84, and 84.
      // 100 (0x64) + 85 == 185 (0xB9)  : ]64,B9]
      // 0xFF - 0xB9 = 70, this leaves 14 tokens including 0, which gives us 13 (0xD): ]B9,0D]
      // 0x61 - 0x0D == 84 tokens. ]0D,61]
      it('should split range that wraps around the ring', tester.rangeSplitTester(hex('d'), hex('a'), 3, [
        [ hex('d'), 'B9' ],
        [ 'B9', '0D' ],
        [ '0D', hex('a') ]
      ]));
      it('should not allow split on minToken', () => {
        const range = new token.TokenRange(_tokenizer.minToken(), _tokenizer.minToken(), _tokenizer);
        assert.throws(() => range.splitEvenly(2), 'Cannot split whole ring with ordered partitioner');
      });
    });
    describe('#compare()', () => {
      const token0 = _tokenizer.parse('00');
      const token1 = _tokenizer.parse('0113427455640312821154458202477256070484');
      const token2 = _tokenizer.parse('56713727820156410577229101238628035242');
      const range1 = new token.TokenRange(token1, token0, _tokenizer);
      const range2 = new token.TokenRange(token2, token0, _tokenizer);
      const range3 = new token.TokenRange(token0, token1, _tokenizer);
      const range4 = new token.TokenRange(token0, token2, _tokenizer);
      it('should return -1 if start less than other.start', () => {
        assert.strictEqual(range1.compare(range2), -1);
      });
      it('should return 1 if start greater than other.start', () => {
        assert.strictEqual(range2.compare(range1), 1);
      });
      it('should return -1 if start the same and end less than other.end', () => {
        assert.strictEqual(range3.compare(range4), -1);
      });
      it('should return 1 if start the same and end greater than other.end', () => {
        assert.strictEqual(range4.compare(range3), 1);
      });
      it('should return 0 if start and end the same', () => {
        assert.strictEqual(range1.compare(range1), 0);
      });
    });
  });
  const token0 = _tokenizer.parse('4611686018427387904');
  const token1 = _tokenizer.parse('4611686018427387906');
  const minToken = _tokenizer.minToken();
  describe('#isEmpty()', () => {
    it('should return true when start === end', () => {
      const range = new token.TokenRange(token0, token0, _tokenizer);
      assert.strictEqual(range.isEmpty(), true);
    });
    it('should return true when start !== end', () => {
      const range = new token.TokenRange(token0, token1, _tokenizer);
      assert.strictEqual(range.isEmpty(), false);
    });
    it('should return false when start and end are minToken', () => {
      const range = new token.TokenRange(minToken, minToken, _tokenizer);
      assert.strictEqual(range.isEmpty(), false);
    });
  });
  describe('#isWrappedAround()', () => {
    it('should return true if start > end', () => {
      const range = new token.TokenRange(token1, token0, _tokenizer);
      assert.strictEqual(range.isWrappedAround(), true);
    });
    it('should return false if start === end', () => {
      const range = new token.TokenRange(token0, token0, _tokenizer);
      assert.strictEqual(range.isWrappedAround(), false);
    });
    it('should return false if start < end', () => {
      const range = new token.TokenRange(token0, token1, _tokenizer);
      assert.strictEqual(range.isWrappedAround(), false);
    });
    it('should return false if start > end, end is minToken', () => {
      const range = new token.TokenRange(token0, minToken, _tokenizer);
      assert.strictEqual(range.isWrappedAround(), false);
    });
  });
  describe('#unwrap()', () => {
    it('should return input range is not wrapped around ring', () => {
      const range = new token.TokenRange(token0, token1, _tokenizer);
      const unwrapped = range.unwrap();
      assert.strictEqual(unwrapped.length, 1);
      assert.strictEqual(unwrapped[0], range);
    });
    it('should return two ranges when range is wrapped around ring', () => {
      const range = new token.TokenRange(token1, token0, _tokenizer);
      const unwrapped = range.unwrap();
      assert.strictEqual(unwrapped.length, 2);
      // should be split into two ranges of ]token1,minToken] and ]minToken,token0]
      const expectedRange0 = new token.TokenRange(token1, minToken, _tokenizer);
      const expectedRange1 = new token.TokenRange(minToken, token0, _tokenizer);
      assert.deepEqual(unwrapped[0], expectedRange0);
      assert.deepEqual(unwrapped[1], expectedRange1);
    });
  });
  describe('#contains()', () => {
    it('should return false if range is empty', () => {
      const range = new token.TokenRange(token0, token0, _tokenizer);
      assert.strictEqual(range.contains(token1), false);
    });
    it('should return true is range covers entire ring', () => {
      const range = new token.TokenRange(minToken, minToken, _tokenizer);
      assert.strictEqual(range.contains(token1), true);
      assert.strictEqual(range.contains(token0), true);
      assert.strictEqual(range.contains(minToken), true);
    });
    it('should return true if > start and end is minToken', () => {
      const range = new token.TokenRange(token0, minToken, _tokenizer);
      assert.strictEqual(range.contains(token1), true);
      assert.strictEqual(range.contains(minToken), true);
    });
    it('should return false if === start and end is minToken', () => {
      const range = new token.TokenRange(token0, minToken, _tokenizer);
      assert.strictEqual(range.contains(token0), false);
    });
    it('should return false if < start and end is minToken', () => {
      const range = new token.TokenRange(token1, minToken, _tokenizer);
      assert.strictEqual(range.contains(token0), false);
    });
    it('should return false if === start', () => {
      const range = new token.TokenRange(token0, token1, _tokenizer);
      assert.strictEqual(range.contains(token0), false);
    });
    describe('when range is not wrapped around the ring', () => {
      it('should return false if < start', () => {
        const range = new token.TokenRange(token1, minToken, _tokenizer);
        assert.strictEqual(range.contains(token0), false);
      });
      it('should return true if > start and < end', () => {
        const range = new token.TokenRange(token0, token1, _tokenizer);
        const middleToken = _tokenizer.parse('4611686018427387905');
        assert.strictEqual(range.contains(middleToken), true);
      });
    });
    describe('when range is wrapped around the ring', () => {
      it('should return true if > start', () => {
        const range = new token.TokenRange(token0, minToken, _tokenizer);
        assert.strictEqual(range.contains(token1), true);
      });
      it('should return true if < start and end', () => {
        const range = new token.TokenRange(token1, token0, _tokenizer);
        assert.strictEqual(range.contains(minToken), true);
      });
      it('should return false if < start and > end', () => {
        const range = new token.TokenRange(token1, token0, _tokenizer);
        const middleToken = _tokenizer.parse('4611686018427387905');
        assert.strictEqual(range.contains(middleToken), false);
      });
    });
  });
  describe('#equals()', () => {
    it('should return true if same object', () => {
      const range = new token.TokenRange(token1, token0, _tokenizer);
      assert.strictEqual(range.equals(range), true);
    });
    it('should return true if start and end are equal with other range', () => {
      const range0 = new token.TokenRange(token1, token0, _tokenizer);
      const range1 = new token.TokenRange(token1, token0, _tokenizer);
      assert.strictEqual(range0.equals(range1), true);
    });
    it('should return false if start and end are not equal with other range', () => {
      const range0 = new token.TokenRange(token0, token1, _tokenizer);
      const range1 = new token.TokenRange(token1, token0, _tokenizer);
      assert.strictEqual(range0.equals(range1), false);
    });
  });
  describe('#toString()', () => {
    it('should produce in format of ]start,end]', () => {
      const range0 = new token.TokenRange(token0, token1, _tokenizer);
      assert.strictEqual(range0.toString(), ']4611686018427387904, 4611686018427387906]');
    });
  });
});