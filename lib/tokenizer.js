'use strict';

const util = require('util');
const types = require('./types');
const token = require('./token');
const utils = require('./utils');
const MutableLong = require('./types/mutable-long');
const Integer = types.Integer;

// Murmur3 constants
//-0x783C846EEEBDAC2B
const mconst1 = new MutableLong(0x53d5, 0x1142, 0x7b91, 0x87c3);
//0x4cf5ad432745937f
const mconst2 = new MutableLong(0x937f, 0x2745, 0xad43, 0x4cf5);
const mlongFive = MutableLong.fromNumber(5);
//0xff51afd7ed558ccd
const mconst3 = new MutableLong(0x8ccd, 0xed55, 0xafd7, 0xff51);
//0xc4ceb9fe1a85ec53
const mconst4 = new MutableLong(0xec53, 0x1a85, 0xb9fe, 0xc4ce);
const mconst5 = MutableLong.fromNumber(0x52dce729);
const mconst6 = MutableLong.fromNumber(0x38495ab5);

/**
 * Represents a set of methods that are able to generate and parse tokens for the C* partitioner.
 * @abstract
 */
class Tokenizer {
  constructor() {

  }

  /**
   * Creates a token based on the Buffer value provided
   * @abstract
   * @param {Buffer|Array} value
   * @returns {Token} Computed token
   */
  hash(value) {
    throw new Error('You must implement a hash function for the tokenizer');
  }

  /**
   * Parses a token string and returns a representation of the token
   * @abstract
   * @param {String} value
   */
  parse(value) {
    throw new Error('You must implement a parse function for the tokenizer');
  }

  minToken() {
    throw new Error('You must implement a minToken function for the tokenizer');
  }

  /**
   * Splits the range specified by start and end into numberOfSplits equal parts.
   * @param {Token} start Starting token
   * @param {Token} end  End token
   * @param {Number} numberOfSplits Number of splits to make.
   */
  split(start, end, numberOfSplits) {
    throw new Error('You must implement a split function for the tokenizer');
  }

  /**
   * Common implementation for splitting token ranges when start is in
   * a shared Integer format.
   *
   * @param {Integer} start Starting token
   * @param {Integer} range How large the range of the split is
   * @param {Integer} ringEnd The end point of the ring so we know where to wrap
   * @param {Integer} ringLength The total size of the ring
   * @param {Number} numberOfSplits The number of splits to make
   * @returns {Array<Integer>} The evenly-split points on the range
   */
  splitBase(start, range, ringEnd, ringLength, numberOfSplits) {
    const numberOfSplitsInt = Integer.fromInt(numberOfSplits);
    const divider = range.divide(numberOfSplitsInt);
    let remainder = range.modulo(numberOfSplitsInt);

    const results = [];
    let current = start;
    const dividerPlusOne = divider.add(Integer.ONE);

    for(let i = 1; i < numberOfSplits; i++) {
      if (remainder.greaterThan(Integer.ZERO)) {
        current = current.add(dividerPlusOne);
      } else {
        current = current.add(divider);
      }
      if (ringLength && current.greaterThan(ringEnd)) {
        current = current.subtract(ringLength);
      }
      results.push(current);
      remainder = remainder.subtract(Integer.ONE);
    }
    return results;
  }

  /**
   * Return internal string based representation of a Token.
   * @param {Token} token 
   */
  stringify(token) {
    return token.getValue().toString();
  }
}

/**
 * Uniformly distributes data across the cluster based on Cassandra flavored Murmur3 hashed values.
 */
class Murmur3Tokenizer extends Tokenizer {

  constructor() {
    super();
  }

  /**
   * @param {Buffer} value
   * @return {Murmur3Token}
   */
  hash(value) {
    // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
    // for M3P. Compared to that methods, there's a few inlining of arguments and we
    // only return the first 64-bits of the result since that's all M3 partitioner uses.

    const data = value;
    let offset = 0;
    const length = data.length;

    const nblocks = length >> 4; // Process as 128-bit blocks.

    const h1 = new MutableLong();
    const h2 = new MutableLong();
    let k1 = new MutableLong();
    let k2 = new MutableLong();

    for (let i = 0; i < nblocks; i++) {
      k1 = this.getBlock(data, offset, i * 2);
      k2 = this.getBlock(data, offset, i * 2 + 1);

      k1.multiply(mconst1);
      this.rotl64(k1, 31);
      k1.multiply(mconst2);

      h1.xor(k1);
      this.rotl64(h1, 27);
      h1.add(h2);
      h1.multiply(mlongFive).add(mconst5);

      k2.multiply(mconst2);
      this.rotl64(k2, 33);
      k2.multiply(mconst1);
      h2.xor(k2);
      this.rotl64(h2, 31);
      h2.add(h1);
      h2.multiply(mlongFive).add(mconst6);
    }
    //----------
    // tail

    // Advance offset to the unprocessed tail of the data.
    offset += nblocks * 16;

    k1 = new MutableLong();
    k2 = new MutableLong();

    /* eslint-disable no-fallthrough */
    switch(length & 15) {
      case 15:
        k2.xor(fromSignedByte(data[offset+14]).shiftLeft(48));
      case 14:
        k2.xor(fromSignedByte(data[offset+13]).shiftLeft(40));
      case 13:
        k2.xor(fromSignedByte(data[offset+12]).shiftLeft(32));
      case 12:
        k2.xor(fromSignedByte(data[offset+11]).shiftLeft(24));
      case 11:
        k2.xor(fromSignedByte(data[offset+10]).shiftLeft(16));
      case 10:
        k2.xor(fromSignedByte(data[offset+9]).shiftLeft(8));
      case 9:
        k2.xor(fromSignedByte(data[offset+8]));
        k2.multiply(mconst2);
        this.rotl64(k2, 33);
        k2.multiply(mconst1);
        h2.xor(k2);
      case 8:
        k1.xor(fromSignedByte(data[offset+7]).shiftLeft(56));
      case 7:
        k1.xor(fromSignedByte(data[offset+6]).shiftLeft(48));
      case 6:
        k1.xor(fromSignedByte(data[offset+5]).shiftLeft(40));
      case 5:
        k1.xor(fromSignedByte(data[offset+4]).shiftLeft(32));
      case 4:
        k1.xor(fromSignedByte(data[offset+3]).shiftLeft(24));
      case 3:
        k1.xor(fromSignedByte(data[offset+2]).shiftLeft(16));
      case 2:
        k1.xor(fromSignedByte(data[offset+1]).shiftLeft(8));
      case 1:
        k1.xor(fromSignedByte(data[offset]));
        k1.multiply(mconst1);
        this.rotl64(k1,31);
        k1.multiply(mconst2);
        h1.xor(k1);
    }
    /* eslint-enable no-fallthrough */

    h1.xor(MutableLong.fromNumber(length));
    h2.xor(MutableLong.fromNumber(length));

    h1.add(h2);
    h2.add(h1);


    this.fmix(h1);
    this.fmix(h2);

    h1.add(h2);

    return new token.Murmur3Token(h1);
  }

  /**
   *
   * @param {Array<Number>} key
   * @param {Number} offset
   * @param {Number} index
   * @return {MutableLong}
   */
  getBlock(key, offset, index) {
    const i8 = index << 3;
    const blockOffset = offset + i8;
    return new MutableLong(
      (key[blockOffset]) | (key[blockOffset + 1] << 8),
      (key[blockOffset + 2]) | (key[blockOffset + 3] << 8),
      (key[blockOffset + 4]) | (key[blockOffset + 5] << 8),
      (key[blockOffset + 6]) | (key[blockOffset + 7] << 8)
    );
  }

  /**
   * @param {MutableLong} v
   * @param {Number} n
   */
  rotl64(v, n) {
    const left = v.clone().shiftLeft(n);
    v.shiftRightUnsigned(64 - n).or(left);
  }

  /** @param {MutableLong} k */
  fmix(k) {
    k.xor(new MutableLong(k.getUint16(2) >>> 1 | ((k.getUint16(3) << 15) & 0xffff), k.getUint16(3) >>> 1, 0, 0));
    k.multiply(mconst3);
    const other = new MutableLong(
      (k.getUint16(2) >>> 1) | ((k.getUint16(3) << 15) & 0xffff),
      k.getUint16(3) >>> 1,
      0,
      0
    );
    k.xor(other);
    k.multiply(mconst4);
    k.xor(new MutableLong(k.getUint16(2) >>> 1 | (k.getUint16(3) << 15 & 0xffff), k.getUint16(3) >>> 1, 0, 0));
  }

  /**
   * Parses a int64 decimal string representation into a MutableLong.
   * @param {String} value
   * @returns {Murmur3Token}
   */
  parse(value) {
    return new token.Murmur3Token(MutableLong.fromString(value));
  }

  minToken() {
    if (!this._minToken) {
      // minimum long value.
      this._minToken = this.parse('-9223372036854775808');
    }
    return this._minToken;
  }

  maxToken() {
    if (!this._maxToken) {
      this._maxToken = this.parse('9223372036854775807');
    }
    return this._maxToken;
  }

  maxValue() {
    if (!this._maxValue) {
      this._maxValue = Integer.fromString('9223372036854775807');
    }
    return this._maxValue;
  }

  minValue() {
    if (!this._minValue) {
      this._minValue = Integer.fromString('-9223372036854775808');
    }
    return this._minValue;
  }

  ringLength() {
    if (!this._ringLength) {
      this._ringLength = this.maxValue().subtract(this.minValue());
    }
    return this._ringLength;
  }

  split(start, end, numberOfSplits) {
    // ]min, min] means the whole ring.
    if (start.equals(end) && start.equals(this.minToken())) {
      end = this.maxToken();
    }

    const startVal = Integer.fromString(start.getValue().toString());
    const endVal = Integer.fromString(end.getValue().toString());

    let range = endVal.subtract(startVal);
    if (range.isNegative()) {
      range = range.add(this.ringLength());
    }

    const values = this.splitBase(startVal, range, this.maxValue(), this.ringLength(), numberOfSplits);
    return values.map(v => this.parse(v.toString()));
  }

  stringify(token) {
    // Get the underlying MutableLong
    const value = token.getValue();
    // We need a way to uniquely represent a token, it doesn't have to be the decimal string representation
    // Using the uint16 avoids divisions and other expensive operations on the longs
    return value.getUint16(0) + ',' + value.getUint16(1) + ',' + value.getUint16(2) + ',' + value.getUint16(3);
  }
}

/**
 * Uniformly distributes data across the cluster based on MD5 hash values.
 */
class RandomTokenizer extends Tokenizer {
  constructor() {
    super();
    // eslint-disable-next-line
    this._crypto = require('crypto');
  }

  /**
   * @param {Buffer|Array} value
   * @returns {RandomToken}
   */
  hash(value) {
    if (util.isArray(value)) {
      value = utils.allocBufferFromArray(value);
    }
    const hashedValue = this._crypto.createHash('md5').update(value).digest();
    return new token.RandomToken(Integer.fromBuffer(hashedValue).abs());
  }

  /**
   * @returns {Token}
   */
  parse(value) {
    return new token.RandomToken(Integer.fromString(value));
  }

  minToken() {
    if (!this._minToken) {
      this._minToken = this.parse('-1');
    }
    return this._minToken;
  }

  maxValue() {
    if (!this._maxValue) {
      this._maxValue = Integer.fromNumber(Math.pow(2, 127));
    }
    return this._maxValue;
  }

  maxToken() {
    if (!this._maxToken) {
      this._maxToken = new token.RandomToken(this.maxValue());
    }
    return this._maxToken;
  }

  ringLength() {
    if (!this._ringLength) {
      this._ringLength = this.maxValue().add(Integer.ONE);
    }
    return this._ringLength;
  }

  split(start, end, numberOfSplits) {
    // ]min, min] means the whole ring.
    if (start.equals(end) && start.equals(this.minToken())) {
      end = this.maxToken();
    }

    const startVal = start.getValue();
    const endVal = end.getValue();

    let range = endVal.subtract(startVal);
    if (range.lessThan(Integer.ZERO)) {
      range = range.add(this.ringLength());
    }

    const values = this.splitBase(startVal, range, this.maxValue(), this.ringLength(), numberOfSplits);
    return values.map(v => new token.RandomToken(v));
  }
}

class ByteOrderedTokenizer extends Tokenizer {
  constructor() {
    super();
  }

  /**
   * @param {Buffer} value
   * @returns {ByteOrderedToken}
   */
  hash(value) {
    // strip any trailing zeros as tokens with trailing zeros are equivalent
    // to those who don't have them.
    if (util.isArray(value)) {
      value = utils.allocBufferFromArray(value);
    }
    let zeroIndex = value.length;
    for(let i = value.length - 1; i > 0; i--) {
      if(value[i] === 0) {
        zeroIndex = i;
      } else {
        break;
      }
    }
    return new token.ByteOrderedToken(value.slice(0, zeroIndex));
  }

  stringify(token) {
    return token.getValue().toString('hex');
  }

  parse(value) {
    return this.hash(utils.allocBufferFromString(value, 'hex'));
  }

  minToken() {
    if (!this._minToken) {
      this._minToken = this.hash([]);
    }
    return this._minToken;
  }

  _toNumber(buffer, significantBytes) {
    // Convert a token's byte array to a number in order to perform computations.
    // This depends on the number of significant bytes that is used to normalize all tokens
    // to the same size.  For example if the token is 0x01 but significant bytes is 2, the
    // result is 0x0100.
    let target = buffer;
    if(buffer.length !== significantBytes) {
      target = Buffer.alloc(significantBytes);
      buffer.copy(target);
    }

    // similar to Integer.fromBuffer except we force the sign to 0.
    const bits = new Array(Math.ceil(target.length / 4));
    for (let i = 0; i < bits.length; i++) {
      let offset = target.length - ((i + 1) * 4);
      let value;
      if (offset < 0) {
        //The buffer length is not multiple of 4
        offset = offset + 4;
        value = 0;
        for (let j = 0; j < offset; j++) {
          const byte = target[j];
          value = value | (byte << (offset - j - 1) * 8);
        }
      }
      else {
        value = target.readInt32BE(offset);
      }
      bits[i] = value;
    }
    return new Integer(bits, 0);
  }

  _toBuffer(number, significantBytes) {
    // Convert numeric representation back to a buffer.
    const buffer = Integer.toBuffer(number);
    if (buffer.length === significantBytes) {
      return buffer;
    }

    // if first byte is a sign byte, skip it.
    let start, length;
    if (buffer[0] === 0) {
      start = 1;
      length = buffer.length - 1;
    } else {
      start = 0;
      length = buffer.length;
    }

    const target = Buffer.alloc(significantBytes);
    buffer.copy(target, significantBytes - length, start, length + start);
    return target;
  }

  split(start, end, numberOfSplits) {
    const tokenOrder = start.compare(end);

    if (tokenOrder === 0 && start.equals(this.minToken())) {
      throw new Error("Cannot split whole ring with ordered partitioner");
    }

    let startVal, endVal, range, ringLength, ringEnd;
    const intNumberOfSplits = Integer.fromNumber(numberOfSplits);
    // Since tokens are compared lexicographically, convert to numbers using the
    // largest length (i.e. given 0x0A and 0x0BCD, switch to 0x0A00 and 0x0BCD)
    let significantBytes = Math.max(start.getValue().length, end.getValue().length);
    if (tokenOrder < 0) {
      let addedBytes = 0;
      while (true) {
        startVal = this._toNumber(start.getValue(), significantBytes);
        endVal = this._toNumber(end.getValue(), significantBytes);
        range = endVal.subtract(startVal);
        if (addedBytes === 4 || range.compare(intNumberOfSplits) >= 0) {
          break;
        }
        significantBytes += 1;
        addedBytes += 1;
      }
    } else {
      let addedBytes = 0;
      while (true) {
        startVal = this._toNumber(start.getValue(), significantBytes);
        endVal = this._toNumber(end.getValue(), significantBytes);
        ringLength = Integer.fromNumber(Math.pow(2, significantBytes * 8));
        ringEnd = ringLength.subtract(Integer.ONE);
        range = endVal.subtract(startVal).add(ringLength);
        if (addedBytes === 4 || range.compare(intNumberOfSplits) >= 0) {
          break;
        }
        significantBytes += 1;
        addedBytes += 1;
      }
    }

    const values = this.splitBase(startVal, range, ringEnd, ringLength, numberOfSplits);
    return values.map(v => new token.ByteOrderedToken(this._toBuffer(v, significantBytes)));
  }
}

/**
 * @param {Number} value
 * @return {MutableLong}
 */
function fromSignedByte(value) {
  if (value < 128) {
    return new MutableLong(value, 0, 0, 0);
  }
  return new MutableLong((value - 256) & 0xffff, 0xffff, 0xffff, 0xffff);
}

exports.Murmur3Tokenizer = Murmur3Tokenizer;
exports.RandomTokenizer = RandomTokenizer;
exports.ByteOrderedTokenizer = ByteOrderedTokenizer;
