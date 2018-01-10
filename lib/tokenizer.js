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
   * @param {Token} value
   */
  parse(value) {
    throw new Error('You must implement a parse function for the tokenizer');
  }

  minToken() {
    throw new Error('You must implement a minToken function for the tokenizer');
  }

  /**
   * Returns 0 if the values are equal, 1 if val1 is greater then val2 and -1 if val2 is greater than val1
   * @param val1
   * @param val2
   * @returns {number}
   */
  compare(val1, val2) {
    if (val1 > val2) {
      return 1;
    }
    if (val1 < val2) {
      return -1;
    }
    return 0;
  }

  stringify(value) {
    return value.toString();
  }
}

/**
 * Uniformly distributes data across the cluster based on Cassandra flavored Murmur3 hashed values.
 */
class Murmur3Tokenizer extends Tokenizer {
  constructor() {
    super();
  }

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

    return new token.Murmur3Token(h1, this);
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
   * @returns {MutableLong}
   */
  parse(value) {
    return new token.Murmur3Token(MutableLong.fromString(value), this);
  }

  /**
   * @param {MutableLong} val1
   * @param {MutableLong} val2
   * @returns {number}
   */
  compare(val1, val2) {
    return val1.compare(val2);
  }

  /**
   * @param {MutableLong} value
   * @return {String}
   */
  stringify(value) {
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
   * @returns {Integer}
   */
  hash(value) {
    if (util.isArray(value)) {
      value = utils.allocBufferFromArray(value);
    }
    const hashedValue = this._crypto.createHash('md5').update(value).digest();
    return new token.RandomToken(Integer.fromBuffer(hashedValue).abs(), this);
  }

  /**
   * @returns {Integer}
   */
  parse(value) {
    return new token.RandomToken(Integer.fromString(value), this);
  }

  /**
   * @param {Integer} val1
   * @param {Integer} val2
   * @returns {number}
   */
  compare(val1, val2) {
    return val1.compare(val2);
  }
}

class ByteOrderedTokenizer extends Tokenizer {
  constructor() {
    super();
  }

  /**
   * @param {Buffer|Array} value
   * @returns {Buffer}
   */
  hash(value) {
    return new token.ByteOrderedToken(value, this);
  }

  stringify(value) {
    return value.toString('hex');
  }

  parse(value) {
    return new token.ByteOrderedToken(utils.allocBufferFromString(value), this);
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
