'use strict';

var util = require('util');
var types = require('./types');
var utils = require('./utils');
var MutableLong = require('./types/mutable-long');
var Integer = types.Integer;

// Murmur3 constants
//-0x783C846EEEBDAC2B
var mconst1 = new MutableLong(0x53d5, 0x1142, 0x7b91, 0x87c3);
//0x4cf5ad432745937f
var mconst2 = new MutableLong(0x937f, 0x2745, 0xad43, 0x4cf5);
var mlongFive = MutableLong.fromNumber(5);
//0xff51afd7ed558ccd
var mconst3 = new MutableLong(0x8ccd, 0xed55, 0xafd7, 0xff51);
//0xc4ceb9fe1a85ec53
var mconst4 = new MutableLong(0xec53, 0x1a85, 0xb9fe, 0xc4ce);
var mconst5 = MutableLong.fromNumber(0x52dce729);
var mconst6 = MutableLong.fromNumber(0x38495ab5);

/**
 * Represents a set of methods that are able to generate and parse tokens for the C* partitioner
 * @constructor
 */
function Tokenizer() {

}

//noinspection JSUnusedLocalSymbols
/**
 * Creates a token based on the Buffer value provided
 * @param {Buffer|Array} value
 */
Tokenizer.prototype.hash = function (value) {
  throw new Error('You must implement a hash function for the tokenizer');
};

//noinspection JSUnusedLocalSymbols
/**
 * Parses a token string and returns a representation of the token
 * @param {String} value
 */
Tokenizer.prototype.parse = function (value) {
  throw new Error('You must implement a parse function for the tokenizer');
};

/**
 * Returns 0 if the values are equal, 1 if val1 is greater then val2 and -1 if val2 is greater than val1
 * @param val1
 * @param val2
 * @returns {number}
 */
Tokenizer.prototype.compare = function (val1, val2) {
  if (val1 > val2) {
    return 1;
  }
  if (val1 < val2) {
    return -1;
  }
  return 0;
};

Tokenizer.prototype.stringify = function (value) {
  return value.toString();
};

/**
 * Uniformly distributes data across the cluster based on Cassandra flavored Murmur3 hashed values.
 * @constructor
 */
function Murmur3Tokenizer() {

}

util.inherits(Murmur3Tokenizer, Tokenizer);

Murmur3Tokenizer.prototype.hash = function hash(value) {
  // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
  // for M3P. Compared to that methods, there's a few inlining of arguments and we
  // only return the first 64-bits of the result since that's all M3 partitioner uses.

  var data = value;
  var offset = 0;
  var length = data.length;

  var nblocks = length >> 4; // Process as 128-bit blocks.

  var h1 = new MutableLong();
  var h2 = new MutableLong();
  var k1 = new MutableLong();
  var k2 = new MutableLong();

  for (var i = 0; i < nblocks; i++) {
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
      k2.xor(fromSignedByte(data[offset+12]).shiftLeft(24));
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

  return h1;
};

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

/**
 *
 * @param {Array<Number>} key
 * @param {Number} offset
 * @param {Number} index
 * @return {MutableLong}
 */
Murmur3Tokenizer.prototype.getBlock = function (key, offset, index) {
  var i8 = index << 3;
  var blockOffset = offset + i8;
  return new MutableLong(
    (key[blockOffset]) | (key[blockOffset + 1] << 8),
    (key[blockOffset + 2]) | (key[blockOffset + 3] << 8),
    (key[blockOffset + 4]) | (key[blockOffset + 5] << 8),
    (key[blockOffset + 6]) | (key[blockOffset + 7] << 8)
  );
};

/**
 * @param {MutableLong} v
 * @param {Number} n
 */
Murmur3Tokenizer.prototype.rotl64 = function (v, n) {
  var left = v.clone().shiftLeft(n);
  v.shiftRightUnsigned(64 - n).or(left);
};

/** @param {MutableLong} k */
Murmur3Tokenizer.prototype.fmix = function (k) {
  k.xor(new MutableLong(k.getUint16(2) >>> 1 | ((k.getUint16(3) << 15) & 0xffff), k.getUint16(3) >>> 1, 0, 0));
  k.multiply(mconst3);
  var other = new MutableLong(
    (k.getUint16(2) >>> 1) | ((k.getUint16(3) << 15) & 0xffff),
    k.getUint16(3) >>> 1,
    0,
    0
  );
  k.xor(other);
  k.multiply(mconst4);
  k.xor(new MutableLong(k.getUint16(2) >>> 1 | (k.getUint16(3) << 15 & 0xffff), k.getUint16(3) >>> 1, 0, 0));
};

/**
 * Parses a int64 decimal string representation into a MutableLong.
 * @param {String} value
 * @returns {MutableLong}
 */
Murmur3Tokenizer.prototype.parse = function (value) {
  return MutableLong.fromString(value);
};

/**
 * @param {MutableLong} val1
 * @param {MutableLong} val2
 * @returns {number}
 */
Murmur3Tokenizer.prototype.compare = function (val1, val2) {
  return val1.compare(val2);
};

/**
 * @param {MutableLong} value
 * @return {String}
 */
Murmur3Tokenizer.prototype.stringify = function (value) {
  // We need a way to uniquely represent a token, it doesn't have to be the decimal string representation
  // Using the uint16 avoids divisions and other expensive operations on the longs
  return value.getUint16(0) + ',' + value.getUint16(1) + ',' + value.getUint16(2) + ',' + value.getUint16(3);
};

/**
 * Uniformly distributes data across the cluster based on MD5 hash values.
 * @constructor
 */
function RandomTokenizer() {
  // eslint-disable-next-line
  this._crypto = require('crypto');
}

util.inherits(RandomTokenizer, Tokenizer);

/**
 * @param {Buffer|Array} value
 * @returns {Integer}
 */
RandomTokenizer.prototype.hash = function (value) {
  if (util.isArray(value)) {
    value = utils.allocBufferFromArray(value);
  }
  var hashedValue = this._crypto.createHash('md5').update(value).digest();
  return Integer.fromBuffer(hashedValue).abs();
};

/**
 * @returns {Integer}
 */
RandomTokenizer.prototype.parse = function (value) {
  return Integer.fromString(value);
};

/**
 * @param {Integer} val1
 * @param {Integer} val2
 * @returns {number}
 */
RandomTokenizer.prototype.compare = function (val1, val2) {
  return val1.compare(val2);
};

function ByteOrderedTokenizer() {

}

util.inherits(ByteOrderedTokenizer, Tokenizer);

/**
 * @param {Buffer|Array} value
 * @returns {Buffer}
 */
ByteOrderedTokenizer.prototype.hash = function (value) {
  return value;
};

ByteOrderedTokenizer.prototype.stringify = function (value) {
  return value.toString('hex');
};

ByteOrderedTokenizer.prototype.parse = function (value) {
  return utils.allocBufferFromString(value);
};

exports.Murmur3Tokenizer = Murmur3Tokenizer;
exports.RandomTokenizer = RandomTokenizer;
exports.ByteOrderedTokenizer = ByteOrderedTokenizer;
