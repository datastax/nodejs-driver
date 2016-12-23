'use strict';
var util = require('util');

var types = require('./types');
var Long = types.Long;
var Integer = types.Integer;

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
 * Uniformly distributes data across the cluster based on Cassandra flavored MurmurHash hash values.
 * @constructor
 */
function Murmur3Tokenizer() {

}

util.inherits(Murmur3Tokenizer, Tokenizer);

/**
 * @param {Buffer|Array} value
 * @returns {Long}
 */
Murmur3Tokenizer.prototype.hash = function (value) {
  // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
  // for M3P. Compared to that methods, there's a few inlining of arguments and we
  // only return the first 64-bits of the result since that's all M3 partitioner uses.

  //Change to Array of signed Int8
  var data = [];
  for (var j = 0; j < value.length; j++)
  {
    var item = value[j];
    if (item > 127) {
      item = item - 256;
    }
    data.push(item);
  }
  var offset = 0;
  var length = data.length;

  var nblocks = length >> 4; // Process as 128-bit blocks.

  var h1 = Long.fromNumber(0);
  var h2 = Long.fromNumber(0);
  var k1 = Long.fromNumber(0);
  var k2 = Long.fromNumber(0);
  //-0x783C846EEEBDAC2B
  var c1 = Long.fromBits(0x114253d5, 0x87c37b91);
  //0x4cf5ad432745937f
  var c2 = Long.fromBits(0x2745937f, 0x4cf5ad43);

  for (var i = 0; i < nblocks; i++) {
    k1 = this.getBlock(data, offset, i * 2 + 0);
    k2 = this.getBlock(data, offset, i * 2 + 1);

    k1 = k1.multiply(c1);
    k1 = this.rotl64(k1, 31);
    k1 = k1.multiply(c2);

    h1 = h1.xor(k1);
    h1 = this.rotl64(h1, 27);
    h1 = h1.add(h2);
    h1 = h1.multiply(Long.fromNumber(5)).add(Long.fromNumber(0x52dce729));

    k2 = k2.multiply(c2);
    k2 = this.rotl64(k2, 33);
    k2 = k2.multiply(c1);
    h2 = h2.xor(k2);
    h2 = this.rotl64(h2, 31);
    h2 = h2.add(h1);
    h2 = h2.multiply(Long.fromNumber(5)).add(Long.fromNumber(0x38495ab5));
  }
  //----------
  // tail

  // Advance offset to the unprocessed tail of the data.
  offset += nblocks * 16;

  k1 = Long.fromNumber(0);
  k2 = Long.fromNumber(0);

  /* eslint-disable no-fallthrough */
  //noinspection FallThroughInSwitchStatementJS
  switch(length & 15) {
    case 15:
      k2 = k2.xor(Long.fromNumber(data[offset+14]).shiftLeft(48));
    case 14:
      k2 = k2.xor(Long.fromNumber(data[offset+13]).shiftLeft(40));
    case 13:
      k2 = k2.xor(Long.fromNumber(data[offset+12]).shiftLeft(32));
    case 12:
      k2 = k2.xor(Long.fromNumber(data[offset+12]).shiftLeft(24));
    case 11:
      k2 = k2.xor(Long.fromNumber(data[offset+10]).shiftLeft(16));
    case 10:
      k2 = k2.xor(Long.fromNumber(data[offset+9]).shiftLeft(8));
    case 9:
      k2 = k2.xor(Long.fromNumber(data[offset+8]));
      k2 = k2.multiply(c2);
      k2 = this.rotl64(k2, 33);
      k2 = k2.multiply(c1);
      h2 = h2.xor(k2);
    case 8:
      k1 = k1.xor(Long.fromNumber(data[offset+7]).shiftLeft(56));
    case 7:
      k1 = k1.xor(Long.fromNumber(data[offset+6]).shiftLeft(48));
    case 6:
      k1 = k1.xor(Long.fromNumber(data[offset+5]).shiftLeft(40));
    case 5:
      k1 = k1.xor(Long.fromNumber(data[offset+4]).shiftLeft(32));
    case 4:
      k1 = k1.xor(Long.fromNumber(data[offset+3]).shiftLeft(24));
    case 3:
      k1 = k1.xor(Long.fromNumber(data[offset+2]).shiftLeft(16));
    case 2:
      k1 = k1.xor(Long.fromNumber(data[offset+1]).shiftLeft(8));
    case 1:
      k1 = k1.xor(Long.fromNumber(data[offset]));
      k1 = k1.multiply(c1);
      k1 = this.rotl64(k1,31);
      k1 = k1.multiply(c2);
      h1 = h1.xor(k1);
  }
  /* eslint-enable no-fallthrough */

  h1 = h1.xor(length);
  h2 = h2.xor(length);

  h1 = h1.add(h2);
  h2 = h2.add(h1);

  h1 = this.fmix(h1);
  h2 = this.fmix(h2);

  h1 = h1.add(h2);

  return h1;
};


Murmur3Tokenizer.prototype.getBlock = function (key, offset, index) {
  var i8 = index << 3;
  var blockOffset = offset + i8;
  return (
    Long.fromNumber(key[blockOffset + 0] & 0xff)
      .add(Long.fromNumber(key[blockOffset + 1] & 0xff).shiftLeft(8))
      .add(Long.fromNumber(key[blockOffset + 2] & 0xff).shiftLeft(16))
      .add(Long.fromNumber(key[blockOffset + 3] & 0xff).shiftLeft(24))
      .add(Long.fromNumber(key[blockOffset + 4] & 0xff).shiftLeft(32))
      .add(Long.fromNumber(key[blockOffset + 5] & 0xff).shiftLeft(40))
      .add(Long.fromNumber(key[blockOffset + 6] & 0xff).shiftLeft(48))
      .add(Long.fromNumber(key[blockOffset + 7] & 0xff).shiftLeft(56))
  );
};

/**
 * @param {Long} v
 * @param {Number} n
 * @returns {Long}
 */
Murmur3Tokenizer.prototype.rotl64 = function (v, n) {
  return (
    v.shiftRightUnsigned(64 - n).or(v.shiftLeft(n))
  );
};

/**
 * @param {Long} k
 * @returns {Long}
 */
Murmur3Tokenizer.prototype.fmix = function (k) {
  k = k.xor(k.shiftRightUnsigned(33));
  //0xff51afd7ed558ccd
  k = k.multiply(Long.fromBits(0xed558ccd, 0xff51afd7));
  k = k.xor(k.shiftRightUnsigned(33));
  //0xc4ceb9fe1a85ec53
  k = k.multiply(Long.fromBits(0x1a85ec53, 0xc4ceb9fe));
  k = k.xor(k.shiftRightUnsigned(33));
  return k;
};

/**
 *
 * @param {String} value
 * @returns {Long}
 */
Murmur3Tokenizer.prototype.parse = function (value) {
  return Long.fromString(value);
};

/**
 * @param {Long} val1
 * @param {Long} val2
 * @returns {number}
 */
Murmur3Tokenizer.prototype.compare = function (val1, val2) {
  return val1.compare(val2);
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
    value = new Buffer(value);
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
  return new Buffer(value);
};

exports.Murmur3Tokenizer = Murmur3Tokenizer;
exports.RandomTokenizer = RandomTokenizer;
exports.ByteOrderedTokenizer = ByteOrderedTokenizer;