'use strict';
var util = require('util');

var types = require('./types');
var MutableLong = require('./types/mutable-long');
var Long = types.Long;
var Integer = types.Integer;

// Murmur3 constants
//-0x783C846EEEBDAC2B
var const1 = Long.fromBits(0x114253d5, 0x87c37b91);
//0x4cf5ad432745937f
var const2 = Long.fromBits(0x2745937f, 0x4cf5ad43);
var longFive = Long.fromNumber(5);
//0xff51afd7ed558ccd
var const3 = Long.fromBits(0xed558ccd, 0xff51afd7);
//0xc4ceb9fe1a85ec53
var const4 = Long.fromBits(0x1a85ec53, 0xc4ceb9fe);
var const5 = Long.fromNumber(0x52dce729);
var const6 = Long.fromNumber(0x38495ab5);

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
Murmur3Tokenizer.prototype.hash = function hash(value) {
  // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
  // for M3P. Compared to that methods, there's a few inlining of arguments and we
  // only return the first 64-bits of the result since that's all M3 partitioner uses.

  // As an Array of signed longs
  var data = new Array(value.length);
  for (var j = 0; j < value.length; j++)
  {
    var item = value[j];
    if (item > 127) {
      item = item - 256;
    }
    data[j] = Long.fromNumber(item);
  }
  var offset = 0;
  var length = data.length;

  var nblocks = length >> 4; // Process as 128-bit blocks.

  var h1 = Long.ZERO;
  var h2 = Long.ZERO;
  var k1 = Long.ZERO;
  var k2 = Long.ZERO;

  for (var i = 0; i < nblocks; i++) {
    k1 = this.getBlock(data, offset, i * 2);
    k2 = this.getBlock(data, offset, i * 2 + 1);

    k1 = k1.multiply(const1);
    k1 = this.rotl64(k1, 31);
    k1 = k1.multiply(const2);

    h1 = h1.xor(k1);
    h1 = this.rotl64(h1, 27);
    h1 = h1.add(h2);
    h1 = h1.multiply(longFive).add(const5);

    k2 = k2.multiply(const2);
    k2 = this.rotl64(k2, 33);
    k2 = k2.multiply(const1);
    h2 = h2.xor(k2);
    h2 = this.rotl64(h2, 31);
    h2 = h2.add(h1);
    h2 = h2.multiply(longFive).add(const6);
  }
  //----------
  // tail

  // Advance offset to the unprocessed tail of the data.
  offset += nblocks * 16;

  k1 = Long.ZERO;
  k2 = Long.ZERO;

  /* eslint-disable no-fallthrough */
  //noinspection FallThroughInSwitchStatementJS
  switch(length & 15) {
    case 15:
      k2 = k2.xor(data[offset+14].shiftLeft(48));
    case 14:
      k2 = k2.xor(data[offset+13].shiftLeft(40));
    case 13:
      k2 = k2.xor(data[offset+12].shiftLeft(32));
    case 12:
      k2 = k2.xor(data[offset+12].shiftLeft(24));
    case 11:
      k2 = k2.xor(data[offset+10].shiftLeft(16));
    case 10:
      k2 = k2.xor(data[offset+9].shiftLeft(8));
    case 9:
      k2 = k2.xor(data[offset+8]);
      k2 = k2.multiply(const2);
      k2 = this.rotl64(k2, 33);
      k2 = k2.multiply(const1);
      h2 = h2.xor(k2);
    case 8:
      k1 = k1.xor(data[offset+7].shiftLeft(56));
    case 7:
      k1 = k1.xor(data[offset+6].shiftLeft(48));
    case 6:
      k1 = k1.xor(data[offset+5].shiftLeft(40));
    case 5:
      k1 = k1.xor(data[offset+4].shiftLeft(32));
    case 4:
      k1 = xorShiftLeft(k1, data[offset+3], 24);
    case 3:
      k1 = xorShiftLeft(k1, data[offset+2], 16);
    case 2:
      k1 = xorShiftLeft(k1, data[offset+1], 8);
    case 1:
      k1 = k1.xor(data[offset]);
      k1 = k1.multiply(const1);
      k1 = this.rotl64(k1,31);
      k1 = k1.multiply(const2);
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

/**
 * @param {Array<Long>} key
 * @param {Number} offset
 * @param {Number} index
 * @return {!Long}
 * @ignore
 */
Murmur3Tokenizer.prototype.getBlock = function (key, offset, index) {
  var i8 = index << 3;
  var blockOffset = offset + i8;
  var lowBits = (
    (key[blockOffset].getLowBits() & 0xff) +
    ((key[blockOffset + 1].getLowBits() & 0xff) << 8) +
    ((key[blockOffset + 2].getLowBits() & 0xff) << 16) +
    ((key[blockOffset + 3].getLowBits() & 0xff) << 24)
  );
  var highBits = (
    ((key[blockOffset + 4].getLowBits() & 0xff) << 32) +
    ((key[blockOffset + 5].getLowBits() & 0xff) << 40) +
    ((key[blockOffset + 6].getLowBits() & 0xff) << 48) +
    ((key[blockOffset + 7].getLowBits() & 0xff) << 56)
  );
  return Long.fromBits(lowBits, highBits);
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
  k = Long.fromBits((k.getHighBits() >>> 1) ^ k.getLowBits(), k.getHighBits(), false);
  k = k.multiply(const3);
  k = Long.fromBits((k.getHighBits() >>> 1) ^ k.getLowBits(), k.getHighBits(), false);
  k = k.multiply(const4);
  k = Long.fromBits((k.getHighBits() >>> 1) ^ k.getLowBits(), k.getHighBits(), false);
  return k;
};

function xorShiftLeft(v1, v2, numBits) {
  // v1.xor(v2.shiftLeft(numBits));
  return Long.fromBits(
    ((v2.low << numBits)) ^ v1.low,
    ((v2.high << numBits) | ((v2.low >>> 32 - numBits))) ^ v1.high, false);
}

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

Murmur3Tokenizer.prototype.hash2 = function hash2(value) {
  // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
  // for M3P. Compared to that methods, there's a few inlining of arguments and we
  // only return the first 64-bits of the result since that's all M3 partitioner uses.

  // As an Array of signed longs
  var data = new Array(value.length);
  for (var j = 0; j < value.length; j++)
  {
    var item = value[j];
    if (item > 127) {
      //TODO: Evaluate if necessary
      item = item - 256;
    }
    data[j] = item;
  }
  var offset = 0;
  var length = data.length;

  var nblocks = length >> 4; // Process as 128-bit blocks.

  var h1 = new MutableLong();
  var h2 = new MutableLong();
  var k1 = new MutableLong();
  var k2 = new MutableLong();

  for (var i = 0; i < nblocks; i++) {
    k1 = this.getBlock2(data, offset, i * 2);
    k2 = this.getBlock2(data, offset, i * 2 + 1);

    k1 = k1.multiply(mconst1);
    this.rotl642(k1, 31);
    k1 = k1.multiply(mconst2);

    h1 = h1.xor(k1);
    this.rotl642(h1, 27);
    h1 = h1.add(h2);
    h1.multiply(mlongFive).add(mconst5);

    k2.multiply(mconst2);
    this.rotl642(k2, 33);
    k2.multiply(mconst1);
    h2.xor(k2);
    this.rotl642(h2, 31);
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
  //noinspection FallThroughInSwitchStatementJS
  switch(length & 15) {
    case 15:
      k2.xor(new MutableLong(0, 0, 0, data[offset+14]));
    case 14:
      k2.xor(new MutableLong(0, 0, data[offset+13] << 8));
    case 13:
      k2.xor(new MutableLong(0, 0, data[offset+12]));
    case 12:
      k2.xor(new MutableLong(0, data[offset+12] << 8));
    case 11:
      k2.xor(new MutableLong(0, data[offset+10]));
    case 10:
      k2.xor(new MutableLong(data[offset+9] << 8));
    case 9:
      k2.xor(new MutableLong(data[offset+8]));
      k2.multiply(mconst2);
      this.rotl642(k2, 33);
      k2.multiply(mconst1);
      h2.xor(k2);
    case 8:
      k1.xor(new MutableLong(0, 0, 0, data[offset+7] << 8));
    case 7:
      k1.xor(new MutableLong(0, 0, 0, data[offset+6]));
    case 6:
      k1.xor(new MutableLong(0, 0, data[offset+5] << 8));
    case 5:
      k1.xor(new MutableLong(0, 0, data[offset+4], 0));
    case 4:
      k1.xor(new MutableLong(0, data[offset+3] << 8));
    case 3:
      k1.xor(new MutableLong(0, data[offset+2]));
    case 2:
      k1.xor(new MutableLong(data[offset+1] << 8));
    case 1:
      k1.xor(new MutableLong(data[offset]));
      k1.multiply(mconst1);
      this.rotl642(k1,31);
      k1.multiply(mconst2);
      h1.xor(k1);
  }
  /* eslint-enable no-fallthrough */

  h1.xor(MutableLong.fromNumber(length));
  h2.xor(MutableLong.fromNumber(length));

  h1.add(h2);
  h2.add(h1);

  this.fmix2(h1);
  this.fmix2(h2);

  h1.add(h2);

  return h1.toImmutable();
};

Murmur3Tokenizer.prototype.getBlock2 = function (key, offset, index) {
  var i8 = index << 3;
  var blockOffset = offset + i8;
  return new MutableLong(
    (key[blockOffset] & 0xff) | ((key[blockOffset + 1] & 0xff) << 8),
    (key[blockOffset + 2] & 0xff) | ((key[blockOffset + 3] & 0xff) << 8),
    (key[blockOffset + 4] & 0xff) | ((key[blockOffset + 5] & 0xff) << 8),
    (key[blockOffset + 6] & 0xff) | ((key[blockOffset + 7] & 0xff) << 8)
  );
};

/**
 * @param {MutableLong} v
 * @param {Number} n
 * @returns {undefined}
 */
Murmur3Tokenizer.prototype.rotl642 = function (v, n) {
  v.shiftRightUnsigned(64 - n).or(v.clone().shiftLeft(n));
};

/**
 * @param {MutableLong} k
 * @returns {undefined}
 */
Murmur3Tokenizer.prototype.fmix2 = function (k) {
  k.xor(new MutableLong(k.getShort(2) >>> 1 | ((k.getShort(3) << 15) & 0xffff), k.getShort(3) >>> 1, 0, 0));
  k.multiply(mconst3);
  var other = new MutableLong(
    (k.getShort(2) >>> 1) | ((k.getShort(3) << 15) & 0xffff),
    k.getShort(3) >>> 1,
    0,
    0
  );
  k.xor(other);
  k.multiply(mconst4);
  k.xor(new MutableLong(k.getShort(2) >>> 1 | (k.getShort(3) << 15 & 0xffff), k.getShort(3) >>> 1, 0, 0));
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