'use strict';

const types = require('./types');
const util = require('util');

const _Murmur3TokenType = types.dataTypes.getByName('bigint');
const _RandomTokenType = types.dataTypes.getByName('varint');
const _OrderedTokenType = types.dataTypes.getByName('blob');

/**
 * Represents a token on the Cassandra ring.
 *
 * @property value The raw value of the token.  Type depends on
 *                 partitioner implementation. 
 * 
 */
class Token {
  constructor(value, tokenizer) {
    this.value = value;
    Object.defineProperty(this, '_tokenizer', { value: tokenizer, enumerable: false});
  }

  /**
   * @returns {{code: number, info: *|Object}} The type info for the
   *                                           type of the value of the token.
   */
  getType() {
    throw new Error('You must implement a getType function for this Token instance');
  }

  /**
   * @returns {*} The raw value of the token.
   */
  getValue() {
    return this.value;
  }

  /**
   * Encodes the underlying value into a buffer that can be
   * used for routing.
   * 
   * @param {Encoder} encoder Encode for encoding to buffer.
   * @returns {Buffer} buffer data for the token.
   */
  toBuffer(encoder) {
    return encoder.encode(this.getValue(), this.getType());
  }

  toString() {
    return this._tokenizer.stringify(this.getValue());
  }

  /**
   * Returns 0 if the values are equal, 1 if greater than other, -1
   * otherwise.
   *
   * @param {Token} other 
   * @returns {Number}
   */
  compare(other) {
    return this._tokenizer.compare(this.getValue(), other.getValue());
  }

  equals(other) {
    return this.compare(other) === 0;
  }
}

/**
 * Represents a token from a Cassandra ring where the partitioner
 * is Murmur3Partitioner.
 * 
 * The raw token type is a varint (represented by MutableLong).
 */
class Murmur3Token extends Token {
  constructor(value, tokenizer) {
    super(value, tokenizer);
  }

  getType() {
    return _Murmur3TokenType;
  }

  toString() {
    // TODO: Custom implementation
    return this._tokenizer.toString(this.getValue());
  }
}

/**
 * Represents a token from a Cassandra ring where the partitioner
 * is RandomPartitioner.
 * 
 * The raw token type is a bigint (represented by Number).
 */
class RandomToken extends Token {
  constructor(value, tokenizer) {
    super(value, tokenizer);
  }

  getType() {
    return _RandomTokenType;
  }
}

/**
 * Represents a token from a Cassandra ring where the partitioner
 * is ByteOrderedPartitioner.
 * 
 * The raw token type is a blob (represented by Buffer or Array).
 */
class ByteOrderedToken extends Token {
  constructor(value, tokenizer) {
    super(value, tokenizer);
  }

  getType() {
    return _OrderedTokenType;
  }
}

/** 
 * Represents a range of tokens on a Cassandra ring.
 *
 * A range is start-exclusive and end-inclusive.  It is empty when
 * start and end are the same token, except if that is the minimum
 * token, in which case the range covers the whole ring (this is
 * consistent with the behavior of CQL range queries).
 *
 * Note that CQL does not handle wrapping.  To query all partitions
 * in a range, see {@link unwrap}.
 */
class TokenRange {
  constructor(start, end, tokenizer) {
    this.start = start;
    this.end = end;
    Object.defineProperty(this, '_tokenizer', { value: tokenizer, enumerable: false});
  }

  /**
   * Splits this range into a number of smaller ranges of equal "size"
   * (referring to the number of tokens, not the actual amount of data).
   *
   * Splitting an empty range is not permitted.  But not that, in edge
   * cases, splitting a range might produce one or more empty ranges.
   *
   * @param {Number} numberOfSplits Number of splits to make.
   * @returns {TokenRage[]} Split ranges.
   * @throws {Error} If splitting an empty range.
   */
  splitEvenly(numberOfSplits) {

  }

  /**
   * A range is empty when start and end are the same token, except if
   * that is the minimum token, in which case the range covers the
   * whole ring.  This is consistent with the behavior of CQL range
   * queries.
   *
   * @returns {boolean} Whether this range is empty.
   */
  isEmpty() {

  }

  /**
   * A range wraps around the end of the ring when the start token
   * is greater than the end token and the end token is not the 
   * minimum token.
   *
   * @returns {boolean} Whether this range wraps around.
   */
  isWrappedAround() {

  }

  /**
   * Splits this range into a list of two non-wrapping ranges.
   *
   * This will return the range itself if it is non-wrapped, or two
   * ranges otherwise.
   *
   * This is useful for CQL range queries, which do not handle
   * wrapping.
   *
   * @returns {TokenRange[]} The list of non-wrapping ranges.
   */
  unwrap() {

  }

  /**
   * @returns {boolean} Whether this range intersects another one.
   */
  intersects(other) {

  }

  /**
   * Computes the intersection of this range with another one.
   *
   * If either of these ranges overlap the ring, they are unwrapped
   * and the unwrapped tokens are compared with one another.
   * 
   * This call will fail if two ranges do not intersect, you must
   * check by calling {@link intersects} beforehand.
   * 
   * @param {TokenRange} other The range to intersect with.
   * @returns {TokenRange[]} The range(s) resulting from the
   *                        intersection.
   * @throws {Error} If the ranges do not intersect.
   */
  intersectWith(other) {

  }

  /**
   * Merges this range with another one. 
   * 
   * The two ranges should either intersect or be adjacent; in other
   * words, the merged range should not include tokens that are in
   * neither of the original ranges.
   * 
   * @param {TokenRange} other The range to merge with.
   * @returns {TokenRange} The merged range.
   * @throws Error If the ranges neither intersect nor are adjacent.
   */
  mergeWith(other) {

  }

  /**
   * Whether this range contains a given Token.
   * 
   * @param {*} token Token to check for.
   * @returns {boolean} Whether or not the Token is in this range.
   */
  contains(token) {

  }

  /**
   * Determines if the input range is equivalent to this one.
   * 
   * @param {TokenRange} other Range to compare with.
   * @returns {boolean} Whether or not the ranges are equal.
   */
  equals(other) {

  }

  /**
   * Returns 0 if the values are equal, otherwise compares against
   * start, if start is equal, compares against end.
   *  
   * @param {TokenRange} other Range to compare with.
   * @returns {Number} 
   */
  compare(other) {

  }

  toString() {
    return util.format(']%s, %s]', 
      this.start.toString(),
      this.end.toString()
    );
  }
}

exports.Token = Token;
exports.TokenRange = TokenRange;
exports.ByteOrderedToken = ByteOrderedToken;
exports.Murmur3Token = Murmur3Token;
exports.RandomToken = RandomToken;