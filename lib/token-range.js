'use strict';

const util = require('util');

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
    this._tokenizer = tokenizer;
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
   * @return {TokenRange[]} The list of non-wrapping ranges.
   */
  unwrap() {

  }

  /**
   * @return {boolean} Whether this range intersects another one.
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
   * @return {TokenRange[]} The range(s) resulting from the
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
      this._tokenizer.stringify(this.start),
      this._tokenizer.stringify(this.end)
    );
  }
}

module.exports = TokenRange;