/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import util from "util";
import type { DataTypeInfo } from "./encoder";
import types from "./types/index";


const _Murmur3TokenType = types.dataTypes.getByName('bigint');
const _RandomTokenType = types.dataTypes.getByName('varint');
const _OrderedTokenType = types.dataTypes.getByName('blob');

/**
 * Represents a token on the Cassandra ring.
 */
class Token {
  protected _value: any;
  /** @internal */
  constructor(value) {
    this._value = value;
  }

  /**
   * @returns {DataTypeInfo} The type info for the
   *                                           type of the value of the token.
   */
  getType(): DataTypeInfo {
    throw new Error('You must implement a getType function for this Token instance');
  }

  /**
   * @returns {*} The raw value of the token.
   */
  getValue(): any {
    return this._value;
  }

  /** @internal */
  toString() {
    return this._value.toString();
  }

  /**
   * Returns 0 if the values are equal, 1 if greater than other, -1
   * otherwise.
   *
   * @param {Token} other 
   * @returns {Number}
   */
  compare(other: Token): number {
    return this._value.compare(other._value);
  }

  equals(other: Token): boolean {
    return this.compare(other) === 0;
  }

  /** @internal */
  inspect() {
    return this.constructor.name + ' { ' + this.toString() + ' }';
  }
}

/**
 * Represents a token from a Cassandra ring where the partitioner
 * is Murmur3Partitioner.
 * 
 * The raw token type is a varint (represented by MutableLong).
 * @internal @ignore
 */
class Murmur3Token extends Token {
  constructor(value) {
    super(value);
  }

  getType() {
    return _Murmur3TokenType;
  }
}

/**
 * Represents a token from a Cassandra ring where the partitioner
 * is RandomPartitioner.
 * 
 * The raw token type is a bigint (represented by Number).
 * @internal @ignore
 */
class RandomToken extends Token {
  constructor(value) {
    super(value);
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
 * @internal @ignore
 */
class ByteOrderedToken extends Token {
  constructor(value) {
    super(value);
  }

  getType() {
    return _OrderedTokenType;
  }

  toString() {
    return this._value.toString('hex').toUpperCase();
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
  end: Token;
  start: Token;
  private _tokenizer: any;

  /** @internal */
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
   * @returns {TokenRange[]} Split ranges.
   * @throws {Error} If splitting an empty range.
   */
  splitEvenly(numberOfSplits: number): TokenRange[] {
    if (numberOfSplits < 1) {
      throw new Error(util.format("numberOfSplits (%d) must be greater than 0.", numberOfSplits));
    }
    if (this.isEmpty()) {
      throw new Error("Can't split empty range " + this.toString());
    }

    const tokenRanges = [];
    const splitPoints = this._tokenizer.split(this.start, this.end, numberOfSplits);
    let splitStart = this.start;
    let splitEnd;
    for (let splitIndex = 0; splitIndex < splitPoints.length; splitIndex++) {
      splitEnd = splitPoints[splitIndex];
      tokenRanges.push(new TokenRange(splitStart, splitEnd, this._tokenizer));
      splitStart = splitEnd;
    }
    tokenRanges.push(new TokenRange(splitStart, this.end, this._tokenizer));
    return tokenRanges;
  }

  /**
   * A range is empty when start and end are the same token, except if
   * that is the minimum token, in which case the range covers the
   * whole ring.  This is consistent with the behavior of CQL range
   * queries.
   *
   * @returns {boolean} Whether this range is empty.
   */
  isEmpty(): boolean {
    return this.start.equals(this.end) && !this.start.equals(this._tokenizer.minToken());
  }

  /**
   * A range wraps around the end of the ring when the start token
   * is greater than the end token and the end token is not the 
   * minimum token.
   *
   * @returns {boolean} Whether this range wraps around.
   */
  isWrappedAround(): boolean {
    return this.start.compare(this.end) > 0 && !this.end.equals(this._tokenizer.minToken());
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
  unwrap(): TokenRange[] {
    if (this.isWrappedAround()) {
      return [
        new TokenRange(this.start, this._tokenizer.minToken(), this._tokenizer),
        new TokenRange(this._tokenizer.minToken(), this.end, this._tokenizer)
      ];
    }
    return [this];
  }

  /**
   * Whether this range contains a given Token.
   * 
   * @param {Token} token Token to check for.
   * @returns {boolean} Whether or not the Token is in this range.
   */
  contains(token: Token): boolean {
    if (this.isEmpty()) {
      return false;
    }
    const minToken = this._tokenizer.minToken();
    if (this.end.equals(minToken)) {
      if (this.start.equals(minToken)) {
        return true; // ]minToken, minToken] === full ring
      } else if (token.equals(minToken)) {
        return true;
      }
      return token.compare(this.start) > 0;
    }

    const isAfterStart = token.compare(this.start) > 0;
    const isBeforeEnd = token.compare(this.end) <= 0;
    // if wrapped around ring, token is in ring if its after start or before end.
    // otherwise, token is in ring if its after start and before end.
    return this.isWrappedAround() 
      ? isAfterStart || isBeforeEnd
      : isAfterStart && isBeforeEnd;
  }

  /**
   * Determines if the input range is equivalent to this one.
   * 
   * @param {TokenRange} other Range to compare with.
   * @returns {boolean} Whether or not the ranges are equal.
   */
  equals(other: TokenRange): boolean {
    if (other === this) {
      return true;
    } else if (other instanceof TokenRange) {
      return this.compare(other) === 0;
    }
    return false;
  }

  /**
   * Returns 0 if the values are equal, otherwise compares against
   * start, if start is equal, compares against end.
   *  
   * @param {TokenRange} other Range to compare with.
   * @returns {Number} 
   */
  compare(other: TokenRange): number {
    const compareStart = this.start.compare(other.start);
    return compareStart !== 0 ? compareStart : this.end.compare(other.end);
  }

  /** @internal */
  toString() {
    return util.format(']%s, %s]', 
      this.start.toString(),
      this.end.toString()
    );
  }
}

export {
  ByteOrderedToken,
  Murmur3Token,
  RandomToken, Token,
  TokenRange
};

export default {
  Token,
  TokenRange,
  ByteOrderedToken,
  Murmur3Token,
  RandomToken
};