// Copyright 2009 The Closure Library Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS-IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import utils from "../utils";

/**
 * A two's-complement integer an array containing bits of the
 * integer in 32-bit (signed) pieces, given in little-endian order (i.e.,
 * lowest-order bits in the first piece), and the sign of -1 or 0.
 *
 * See the from* functions below for other convenient ways of constructing
 * Integers.
 *
 * The internal representation of an integer is an array of 32-bit signed
 * pieces, along with a sign (0 or -1) that indicates the contents of all the
 * other 32-bit pieces out to infinity.  We use 32-bit pieces because these are
 * the size of integers on which Javascript performs bit-operations.  For
 * operations like addition and multiplication, we split each number into 16-bit
 * pieces, which can easily be multiplied within Javascript's floating-point
 * representation without overflow or change in sign.
 * @final
 */
class Integer {
  private bits_: number[];
  private sign_: number;

  /**
   * Constructs a two's-complement integer an array containing bits of the
   * integer in 32-bit (signed) pieces, given in little-endian order (i.e.,
   * lowest-order bits in the first piece), and the sign of -1 or 0.
   *
   * See the from* functions below for other convenient ways of constructing
   * Integers.
   *
   * The internal representation of an integer is an array of 32-bit signed
   * pieces, along with a sign (0 or -1) that indicates the contents of all the
   * other 32-bit pieces out to infinity.  We use 32-bit pieces because these are
   * the size of integers on which Javascript performs bit-operations.  For
   * operations like addition and multiplication, we split each number into 16-bit
   * pieces, which can easily be multiplied within Javascript's floating-point
   * representation without overflow or change in sign.
   *
   * @constructor
   * @param {Array.<number>} bits Array containing the bits of the number.
   * @param {number} sign The sign of the number: -1 for negative and 0 positive.
   * @final
   */
  constructor(bits: number[], sign: number) {
    this.bits_ = [];
    this.sign_ = sign;

    // Copy the 32-bit signed integer values passed in.  We prune out those at the
    // top that equal the sign since they are redundant.
    let top = true;
    for (let i = bits.length - 1; i >= 0; i--) {
      const val = bits[i] | 0;
      if (!top || val !== sign) {
        this.bits_[i] = val;
        top = false;
      }
    }
  }


  // NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
  // from* methods on which they depend.


  /**
   * A cache of the Integer representations of small integer values.
   * @type {!Object}
   * @private
   */
  private static IntCache_: { [key: number]: Integer } = {};

  /**
   * Returns an Integer representing the given (32-bit) integer value.
   * @param {number} value A 32-bit integer value.
   * @return {!Integer} The corresponding Integer value.
   */
  static fromInt(value: number): Integer {
    if (value >= -128 && value < 128) {
      const cachedObj = Integer.IntCache_[value];
      if (cachedObj) {
        return cachedObj;
      }
    }

    const obj = new Integer([value | 0], value < 0 ? -1 : 0);
    if (value >= -128 && value < 128) {
      Integer.IntCache_[value] = obj;
    }
    return obj;
  }

  /**
   * Returns an Integer representing the given value, provided that it is a finite
   * number.  Otherwise, zero is returned.
   * @param {number} value The value in question.
   * @return {!Integer} The corresponding Integer value.
   */
  static fromNumber(value: number): Integer {
    if (isNaN(value) || !isFinite(value)) {
      return Integer.ZERO;
    } else if (value < 0) {
      return Integer.fromNumber(-value).negate();
    } 
    const bits = [];
    let pow = 1;
    for (let i = 0; value >= pow; i++) {
      bits[i] = (value / pow) | 0;
      pow *= Integer.TWO_PWR_32_DBL_;
    }
    return new Integer(bits, 0);
    
  }

  /**
   * Returns a Integer representing the value that comes by concatenating the
   * given entries, each is assumed to be 32 signed bits, given in little-endian
   * order (lowest order bits in the lowest index), and sign-extending the highest
   * order 32-bit value.
   * @param {Array.<number>} bits The bits of the number, in 32-bit signed pieces,
   *     in little-endian order.
   * @return {!Integer} The corresponding Integer value.
   */
  static fromBits(bits: number[]): Integer {
    const high = bits[bits.length - 1];
    //noinspection JSBitwiseOperatorUsage
    return new Integer(bits, high & (1 << 31) ? -1 : 0);
  }

  /**
   * Returns an Integer representation of the given string, written using the
   * given radix.
   * @param {string} str The textual representation of the Integer.
   * @param {number=} opt_radix The radix in which the text is written.
   * @return {!Integer} The corresponding Integer value.
   */
  static fromString(str: string, opt_radix?: number): Integer {
    if (str.length === 0) {
      throw TypeError('number format error: empty string');
    }

    const radix = opt_radix || 10;
    if (radix < 2 || radix > 36) {
      throw Error('radix out of range: ' + radix);
    }

    if (str.charAt(0) === '-') {
      return Integer.fromString(str.substring(1), radix).negate();
    } else if (str.indexOf('-') >= 0) {
      throw TypeError('number format error: interior "-" character');
    }

    // Do several (8) digits each time through the loop, so as to
    // minimize the calls to the very expensive emulated div.
    const radixToPower = Integer.fromNumber(Math.pow(radix, 8));

    let result = Integer.ZERO;
    for (let i = 0; i < str.length; i += 8) {
      const size = Math.min(8, str.length - i);
      const value = parseInt(str.substring(i, i + size), radix);
      if (size < 8) {
        const power = Integer.fromNumber(Math.pow(radix, size));
        result = result.multiply(power).add(Integer.fromNumber(value));
      } else {
        result = result.multiply(radixToPower);
        result = result.add(Integer.fromNumber(value));
      }
    }
    return result;
  }

  /**
   * Returns an Integer representation of a given big endian Buffer.
   * The internal representation of bits contains bytes in groups of 4
   * @param {Buffer} buf
   * @returns {Integer}
   */
  static fromBuffer(buf: Buffer): Integer {
    const bits = new Array(Math.ceil(buf.length / 4));
    //noinspection JSBitwiseOperatorUsage
    const sign = buf[0] & (1 << 7) ? -1 : 0;
    for (let i = 0; i < bits.length; i++) {
      let offset = buf.length - ((i + 1) * 4);
      let value;
      if (offset < 0) {
        //The buffer length is not multiple of 4
        offset = offset + 4;
        value = 0;
        for (let j = 0; j < offset; j++) {
          let byte = buf[j];
          if (sign === -1) {
            //invert the bits
            byte = ~byte & 0xff;
          }
          value = value | (byte << (offset - j - 1) * 8);
        }
        if (sign === -1) {
          //invert all the bits
          value = ~value;
        }
      } else {
        value = buf.readInt32BE(offset);
      }
      bits[i] = value;
    }
    return new Integer(bits, sign);
  }

  /**
   * Returns a big endian buffer representation of an Integer.
   * Internally the bits are represented using 4 bytes groups (numbers),
   * in the Buffer representation there might be the case where we need less than the 4 bytes.
   * For example: 0x00000001 -> '01', 0xFFFFFFFF -> 'FF', 0xFFFFFF01 -> 'FF01'
   * @param {Integer} value
   * @returns {Buffer}
   */
  static toBuffer(value: Integer): Buffer {
    const sign = value.sign_;
    const bits = value.bits_;
    if (bits.length === 0) {
      //[0] or [0xffffffff]
      return utils.allocBufferFromArray([value.sign_]);
    }
    //the high bits might need to be represented in less than 4 bytes
    let highBits = bits[bits.length - 1];
    if (sign === -1) {
      highBits = ~highBits;
    }
    const high = [];
    if (highBits >>> 24 > 0) {
      high.push((highBits >> 24) & 0xff);
    }
    if (highBits >>> 16 > 0) {
      high.push((highBits >> 16) & 0xff);
    }
    if (highBits >>> 8 > 0) {
      high.push((highBits >> 8) & 0xff);
    }
    high.push(highBits & 0xff);
    if (sign === -1) {
      //The byte containing the sign bit got removed
      if (high[0] >> 7 !== 0) {
        //it is going to be negated
        high.unshift(0);
      }
    }
    else if (high[0] >> 7 !== 0) {
      //its positive but it lost the byte containing the sign bit
      high.unshift(0);
    }
    const buf = utils.allocBufferUnsafe(high.length + ((bits.length - 1) * 4));
    for (let j = 0; j < high.length; j++) {
      const b = high[j];
      if (sign === -1) {
        buf[j] = ~b;
      } else {
        buf[j] = b;
      }
    }
    for (let i = 0; i < bits.length - 1; i++) {
      const group = bits[bits.length - 2 - i];
      const offset = high.length + i * 4;
      buf.writeInt32BE(group, offset);
    }
    return buf;
  }

  /**
   * A number used repeatedly in calculations.  This must appear before the first
   * call to the from* functions below.
   * @type {number}
   * @private
   */
  private static TWO_PWR_32_DBL_: number = (1 << 16) * (1 << 16);

  /** @type {!Integer} */
  static ZERO: Integer = Integer.fromInt(0);

  /** @type {!Integer} */
  static ONE: Integer = Integer.fromInt(1);

  /**
   * @type {!Integer}
   * @private
   */
  private static TWO_PWR_24_: Integer = Integer.fromInt(1 << 24);

  /**
   * Returns the value, assuming it is a 32-bit integer.
   * @return {number} The corresponding int value.
   */
  toInt(): number {
    return this.bits_.length > 0 ? this.bits_[0] : this.sign_;
  }

  /** @return {number} The closest floating-point representation to this value. */
  toNumber(): number {
    if (this.isNegative()) {
      return -this.negate().toNumber();
    } 
    let val = 0;
    let pow = 1;
    for (let i = 0; i < this.bits_.length; i++) {
      val += this.getBitsUnsigned(i) * pow;
      pow *= Integer.TWO_PWR_32_DBL_;
    }
    return val;
    
  }

  /**
   * @param {number=} opt_radix The radix in which the text should be written.
   * @return {string} The textual representation of this value.
   * @override
   */
  toString(opt_radix?: number): string {
    const radix = opt_radix || 10;
    if (radix < 2 || radix > 36) {
      throw Error('radix out of range: ' + radix);
    }

    if (this.isZero()) {
      return '0';
    } else if (this.isNegative()) {
      return '-' + this.negate().toString(radix);
    }

    // Do several (6) digits each time through the loop, so as to
    // minimize the calls to the very expensive emulated div.
    const radixToPower = Integer.fromNumber(Math.pow(radix, 6));

    let rem : Integer = this;
    let result = '';
    while (true) {
      const remDiv = rem.divide(radixToPower);
      const intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
      let digits = intval.toString(radix);

      rem = remDiv;
      if (rem.isZero()) {
        return digits + result;
      } 
      while (digits.length < 6) {
        digits = '0' + digits;
      }
      result = '' + digits + result;
      
    }
  }

  /**
   * Returns the index-th 32-bit (signed) piece of the Integer according to
   * little-endian order (i.e., index 0 contains the smallest bits).
   * @param {number} index The index in question.
   * @return {number} The requested 32-bits as a signed number.
   */
  getBits(index: number): number {
    if (index < 0) {
      return 0;
    } else if (index < this.bits_.length) {
      return this.bits_[index];
    } 
    return this.sign_;
    
  }

  /**
   * Returns the index-th 32-bit piece as an unsigned number.
   * @param {number} index The index in question.
   * @return {number} The requested 32-bits as an unsigned number.
   */
  getBitsUnsigned(index: number): number {
    const val = this.getBits(index);
    return val >= 0 ? val : Integer.TWO_PWR_32_DBL_ + val;
  }

  /** @return {number} The sign bit of this number, -1 or 0. */
  getSign(): number {
    return this.sign_;
  }

  /** @return {boolean} Whether this value is zero. */
  isZero(): boolean {
    if (this.sign_ !== 0) {
      return false;
    }
    for (let i = 0; i < this.bits_.length; i++) {
      if (this.bits_[i] !== 0) {
        return false;
      }
    }
    return true;
  }

  /** @return {boolean} Whether this value is negative. */
  isNegative(): boolean {
    return this.sign_ === -1;
  }

  /** @return {boolean} Whether this value is odd. */
  isOdd(): boolean {
    return (this.bits_.length === 0 && this.sign_ === -1) ||
      (this.bits_.length > 0 && (this.bits_[0] & 1) !== 0);
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer equals the other.
   */
  equals(other: Integer): boolean {
    if (this.sign_ !== other.sign_) {
      return false;
    }
    const len = Math.max(this.bits_.length, other.bits_.length);
    for (let i = 0; i < len; i++) {
      if (this.getBits(i) !== other.getBits(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer does not equal the other.
   */
  notEquals(other: Integer): boolean {
    return !this.equals(other);
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer is greater than the other.
   */
  greaterThan(other: Integer): boolean {
    return this.compare(other) > 0;
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer is greater than or equal to the other.
   */
  greaterThanOrEqual(other: Integer): boolean {
    return this.compare(other) >= 0;
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer is less than the other.
   */
  lessThan(other: Integer): boolean {
    return this.compare(other) < 0;
  }

  /**
   * @param {Integer} other Integer to compare against.
   * @return {boolean} Whether this Integer is less than or equal to the other.
   */
  lessThanOrEqual(other: Integer): boolean {
    return this.compare(other) <= 0;
  }

  /**
   * Compares this Integer with the given one.
   * @param {Integer} other Integer to compare against.
   * @return {number} 0 if they are the same, 1 if the this is greater, and -1
   *     if the given one is greater.
   */
  compare(other: Integer): number {
    const diff = this.subtract(other);
    if (diff.isNegative()) {
      return -1;
    } else if (diff.isZero()) {
      return 0;
    } 
    return +1;
    
  }

  /**
   * Returns an integer with only the first numBits bits of this value, sign
   * extended from the final bit.
   * @param {number} numBits The number of bits by which to shift.
   * @return {!Integer} The shorted integer value.
   */
  shorten(numBits: number): Integer {
    const arr_index = (numBits - 1) >> 5;
    const bit_index = (numBits - 1) % 32;
    const bits = [];
    for (let i = 0; i < arr_index; i++) {
      bits[i] = this.getBits(i);
    }
    const sigBits = bit_index === 31 ? 0xFFFFFFFF : (1 << (bit_index + 1)) - 1;
    let val = this.getBits(arr_index) & sigBits;
    //noinspection JSBitwiseOperatorUsage
    if (val & (1 << bit_index)) {
      val |= 0xFFFFFFFF - sigBits;
      bits[arr_index] = val;
      return new Integer(bits, -1);
    } 
    bits[arr_index] = val;
    return new Integer(bits, 0);
    
  }

  /** @return {!Integer} The negation of this value. */
  negate(): Integer {
    return this.not().add(Integer.ONE);
  }

  /**
   * Returns the sum of this and the given Integer.
   * @param {Integer} other The Integer to add to this.
   * @return {!Integer} The Integer result.
   */
  add(other: Integer): Integer {
    const len = Math.max(this.bits_.length, other.bits_.length);
    const arr = [];
    let carry = 0;

    for (let i = 0; i <= len; i++) {
      const a1 = this.getBits(i) >>> 16;
      const a0 = this.getBits(i) & 0xFFFF;

      const b1 = other.getBits(i) >>> 16;
      const b0 = other.getBits(i) & 0xFFFF;

      let c0 = carry + a0 + b0;
      let c1 = (c0 >>> 16) + a1 + b1;
      carry = c1 >>> 16;
      c0 &= 0xFFFF;
      c1 &= 0xFFFF;
      arr[i] = (c1 << 16) | c0;
    }
    return Integer.fromBits(arr);
  }

  /**
   * Returns the difference of this and the given Integer.
   * @param {Integer} other The Integer to subtract from this.
   * @return {!Integer} The Integer result.
   */
  subtract(other: Integer): Integer {
    return this.add(other.negate());
  }

  /**
   * Returns the product of this and the given Integer.
   * @param {Integer} other The Integer to multiply against this.
   * @return {!Integer} The product of this and the other.
   */
  multiply(other: Integer): Integer {
    if (this.isZero()) {
      return Integer.ZERO;
    } else if (other.isZero()) {
      return Integer.ZERO;
    }

    if (this.isNegative()) {
      if (other.isNegative()) {
        return this.negate().multiply(other.negate());
      } 
      return this.negate().multiply(other).negate();
      
    } else if (other.isNegative()) {
      return this.multiply(other.negate()).negate();
    }

    // If both numbers are small, use float multiplication
    if (this.lessThan(Integer.TWO_PWR_24_) && other.lessThan(Integer.TWO_PWR_24_)) {
      return Integer.fromNumber(this.toNumber() * other.toNumber());
    }

    // Fill in an array of 16-bit products.
    const len = this.bits_.length + other.bits_.length;
    const arr = [];
    for (let i = 0; i < 2 * len; i++) {
      arr[i] = 0;
    }
    for (let i = 0; i < this.bits_.length; i++) {
      for (let j = 0; j < other.bits_.length; j++) {
        const a1 = this.getBits(i) >>> 16;
        const a0 = this.getBits(i) & 0xFFFF;

        const b1 = other.getBits(j) >>> 16;
        const b0 = other.getBits(j) & 0xFFFF;

        arr[2 * i + 2 * j] += a0 * b0;
        Integer.carry16_(arr, 2 * i + 2 * j);
        arr[2 * i + 2 * j + 1] += a1 * b0;
        Integer.carry16_(arr, 2 * i + 2 * j + 1);
        arr[2 * i + 2 * j + 1] += a0 * b1;
        Integer.carry16_(arr, 2 * i + 2 * j + 1);
        arr[2 * i + 2 * j + 2] += a1 * b1;
        Integer.carry16_(arr, 2 * i + 2 * j + 2);
      }
    }

    for (let i = 0; i < len; i++) {
      arr[i] = (arr[2 * i + 1] << 16) | arr[2 * i];
    }
    for (let i = len; i < 2 * len; i++) {
      arr[i] = 0;
    }
    return new Integer(arr, 0);
  }

  /**
   * Carries any overflow from the given index into later entries.
   * @param {Array.<number>} bits Array of 16-bit values in little-endian order.
   * @param {number} index The index in question.
   * @private
   */
  private static carry16_(bits: number[], index: number) {
    while ((bits[index] & 0xFFFF) != bits[index]) {
      bits[index + 1] += bits[index] >>> 16;
      bits[index] &= 0xFFFF;
    }
  }

  /**
   * Returns this Integer divided by the given one.
   * @param {Integer} other Th Integer to divide this by.
   * @return {!Integer} This value divided by the given one.
   */
  divide(other: Integer): Integer {
    if (other.isZero()) {
      throw Error('division by zero');
    } else if (this.isZero()) {
      return Integer.ZERO;
    }

    if (this.isNegative()) {
      if (other.isNegative()) {
        return this.negate().divide(other.negate());
      } 
      return this.negate().divide(other).negate();
      
    } else if (other.isNegative()) {
      return this.divide(other.negate()).negate();
    }

    // Repeat the following until the remainder is less than other:  find a
    // floating-point that approximates remainder / other *from below*, add this
    // into the result, and subtract it from the remainder.  It is critical that
    // the approximate value is less than or equal to the real value so that the
    // remainder never becomes negative.
    let res = Integer.ZERO;
    let rem: Integer = this;
    while (rem.greaterThanOrEqual(other)) {
      // Approximate the result of division. This may be a little greater or
      // smaller than the actual value.
      let approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));


      // We will tweak the approximate result by changing it in the 48-th digit or
      // the smallest non-fractional digit, whichever is larger.
      const log2 = Math.ceil(Math.log(approx) / Math.LN2);
      const delta = (log2 <= 48) ? 1 : Math.pow(2, log2 - 48);

      // Decrease the approximation until it is smaller than the remainder.  Note
      // that if it is too large, the product overflows and is negative.
      let approxRes = Integer.fromNumber(approx);
      let approxRem = approxRes.multiply(other);
      while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
        approx -= delta;
        approxRes = Integer.fromNumber(approx);
        approxRem = approxRes.multiply(other);
      }

      // We know the answer can't be zero... and actually, zero would cause
      // infinite recursion since we would make no progress.
      if (approxRes.isZero()) {
        approxRes = Integer.ONE;
      }

      res = res.add(approxRes);
      rem = rem.subtract(approxRem);
    }
    return res;
  }

  /**
   * Returns this Integer modulo the given one.
   * @param {Integer} other The Integer by which to mod.
   * @return {!Integer} This value modulo the given one.
   */
  modulo(other: Integer): Integer {
    return this.subtract(this.divide(other).multiply(other));
  }

  /** @return {!Integer} The bitwise-NOT of this value. */
  not(): Integer {
    const len = this.bits_.length;
    const arr = [];
    for (let i = 0; i < len; i++) {
      arr[i] = ~this.bits_[i];
    }
    return new Integer(arr, ~this.sign_);
  }

  /**
   * Returns the bitwise-AND of this Integer and the given one.
   * @param {Integer} other The Integer to AND with this.
   * @return {!Integer} The bitwise-AND of this and the other.
   */
  and(other: Integer): Integer {
    const len = Math.max(this.bits_.length, other.bits_.length);
    const arr = [];
    for (let i = 0; i < len; i++) {
      arr[i] = this.getBits(i) & other.getBits(i);
    }
    return new Integer(arr, this.sign_ & other.sign_);
  }

  /**
   * Returns the bitwise-OR of this Integer and the given one.
   * @param {Integer} other The Integer to OR with this.
   * @return {!Integer} The bitwise-OR of this and the other.
   */
  or(other: Integer): Integer {
    const len = Math.max(this.bits_.length, other.bits_.length);
    const arr = [];
    for (let i = 0; i < len; i++) {
      arr[i] = this.getBits(i) | other.getBits(i);
    }
    return new Integer(arr, this.sign_ | other.sign_);
  }

  /**
   * Returns the bitwise-XOR of this Integer and the given one.
   * @param {Integer} other The Integer to XOR with this.
   * @return {!Integer} The bitwise-XOR of this and the other.
   */
  xor(other: Integer): Integer {
    const len = Math.max(this.bits_.length, other.bits_.length);
    const arr = [];
    for (let i = 0; i < len; i++) {
      arr[i] = this.getBits(i) ^ other.getBits(i);
    }
    return new Integer(arr, this.sign_ ^ other.sign_);
  }

  /**
   * Returns this value with bits shifted to the left by the given amount.
   * @param {number} numBits The number of bits by which to shift.
   * @return {!Integer} This shifted to the left by the given amount.
   */
  shiftLeft(numBits: number): Integer {
    const arr_delta = numBits >> 5;
    const bit_delta = numBits % 32;
    const len = this.bits_.length + arr_delta + (bit_delta > 0 ? 1 : 0);
    const arr = [];
    for (let i = 0; i < len; i++) {
      if (bit_delta > 0) {
        arr[i] = (this.getBits(i - arr_delta) << bit_delta) |
          (this.getBits(i - arr_delta - 1) >>> (32 - bit_delta));
      } else {
        arr[i] = this.getBits(i - arr_delta);
      }
    }
    return new Integer(arr, this.sign_);
  }

  /**
   * Returns this value with bits shifted to the right by the given amount.
   * @param {number} numBits The number of bits by which to shift.
   * @return {!Integer} This shifted to the right by the given amount.
   */
  shiftRight(numBits: number): Integer {
    const arr_delta = numBits >> 5;
    const bit_delta = numBits % 32;
    const len = this.bits_.length - arr_delta;
    const arr = [];
    for (let i = 0; i < len; i++) {
      if (bit_delta > 0) {
        arr[i] = (this.getBits(i + arr_delta) >>> bit_delta) |
          (this.getBits(i + arr_delta + 1) << (32 - bit_delta));
      } else {
        arr[i] = this.getBits(i + arr_delta);
      }
    }
    return new Integer(arr, this.sign_);
  }

  /**
   * Provide the name of the constructor and the string representation
   * @returns {string}
   */
  inspect(): string {
    return this.constructor.name + ': ' + this.toString();
  }

  /**
   * Returns a Integer whose value is the absolute value of this
   * @returns {Integer}
   */
  abs(): Integer {
    return this.sign_ === 0 ? this : this.negate();
  }

  /**
   * Returns the string representation.
   * Method used by the native JSON.stringify() to serialize this instance.
   */
  toJSON() {
    return this.toString();
  }
}

export default Integer;
