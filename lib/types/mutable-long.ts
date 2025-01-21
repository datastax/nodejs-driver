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
"use strict";

const Long = require('long');

const TWO_PWR_16_DBL = 1 << 16;
const TWO_PWR_32_DBL = TWO_PWR_16_DBL * TWO_PWR_16_DBL;

/**
 * Constructs a signed int64 representation.
 * @ignore
 */
class MutableLong {
  constructor(b00, b16, b32, b48) {
    // Use an array of uint16
    this._arr = [b00 & 0xffff, b16 & 0xffff, b32 & 0xffff, b48 & 0xffff];
  }

  toString() {
    return this.toImmutable().toString();
  }

  /**
   * Compares this value with the provided value.
   * @param {MutableLong} other
   * @return {number}
   */
  compare(other) {
    const thisNeg = this.isNegative();
    const otherNeg = other.isNegative();
    if (thisNeg && !otherNeg) {
      return -1;
    }
    if (!thisNeg && otherNeg) {
      return 1;
    }
    // At this point the sign bits are the same
    return this._compareBits(other);
  }

  _compareBits(other) {
    for (let i = 3; i >= 0; i--) {
      if (this._arr[i] > other._arr[i]) {
        return 1;
      }
      if (this._arr[i] < other._arr[i]) {
        return -1;
      }
    }
    return 0;
  }

  getUint16(index) {
    return this._arr[index];
  }

  getLowBitsUnsigned() {
    return (this._arr[0] | ((this._arr[1] & 0xffff) << 16)) >>> 0;
  }

  getHighBitsUnsigned() {
    return (this._arr[2] | (this._arr[3] << 16)) >>> 0;
  }

  toNumber() {
    return (this._arr[3] << 16 | this._arr[2]) * TWO_PWR_32_DBL + ((this._arr[1] << 16 | this._arr[0]) >>> 0);
  }

  /**
   * Performs the bitwise NOT of this value.
   * @return {MutableLong}
   */
  not() {
    this._arr[0] = ~this._arr[0] & 0xffff;
    this._arr[1] = ~this._arr[1] & 0xffff;
    this._arr[2] = ~this._arr[2] & 0xffff;
    this._arr[3] = ~this._arr[3] & 0xffff;
    return this;
  }

  add(addend) {
    let c48 = 0, c32 = 0, c16 = 0, c00 = 0;
    c00 += this._arr[0] + addend._arr[0];
    this._arr[0] = c00 & 0xffff;
    c16 += c00 >>> 16;
    c16 += this._arr[1] + addend._arr[1];
    this._arr[1] = c16 & 0xffff;
    c32 += c16 >>> 16;
    c32 += this._arr[2] + addend._arr[2];
    this._arr[2] = c32 & 0xffff;
    c48 += c32 >>> 16;
    c48 += this._arr[3] + addend._arr[3];
    this._arr[3] = c48 & 0xffff;
    return this;
  }

  shiftLeft(numBits) {
    if (numBits === 0) {
      return this;
    }
    if (numBits >= 64) {
      return this.toZero();
    }
    const remainingBits = numBits % 16;
    const pos = Math.floor(numBits / 16);
    if (pos > 0) {
      this._arr[3] = this._arr[3 - pos];
      this._arr[2] = pos > 2 ? 0 : this._arr[2 - pos];
      this._arr[1] = pos > 1 ? 0 : this._arr[0];
      this._arr[0] = 0;
    }
    if (remainingBits > 0) {
      // shift left within the int16 and the next one
      this._arr[3] = ((this._arr[3] << remainingBits) | (this._arr[2] >>> (16 - remainingBits))) & 0xffff;
      this._arr[2] = ((this._arr[2] << remainingBits) | (this._arr[1] >>> (16 - remainingBits))) & 0xffff;
      this._arr[1] = ((this._arr[1] << remainingBits) | (this._arr[0] >>> (16 - remainingBits))) & 0xffff;
      this._arr[0] = (this._arr[0] << remainingBits) & 0xffff;
    }
    return this;
  }

  shiftRightUnsigned(numBits) {
    if (numBits === 0) {
      return this;
    }
    if (numBits >= 64) {
      return this.toZero();
    }
    const remainingBits = numBits % 16;
    const pos = Math.floor(numBits / 16);
    if (pos > 0) {
      this._arr[0] = this._arr[pos];
      this._arr[1] = pos > 2 ? 0 : this._arr[1 + pos];
      this._arr[2] = pos > 1 ? 0 : this._arr[3];
      this._arr[3] = 0;
    }
    if (remainingBits > 0) {
      this._arr[0] = (this._arr[0] >>> remainingBits) | ((this._arr[1] << (16 - remainingBits)) & 0xffff);
      this._arr[1] = (this._arr[1] >>> remainingBits) | ((this._arr[2] << (16 - remainingBits)) & 0xffff);
      this._arr[2] = (this._arr[2] >>> remainingBits) | ((this._arr[3] << (16 - remainingBits)) & 0xffff);
      this._arr[3] = this._arr[3] >>> remainingBits;
    }
    return this;
  }

  or(other) {
    this._arr[0] |= other._arr[0];
    this._arr[1] |= other._arr[1];
    this._arr[2] |= other._arr[2];
    this._arr[3] |= other._arr[3];
    return this;
  }

  /**
   * Returns the bitwise XOR of this Long and the given one.
   * @param {MutableLong} other
   * @returns {MutableLong} this instance.
   */
  xor(other) {
    this._arr[0] ^= other._arr[0];
    this._arr[1] ^= other._arr[1];
    this._arr[2] ^= other._arr[2];
    this._arr[3] ^= other._arr[3];
    return this;
  }

  clone() {
    return new MutableLong(this._arr[0], this._arr[1], this._arr[2], this._arr[3]);
  }

  /**
   * Performs the product of this and the specified Long.
   * @param {MutableLong} multiplier
   * @returns {MutableLong} this instance.
   */
  multiply(multiplier) {
    let negate = false;
    if (this.isZero() || multiplier.isZero()) {
      return this.toZero();
    }
    if (this.isNegative()) {
      this.negate();
      negate = !negate;
    }
    if (multiplier.isNegative()) {
      multiplier = multiplier.clone().negate();
      negate = !negate;
    }
    // We can skip products that would overflow.
    let c48 = 0, c32 = 0, c16 = 0, c00 = 0;
    c00 += this._arr[0] * multiplier._arr[0];
    c16 += c00 >>> 16;
    c16 += this._arr[1] * multiplier._arr[0];
    c32 += c16 >>> 16;
    c16 &= 0xFFFF;
    c16 += this._arr[0] * multiplier._arr[1];
    c32 += c16 >>> 16;
    c32 += this._arr[2] * multiplier._arr[0];
    c48 += c32 >>> 16;
    c32 &= 0xFFFF;
    c32 += this._arr[1] * multiplier._arr[1];
    c48 += c32 >>> 16;
    c32 &= 0xFFFF;
    c32 += this._arr[0] * multiplier._arr[2];
    c48 += c32 >>> 16;
    c48 += this._arr[3] * multiplier._arr[0] + this._arr[2] * multiplier._arr[1] +
      this._arr[1] * multiplier._arr[2] + this._arr[0] * multiplier._arr[3];
    this._arr[0] = c00 & 0xffff;
    this._arr[1] = c16 & 0xffff;
    this._arr[2] = c32 & 0xffff;
    this._arr[3] = c48 & 0xffff;
    if (negate) {
      this.negate();
    }
    return this;
  }

  toZero() {
    this._arr[3] = this._arr[2] = this._arr[1] = this._arr[0] = 0;
    return this;
  }

  isZero() {
    return (this._arr[3] === 0 && this._arr[2] === 0 && this._arr[1] === 0 && this._arr[0] === 0);
  }

  isNegative() {
    // most significant bit turned on
    return (this._arr[3] & 0x8000) > 0;
  }

  /**
   * Negates this value.
   * @return {MutableLong}
   */
  negate() {
    return this.not().add(MutableLong.one);
  }

  equals(other) {
    if (!(other instanceof MutableLong)) {
      return false;
    }
    return (this._arr[0] === other._arr[0] && this._arr[1] === other._arr[1] &&
      this._arr[2] === other._arr[2] && this._arr[3] === other._arr[3]);
  }

  toImmutable() {
    return Long.fromBits(this.getLowBitsUnsigned(), this.getHighBitsUnsigned(), false);
  }

  static fromNumber(value) {
    if (isNaN(value) || !isFinite(value)) {
      return new MutableLong();
    }
    if (value < 0) {
      return MutableLong.fromNumber(-value).negate();
    }
    const low32Bits = value % TWO_PWR_32_DBL;
    const high32Bits = value / TWO_PWR_32_DBL;
    return MutableLong.fromBits(low32Bits, high32Bits);
  }

  static fromBits(low32Bits, high32Bits) {
    return new MutableLong(low32Bits, low32Bits >>> 16, high32Bits, high32Bits >>> 16);
  }

  /**
   * Returns a Long representation of the given string, written using the specified radix.
   * @param {String} str
   * @param {Number} [radix]
   * @return {MutableLong}
   */
  static fromString(str, radix) {
    if (typeof str !== 'string') {
      throw new Error('String format is not valid: ' + str);
    }
    if (str.length === 0) {
      throw Error('number format error: empty string');
    }
    if (str === "NaN" || str === "Infinity" || str === "+Infinity" || str === "-Infinity") {
      return new MutableLong();
    }
    radix = radix || 10;
    if (radix < 2 || radix > 36) {
      throw Error('radix out of range: ' + radix);
    }
    let p;
    if ((p = str.indexOf('-')) > 0) {
      throw Error('number format error: interior "-" character: ' + str);
    }
    if (p === 0) {
      return MutableLong.fromString(str.substring(1), radix).negate();
    }
    // Do several (8) digits each time through the loop
    const radixToPower = MutableLong.fromNumber(Math.pow(radix, 8));
    const result = new MutableLong();
    for (let i = 0; i < str.length; i += 8) {
      const size = Math.min(8, str.length - i);
      const value = parseInt(str.substring(i, i + size), radix);
      if (size < 8) {
        const power = MutableLong.fromNumber(Math.pow(radix, size));
        result.multiply(power).add(MutableLong.fromNumber(value));
        break;
      }
      result.multiply(radixToPower);
      result.add(MutableLong.fromNumber(value));
    }
    return result;
  }
}

MutableLong.one = new MutableLong(1, 0, 0, 0);

module.exports = MutableLong;