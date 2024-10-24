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
'use strict';
const Long = require('long');
const util = require('util');
const utils = require('../utils');
const VIntCoding = utils.VIntCoding;

/** @module types */

// Reuse the same buffers that should perform slightly better than built-in buffer pool
const reusableBuffers = {
  months: utils.allocBuffer(9),
  days: utils.allocBuffer(9),
  nanoseconds: utils.allocBuffer(9)
};

const maxInt32 = 0x7FFFFFFF;
const longOneThousand = Long.fromInt(1000);
const nanosPerMicro = longOneThousand;
const nanosPerMilli = longOneThousand.multiply(nanosPerMicro);
const nanosPerSecond = longOneThousand.multiply(nanosPerMilli);
const nanosPerMinute = Long.fromInt(60).multiply(nanosPerSecond);
const nanosPerHour = Long.fromInt(60).multiply(nanosPerMinute);
const daysPerWeek = 7;
const monthsPerYear = 12;
const standardRegex = /(\d+)(y|mo|w|d|h|s|ms|us|µs|ns|m)/gi;
const iso8601Regex = /P((\d+)Y)?((\d+)M)?((\d+)D)?(T((\d+)H)?((\d+)M)?((\d+)S)?)?/;
const iso8601WeekRegex = /P(\d+)W/;
const iso8601AlternateRegex = /P(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/;

/**
 * Creates a new instance of {@link Duration}.
 * @classdesc
 * Represents a duration. A duration stores separately months, days, and seconds due to the fact that the number of
 * days in a month varies, and a day can have 23 or 25 hours if a daylight saving is involved.
 * @param {Number} months The number of months.
 * @param {Number} days The number of days.
 * @param {Number|Long} nanoseconds The number of nanoseconds.
 * @constructor
 */
function Duration(months, days, nanoseconds) {
  /**
   * Gets the number of months.
   * @type {Number}
   */
  this.months = months;
  /**
   * Gets the number of days.
   * @type {Number}
   */
  this.days = days;
  /**
   * Gets the number of nanoseconds represented as a <code>int64</code>.
   * @type {Long}
   */
  this.nanoseconds = typeof nanoseconds === 'number' ? Long.fromNumber(nanoseconds) : nanoseconds;
}

Duration.prototype.equals = function (other) {
  if (!(other instanceof Duration)) {
    return false;
  }
  return this.months === other.months &&
    this.days === other.days &&
    this.nanoseconds.equals(other.nanoseconds);
};

/**
 * Serializes the duration and returns the representation of the value in bytes.
 * @returns {Buffer}
 */
Duration.prototype.toBuffer = function () {
  const lengthMonths = VIntCoding.writeVInt(Long.fromNumber(this.months), reusableBuffers.months);
  const lengthDays = VIntCoding.writeVInt(Long.fromNumber(this.days), reusableBuffers.days);
  const lengthNanoseconds = VIntCoding.writeVInt(this.nanoseconds, reusableBuffers.nanoseconds);
  const buffer = utils.allocBufferUnsafe(lengthMonths + lengthDays + lengthNanoseconds);
  reusableBuffers.months.copy(buffer, 0, 0, lengthMonths);
  let offset = lengthMonths;
  reusableBuffers.days.copy(buffer, offset, 0, lengthDays);
  offset += lengthDays;
  reusableBuffers.nanoseconds.copy(buffer, offset, 0, lengthNanoseconds);
  return buffer;
};

/**
 * Returns the string representation of the value.
 * @return {string}
 */
Duration.prototype.toString = function () {
  let value = '';
  function append(dividend, divisor, unit) {
    if (dividend === 0 || dividend < divisor) {
      return dividend;
    }
    // string concatenation is supposed to be fasted than join()
    value += (dividend / divisor).toFixed(0) + unit;
    return dividend % divisor;
  }
  function append64(dividend, divisor, unit) {
    if (dividend.equals(Long.ZERO) || dividend.lessThan(divisor)) {
      return dividend;
    }
    // string concatenation is supposed to be fasted than join()
    value += dividend.divide(divisor).toString() + unit;
    return dividend.modulo(divisor);
  }
  if (this.months < 0 || this.days < 0 || this.nanoseconds.isNegative()) {
    value = '-';
  }
  let remainder = append(Math.abs(this.months), monthsPerYear, "y");
  append(remainder, 1, "mo");
  append(Math.abs(this.days), 1, "d");

  if (!this.nanoseconds.equals(Long.ZERO)) {
    const nanos = this.nanoseconds.isNegative() ? this.nanoseconds.negate() : this.nanoseconds;
    remainder = append64(nanos, nanosPerHour, "h");
    remainder = append64(remainder, nanosPerMinute, "m");
    remainder = append64(remainder, nanosPerSecond, "s");
    remainder = append64(remainder, nanosPerMilli, "ms");
    remainder = append64(remainder, nanosPerMicro, "us");
    append64(remainder, Long.ONE, "ns");
  }
  return value;
};

/**
 * Creates a new {@link Duration} instance from the binary representation of the value.
 * @param {Buffer} buffer
 * @returns {Duration}
 */
Duration.fromBuffer = function (buffer) {
  const offset = { value: 0 };
  const months = VIntCoding.readVInt(buffer, offset).toNumber();
  const days = VIntCoding.readVInt(buffer, offset).toNumber();
  const nanoseconds = VIntCoding.readVInt(buffer, offset);
  return new Duration(months, days, nanoseconds);
};

/**
 * Creates a new {@link Duration} instance from the string representation of the value.
 * <p>
 *   Accepted formats:
 * </p>
 * <ul>
 * <li>multiple digits followed by a time unit like: 12h30m where the time unit can be:
 *   <ul>
 *     <li>{@code y}: years</li>
 *     <li>{@code m}: months</li>
 *     <li>{@code w}: weeks</li>
 *     <li>{@code d}: days</li>
 *     <li>{@code h}: hours</li>
 *     <li>{@code m}: minutes</li>
 *     <li>{@code s}: seconds</li>
 *     <li>{@code ms}: milliseconds</li>
 *     <li>{@code us} or {@code µs}: microseconds</li>
 *     <li>{@code ns}: nanoseconds</li>
 *   </ul>
 * </li>
 * <li>ISO 8601 format:  <code>P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W</code></li>
 * <li>ISO 8601 alternative format: <code>P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]</code></li>
 * </ul>
 * @param {String} input
 * @returns {Duration}
 */
Duration.fromString = function (input) {
  const isNegative = input.charAt(0) === '-';
  const source = isNegative ? input.substr(1) : input;
  if (source.charAt(0) === 'P') {
    if (source.charAt(source.length - 1) === 'W') {
      return parseIso8601WeekFormat(isNegative, source);
    }
    if (source.indexOf('-') > 0) {
      return parseIso8601AlternativeFormat(isNegative, source);
    }
    return parseIso8601Format(isNegative, source);
  }
  return parseStandardFormat(isNegative, source);
};

/**
 * @param {Boolean} isNegative
 * @param {String} source
 * @returns {Duration}
 * @private
 */
function parseStandardFormat(isNegative, source) {
  const builder = new Builder(isNegative);
  standardRegex.lastIndex = 0;
  let matches;
  while ((matches = standardRegex.exec(source)) && matches.length <= 3) {
    builder.add(matches[1], matches[2]);
  }
  return builder.build();
}

/**
 * @param {Boolean} isNegative
 * @param {String} source
 * @returns {Duration}
 * @private
 */
function parseIso8601Format(isNegative, source) {
  const matches = iso8601Regex.exec(source);
  if (!matches || matches[0] !== source) {
    throw new TypeError(util.format("Unable to convert '%s' to a duration", source));
  }
  const builder = new Builder(isNegative);
  if (matches[1]) {
    builder.addYears(matches[2]);
  }
  if (matches[3]) {
    builder.addMonths(matches[4]);
  }
  if (matches[5]) {
    builder.addDays(matches[6]);
  }
  if (matches[7]) {
    if (matches[8]) {
      builder.addHours(matches[9]);
    }
    if (matches[10]) {
      builder.addMinutes(matches[11]);
    }
    if (matches[12]) {
      builder.addSeconds(matches[13]);
    }
  }
  return builder.build();
}

/**
 * @param {Boolean} isNegative
 * @param {String} source
 * @returns {Duration}
 * @private
 */
function parseIso8601WeekFormat(isNegative, source) {
  const matches = iso8601WeekRegex.exec(source);
  if (!matches || matches[0] !== source) {
    throw new TypeError(util.format("Unable to convert '%s' to a duration", source));
  }
  return new Builder(isNegative)
    .addWeeks(matches[1])
    .build();
}

/**
 * @param {Boolean} isNegative
 * @param {String} source
 * @returns {Duration}
 * @private
 */
function parseIso8601AlternativeFormat(isNegative, source) {
  const matches = iso8601AlternateRegex.exec(source);
  if (!matches || matches[0] !== source) {
    throw new TypeError(util.format("Unable to convert '%s' to a duration", source));
  }
  return new Builder(isNegative).addYears(matches[1])
    .addMonths(matches[2])
    .addDays(matches[3])
    .addHours(matches[4])
    .addMinutes(matches[5])
    .addSeconds(matches[6])
    .build();
}

/**
 * @param {Boolean} isNegative
 * @private
 * @constructor
 */
function Builder(isNegative) {
  this._isNegative = isNegative;
  this._unitIndex = 0;
  this._months = 0;
  this._days = 0;
  this._nanoseconds = Long.ZERO;
  this._addMethods = {
    'y': this.addYears,
    'mo': this.addMonths,
    'w': this.addWeeks,
    'd': this.addDays,
    'h': this.addHours,
    'm': this.addMinutes,
    's': this.addSeconds,
    'ms': this.addMillis,
    // µs
    '\u00B5s': this.addMicros,
    'us': this.addMicros,
    'ns': this.addNanos
  };
  this._unitByIndex = [
    null, 'years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'milliseconds', 'microseconds',
    'nanoseconds'
  ];
}

Builder.prototype._validateOrder = function (unitIndex) {
  if (unitIndex === this._unitIndex) {
    throw new TypeError(util.format("Invalid duration. The %s are specified multiple times", this._getUnitName(unitIndex)));
  }

  if (unitIndex <= this._unitIndex) {
    throw new TypeError(util.format("Invalid duration. The %s should be after %s",
      this._getUnitName(this._unitIndex),
      this._getUnitName(unitIndex)));
  }
  this._unitIndex = unitIndex;
};

/**
 * @param {Number} units
 * @param {Number} monthsPerUnit
 */
Builder.prototype._validateMonths = function(units, monthsPerUnit) {
  this._validate32(units, (maxInt32 - this._months) / monthsPerUnit, "months");
};

/**
 * @param {Number} units
 * @param {Number} daysPerUnit
 */
Builder.prototype._validateDays = function(units, daysPerUnit) {
  this._validate32(units, (maxInt32 - this._days) / daysPerUnit, "days");
};

/**
 * @param {Long} units
 * @param {Long} nanosPerUnit
 */
Builder.prototype._validateNanos = function(units, nanosPerUnit) {
  this._validate64(units, Long.MAX_VALUE.subtract(this._nanoseconds).divide(nanosPerUnit), "nanoseconds");
};

/**
 * @param {Number} units
 * @param {Number} limit
 * @param {String} unitName
 */
Builder.prototype._validate32 = function(units, limit, unitName) {
  if (units > limit) {
    throw new TypeError(util.format('Invalid duration. The total number of %s must be less or equal to %s',
      unitName,
      maxInt32));
  }
};

/**
 * @param {Long} units
 * @param {Long} limit
 * @param {String} unitName
 */
Builder.prototype._validate64 = function(units, limit, unitName) {
  if (units.greaterThan(limit)) {
    throw new TypeError(util.format('Invalid duration. The total number of %s must be less or equal to %s',
      unitName,
      Long.MAX_VALUE.toString()));
  }
};

Builder.prototype._getUnitName = function(unitIndex) {
  const name = this._unitByIndex[+unitIndex];
  if (!name) {
    throw new Error('unknown unit index: ' + unitIndex);
  }
  return name;
};

Builder.prototype.add = function (textValue, symbol) {
  const addMethod = this._addMethods[symbol.toLowerCase()];
  if (!addMethod) {
    throw new TypeError(util.format("Unknown duration symbol '%s'", symbol));
  }
  return addMethod.call(this, textValue);
};

/**
 * @param {String|Number} years
 * @return {Builder}
 */
Builder.prototype.addYears = function (years) {
  const value = +years;
  this._validateOrder(1);
  this._validateMonths(value, monthsPerYear);
  this._months += value * monthsPerYear;
  return this;
};

/**
 * @param {String|Number} months
 * @return {Builder}
 */
Builder.prototype.addMonths = function(months) {
  const value = +months;
  this._validateOrder(2);
  this._validateMonths(value, 1);
  this._months += value;
  return this;
};

/**
 * @param {String|Number} weeks
 * @return {Builder}
 */
Builder.prototype.addWeeks = function(weeks) {
  const value = +weeks;
  this._validateOrder(3);
  this._validateDays(value, daysPerWeek);
  this._days += value * daysPerWeek;
  return this;
};

/**
 * @param {String|Number} days
 * @return {Builder}
 */
Builder.prototype.addDays = function(days) {
  const value = +days;
  this._validateOrder(4);
  this._validateDays(value, 1);
  this._days += value;
  return this;
};

/**
 * @param {String|Long} hours
 * @return {Builder}
 */
Builder.prototype.addHours = function(hours) {
  const value = typeof hours === 'string' ? Long.fromString(hours) : hours;
  this._validateOrder(5);
  this._validateNanos(value, nanosPerHour);
  this._nanoseconds = this._nanoseconds.add(value.multiply(nanosPerHour));
  return this;
};

/**
 * @param {String|Long} minutes
 * @return {Builder}
 */
Builder.prototype.addMinutes = function(minutes) {
  const value = typeof minutes === 'string' ? Long.fromString(minutes) : minutes;
  this._validateOrder(6);
  this._validateNanos(value, nanosPerMinute);
  this._nanoseconds = this._nanoseconds.add(value.multiply(nanosPerMinute));
  return this;
};

/**
 * @param {String|Long} seconds
 * @return {Builder}
 */
Builder.prototype.addSeconds = function(seconds) {
  const value = typeof seconds === 'string' ? Long.fromString(seconds) : seconds;
  this._validateOrder(7);
  this._validateNanos(value, nanosPerSecond);
  this._nanoseconds = this._nanoseconds.add(value.multiply(nanosPerSecond));
  return this;
};

/**
 * @param {String|Long} millis
 * @return {Builder}
 */
Builder.prototype.addMillis = function(millis) {
  const value = typeof millis === 'string' ? Long.fromString(millis) : millis;
  this._validateOrder(8);
  this._validateNanos(value, nanosPerMilli);
  this._nanoseconds = this._nanoseconds.add(value.multiply(nanosPerMilli));
  return this;
};

/**
 * @param {String|Long} micros
 * @return {Builder}
 */
Builder.prototype.addMicros = function(micros) {
  const value = typeof micros === 'string' ? Long.fromString(micros) : micros;
  this._validateOrder(9);
  this._validateNanos(value, nanosPerMicro);
  this._nanoseconds = this._nanoseconds.add(value.multiply(nanosPerMicro));
  return this;
};

/**
 * @param {String|Long} nanos
 * @return {Builder}
 */
Builder.prototype.addNanos = function(nanos) {
  const value = typeof nanos === 'string' ? Long.fromString(nanos) : nanos;
  this._validateOrder(10);
  this._validateNanos(value, Long.ONE);
  this._nanoseconds = this._nanoseconds.add(value);
  return this;
};

/** @return {Duration} */
Builder.prototype.build = function () {
  return (this._isNegative ?
    new Duration(-this._months, -this._days, this._nanoseconds.negate()) :
    new Duration(this._months, this._days, this._nanoseconds));
};

module.exports = Duration;
