'use strict';
var Long = require('long');
var util = require('util');
var utils = require('../utils');

/** @module types */

// Reuse the same buffers that should perform slightly better than built-in buffer pool
var reusableBuffers = {
  months: utils.allocBuffer(9),
  days: utils.allocBuffer(9),
  nanoseconds: utils.allocBuffer(9)
};

var maxInt32 = 0x7FFFFFFF;
var longOneThousand = Long.fromInt(1000);
var nanosPerMicro = longOneThousand;
var nanosPerMilli = longOneThousand.multiply(nanosPerMicro);
var nanosPerSecond = longOneThousand.multiply(nanosPerMilli);
var nanosPerMinute = Long.fromInt(60).multiply(nanosPerSecond);
var nanosPerHour = Long.fromInt(60).multiply(nanosPerMinute);
var daysPerWeek = 7;
var monthsPerYear = 12;
var standardRegex = /(\d+)(y|mo|w|d|h|s|ms|us|µs|ns|m)/gi;
var iso8601Regex = /P((\d+)Y)?((\d+)M)?((\d+)D)?(T((\d+)H)?((\d+)M)?((\d+)S)?)?/;
var iso8601WeekRegex = /P(\d+)W/;
var iso8601AlternateRegex = /P(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/;

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
  var lengthMonths = VIntCoding.writeVInt(Long.fromNumber(this.months), reusableBuffers.months);
  var lengthDays = VIntCoding.writeVInt(Long.fromNumber(this.days), reusableBuffers.days);
  var lengthNanoseconds = VIntCoding.writeVInt(this.nanoseconds, reusableBuffers.nanoseconds);
  var buffer = utils.allocBufferUnsafe(lengthMonths + lengthDays + lengthNanoseconds);
  reusableBuffers.months.copy(buffer, 0, 0, lengthMonths);
  var offset = lengthMonths;
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
  var value = '';
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
  var remainder = append(Math.abs(this.months), monthsPerYear, "y");
  append(remainder, 1, "mo");
  append(Math.abs(this.days), 1, "d");

  if (!this.nanoseconds.equals(Long.ZERO)) {
    var nanos = this.nanoseconds.isNegative() ? this.nanoseconds.negate() : this.nanoseconds;
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
  var offset = { value: 0 };
  var months = VIntCoding.readVInt(buffer, offset).toNumber();
  var days = VIntCoding.readVInt(buffer, offset).toNumber();
  var nanoseconds = VIntCoding.readVInt(buffer, offset);
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
  var isNegative = input.charAt(0) === '-';
  var source = isNegative ? input.substr(1) : input;
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
  var builder = new Builder(isNegative);
  standardRegex.lastIndex = 0;
  var matches;
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
  var matches = iso8601Regex.exec(source);
  if (!matches || matches[0] !== source) {
    throw new TypeError(util.format("Unable to convert '%s' to a duration", source));
  }
  var builder = new Builder(isNegative);
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
  var matches = iso8601WeekRegex.exec(source);
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
  var matches = iso8601AlternateRegex.exec(source);
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
  var name = this._unitByIndex[+unitIndex];
  if (!name) {
    throw new Error('unknown unit index: ' + unitIndex);
  }
  return name;
};

Builder.prototype.add = function (textValue, symbol) {
  var addMethod = this._addMethods[symbol.toLowerCase()];
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
  var value = +years;
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
  var value = +months;
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
  var value = +weeks;
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
  var value = +days;
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
  var value = typeof hours === 'string' ? Long.fromString(hours) : hours;
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
  var value = typeof minutes === 'string' ? Long.fromString(minutes) : minutes;
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
  var value = typeof seconds === 'string' ? Long.fromString(seconds) : seconds;
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
  var value = typeof millis === 'string' ? Long.fromString(millis) : millis;
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
  var value = typeof micros === 'string' ? Long.fromString(micros) : micros;
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
  var value = typeof nanos === 'string' ? Long.fromString(nanos) : nanos;
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

/**
 * Contains the methods for reading and writing vints into binary format.
 * Exposes only 2 internal methods, the rest are hidden.
 * @private
 */
var VIntCoding = (function () {
  /** @param {Long} n */
  function encodeZigZag64(n) {
    //     (n << 1) ^ (n >> 63);
    return n.toUnsigned().shiftLeft(1).xor(n.shiftRight(63));
  }

  /** @param {Long} n */
  function decodeZigZag64(n) {
    //     (n >>> 1) ^ -(n & 1);
    return n.shiftRightUnsigned(1).xor(n.and(Long.ONE).negate());
  }

  /**
   * @param {Long} value
   * @param {Buffer} buffer
   * @returns {Number}
   */
  function writeVInt(value, buffer) {
    return writeUnsignedVInt(encodeZigZag64(value), buffer);
  }

  /**
   * @param {Long} value
   * @param {Buffer} buffer
   * @returns {number}
   */
  function writeUnsignedVInt(value, buffer) {
    var size = computeUnsignedVIntSize(value);
    if (size === 1) {
      buffer[0] = value.getLowBits();
      return 1;
    }
    encodeVInt(value, size, buffer);
    return size;
  }

  /**
   * @param {Long} value
   * @returns {number}
   */
  function computeUnsignedVIntSize(value) {
    var magnitude = numberOfLeadingZeros(value.or(Long.ONE));
    return (639 - magnitude * 9) >> 6;
  }

  /**
   * @param {Long} value
   * @param {Number} size
   * @param {Buffer} buffer
   */
  function encodeVInt(value, size, buffer) {
    var extraBytes = size - 1;
    var intValue = value.getLowBits();
    var i;
    var intBytes = 4;
    for (i = extraBytes; i >= 0 && (intBytes--) > 0; i--) {
      buffer[i] = 0xFF & intValue;
      intValue >>= 8;
    }
    intValue = value.getHighBits();
    for (; i >= 0; i--) {
      buffer[i] = 0xFF & intValue;
      intValue >>= 8;
    }
    buffer[0] |= encodeExtraBytesToRead(extraBytes);
  }
  /**
   * Returns the number of zero bits preceding the highest-order one-bit in the binary representation of the value.
   * @param {Long} value
   * @returns {Number}
   */
  function numberOfLeadingZeros(value) {
    if (value.equals(Long.ZERO)) {
      return 64;
    }
    var n = 1;
    var x = value.getHighBits();
    if (x === 0) {
      n += 32;
      x = value.getLowBits();
    }
    if (x >>> 16 === 0) {
      n += 16;
      x <<= 16;
    }
    if (x >>> 24 === 0) {
      n += 8;
      x <<= 8;
    }
    if (x >>> 28 === 0) {
      n += 4;
      x <<= 4;
    }
    if (x >>> 30 === 0) {
      n += 2;
      x <<= 2;
    }
    n -= x >>> 31;
    return n;
  }


  function encodeExtraBytesToRead(extraBytesToRead) {
    return ~(0xff >> extraBytesToRead);
  }

  /**
   * @param {Buffer} buffer
   * @param {{value: number}} offset
   * @returns {Long}
   */
  function readVInt(buffer, offset) {
    return decodeZigZag64(readUnsignedVInt(buffer, offset));
  }

  /**
   * @param {Buffer} input
   * @param {{ value: number}} offset
   * @returns {Long}
   */
  function readUnsignedVInt(input, offset) {
    var firstByte = input[offset.value++];
    if ((firstByte & 0x80) === 0) {
      return Long.fromInt(firstByte);
    }
    var sByteInt = fromSignedByteToInt(firstByte);
    var size = numberOfExtraBytesToRead(sByteInt);
    var result = Long.fromInt(sByteInt & firstByteValueMask(size));
    for (var ii = 0; ii < size; ii++) {
      var b = Long.fromInt(input[offset.value++]);
      //       (result << 8) | b
      result = result.shiftLeft(8).or(b);
    }
    return result;
  }

  function fromSignedByteToInt(value) {
    if (value > 0x7f) {
      return value - 0x0100;
    }
    return value;
  }

  function numberOfLeadingZerosInt32(i) {
    if (i === 0) {
      return 32;
    }
    var n = 1;
    if (i >>> 16 === 0) {
      n += 16;
      i <<= 16;
    }
    if (i >>> 24 === 0) {
      n += 8;
      i <<= 8;
    }
    if (i >>> 28 === 0) {
      n += 4;
      i <<= 4;
    }
    if (i >>> 30 === 0) {
      n += 2;
      i <<= 2;
    }
    n -= i >>> 31;
    return n;
  }

  /**
   * @param {Number} firstByte
   * @returns {Number}
   */
  function numberOfExtraBytesToRead(firstByte) {
    // Instead of counting 1s of the byte, we negate and count 0 of the byte
    return numberOfLeadingZerosInt32(~firstByte) - 24;
  }

  /**
   * @param {Number} extraBytesToRead
   * @returns {Number}
   */
  function firstByteValueMask(extraBytesToRead) {
    return 0xff >> extraBytesToRead;
  }

  return {
    readVInt: readVInt,
    writeVInt: writeVInt
  };
})();

module.exports = Duration;
