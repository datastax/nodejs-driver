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

const utils = require('../../utils');
const Long = require('long');

/**
 * Regex to parse dates in the following format YYYY-MM-DDThh:mm:ss.mssZ
 * Looks cumbersome but it's straightforward:
 * - "(\d{1,6})": year mandatory 1 to 6 digits
 * - (?:-(\d{1,2}))?(?:-(\d{1,2}))? two non-capturing groups representing the month and day (1 to 2 digits captured).
 * - (?:T(\d{1,2}?)?(?::(\d{1,2}))?(?::(\d{1,2}))?)?Z? A non-capturing group for the time portion
 * @private
 */
const dateRegex =
  /^[-+]?(\d{1,6})(?:-(\d{1,2}))?(?:-(\d{1,2}))?(?:T(\d{1,2}?)?(?::(\d{1,2}))?(?::(\d{1,2})(?:\.(\d{1,3}))?)?)?Z?$/;
const multipleBoundariesRegex = /^\[(.+?) TO (.+)]$/;
const unbounded = Object.freeze(new DateRangeBound(null, -1));

const dateRangeType = {
  // single value as in "2001-01-01"
  singleValue: 0,
  // closed range as in "[2001-01-01 TO 2001-01-31]"
  closedRange: 1,
  // open range high as in "[2001-01-01 TO *]"
  openRangeHigh: 2,
  // - 0x03 - open range low as in "[* TO 2001-01-01]"
  openRangeLow: 3,
  // - 0x04 - both ranges open as in "[* TO *]"
  openBoth: 4,
  // - 0x05 - single open range as in "[*]"
  openSingle: 5
};

/**
 * Defines the possible values of date range precision.
 * @type {Object}
 * @property {Number} year
 * @property {Number} month
 * @property {Number} day
 * @property {Number} hour
 * @property {Number} minute
 * @property {Number} second
 * @property {Number} millisecond
 * @memberof module:search
 */
const dateRangePrecision = {
  year: 0,
  month: 1,
  day: 2,
  hour: 3,
  minute: 4,
  second: 5,
  millisecond: 6
};

/**
 * Creates a new instance of <code>DateRange</code> using a lower bound and an upper bound.
 * <p>Consider using <code>DateRange.fromString()</code> to create instances more easily.</p>
 * @classdesc
 * Represents a range of dates, corresponding to the Apache Solr type
 * <a href="https://cwiki.apache.org/confluence/display/solr/Working+with+Dates"><code>DateRangeField</code></a>.
 * <p>
 *   A date range can have one or two bounds, namely lower bound and upper bound, to represent an interval of time.
 *   Date range bounds are both inclusive. For example:
 * </p>
 * <ul>
 *   <li><code>2015 TO 2016-10</code> represents from the first day of 2015 to the last day of October 2016</li>
 *   <li><code>2015</code> represents during the course of the year 2015.</li>
 *   <li><code>2017 TO *</code> represents any date greater or equals to the first day of the year 2017.</li>
 * </ul>
 * <p>
 *   Note that this JavaScript representation of <code>DateRangeField</code> does not support Dates outside of the range
 *   supported by ECMAScript Date: –100,000,000 days to 100,000,000 days measured relative to midnight at the
 *   beginning of 01 January, 1970 UTC. Being <code>-271821-04-20T00:00:00.000Z</code> the minimum lower boundary
 *   and <code>275760-09-13T00:00:00.000Z</code> the maximum higher boundary.
 * <p>
 * @param {DateRangeBound} lowerBound A value representing the range lower bound, composed by a
 * <code>Date</code> and a precision. Use <code>DateRangeBound.unbounded</code> for an open lower bound.
 * @param {DateRangeBound} [upperBound] A value representing the range upper bound, composed by a
 * <code>Date</code> and a precision. Use <code>DateRangeBound.unbounded</code> for an open upper bound. When it's not
 * defined, the <code>DateRange</code> instance is considered as a single value range.
 * @constructor
 * @memberOf module:datastax/search
 */
function DateRange(lowerBound, upperBound) {
  if (!lowerBound) {
    throw new TypeError('The lower boundaries must be defined');
  }
  /**
   * Gets the lower bound of this range (inclusive).
   * @type {DateRangeBound}
   */
  this.lowerBound = lowerBound;
  /**
   * Gets the upper bound of this range (inclusive).
   * @type {DateRangeBound|null}
   */
  this.upperBound = upperBound || null;

  // Define the type
  if (this.upperBound === null) {
    if (this.lowerBound !== unbounded) {
      this._type = dateRangeType.singleValue;
    }
    else {
      this._type = dateRangeType.openSingle;
    }
  }
  else {
    if (this.lowerBound !== unbounded) {
      this._type = this.upperBound !== unbounded ? dateRangeType.closedRange : dateRangeType.openRangeHigh;
    }
    else {
      this._type = this.upperBound !== unbounded ? dateRangeType.openRangeLow : dateRangeType.openBoth;
    }
  }
}

/**
 * Returns true if the value of this DateRange instance and other are the same.
 * @param {DateRange} other
 * @returns {Boolean}
 */
DateRange.prototype.equals = function (other) {
  if (!(other instanceof DateRange)) {
    return false;
  }
  return (other.lowerBound.equals(this.lowerBound) &&
  (other.upperBound ? other.upperBound.equals(this.upperBound) : !this.upperBound));
};

/**
 * Returns the string representation of the instance.
 * @return {String}
 */
DateRange.prototype.toString = function () {
  if (this.upperBound === null) {
    return this.lowerBound.toString();
  }
  return '[' + this.lowerBound.toString() + ' TO ' + this.upperBound.toString() + ']';
};

DateRange.prototype.toBuffer = function () {
  // Serializes the value containing:
  // <type>[<time0><precision0><time1><precision1>]
  if (this._type === dateRangeType.openBoth || this._type === dateRangeType.openSingle) {
    return utils.allocBufferFromArray([ this._type ]);
  }
  let buffer;
  let offset = 0;
  if (this._type !== dateRangeType.closedRange) {
    // byte + long + byte
    const boundary = this._type !== dateRangeType.openRangeLow ? this.lowerBound : this.upperBound;
    buffer = utils.allocBufferUnsafe(10);
    buffer.writeUInt8(this._type, offset++);
    offset = writeDate(boundary.date, buffer, offset);
    buffer.writeUInt8(boundary.precision, offset);
    return buffer;
  }
  // byte + long + byte + long + byte
  buffer = utils.allocBufferUnsafe(19);
  buffer.writeUInt8(this._type, offset++);
  offset = writeDate(this.lowerBound.date, buffer, offset);
  buffer.writeUInt8(this.lowerBound.precision, offset++);
  offset = writeDate(this.upperBound.date, buffer, offset);
  buffer.writeUInt8(this.upperBound.precision, offset);
  return buffer;
};

/**
 * Returns the <code>DateRange</code> representation of a given string.
 * <p>String representations of dates are always expressed in Coordinated Universal Time (UTC)</p>
 * @param {String} dateRangeString
 */
DateRange.fromString = function (dateRangeString) {
  const matches = multipleBoundariesRegex.exec(dateRangeString);
  if (!matches) {
    return new DateRange(DateRangeBound.toLowerBound(DateRangeBound.fromString(dateRangeString)));
  }
  return new DateRange(DateRangeBound.toLowerBound(DateRangeBound.fromString(matches[1])), DateRangeBound.toUpperBound(DateRangeBound.fromString(matches[2])));
};

/**
 * Deserializes the buffer into a <code>DateRange</code>
 * @param {Buffer} buffer
 * @return {DateRange}
 */
DateRange.fromBuffer = function (buffer) {
  if (buffer.length === 0) {
    throw new TypeError('DateRange serialized value must have at least 1 byte');
  }
  const type = buffer.readUInt8(0);
  if (type === dateRangeType.openBoth) {
    return new DateRange(unbounded, unbounded);
  }
  if (type === dateRangeType.openSingle) {
    return new DateRange(unbounded);
  }
  let offset = 1;
  let date1;
  let lowerBound;
  let upperBound = null;
  if (type !== dateRangeType.closedRange) {
    date1 = readDate(buffer, offset);
    offset += 8;
    lowerBound = new DateRangeBound(date1, buffer.readUInt8(offset));
    if (type === dateRangeType.openRangeLow) {
      // lower boundary is open, the first serialized boundary is the upperBound
      upperBound = lowerBound;
      lowerBound = unbounded;
    }
    else {
      upperBound = type === dateRangeType.openRangeHigh ? unbounded : null;
    }
    return new DateRange(lowerBound, upperBound);
  }
  date1 = readDate(buffer, offset);
  offset += 8;
  lowerBound = new DateRangeBound(date1, buffer.readUInt8(offset++));
  const date2 = readDate(buffer, offset);
  offset += 8;
  upperBound = new DateRangeBound(date2, buffer.readUInt8(offset));
  return new DateRange(lowerBound, upperBound);
};

/**
 * Writes a Date, long millis since epoch, to a buffer starting from offset.
 * @param {Date} date
 * @param {Buffer} buffer
 * @param {Number} offset
 * @return {Number} The new offset.
 * @private
 */
function writeDate(date, buffer, offset) {
  const long = Long.fromNumber(date.getTime());
  buffer.writeUInt32BE(long.getHighBitsUnsigned(), offset);
  buffer.writeUInt32BE(long.getLowBitsUnsigned(), offset + 4);
  return offset + 8;
}

/**
 * Reads a Date, long millis since epoch, from a buffer starting from offset.
 * @param {Buffer} buffer
 * @param {Number} offset
 * @return {Date}
 * @private
 */
function readDate(buffer, offset) {
  const long = new Long(buffer.readInt32BE(offset+4), buffer.readInt32BE(offset));
  return new Date(long.toNumber());
}

/**
 * @classdesc
 * Represents a date range boundary, composed by a <code>Date</code> and a precision.
 * @param {Date} date The timestamp portion, representing a single moment in time. Consider using
 * <code>Date.UTC()</code> method to build the <code>Date</code> instance.
 * @param {Number} precision The precision portion. Valid values for <code>DateRangeBound</code> precision are
 * defined in the [dateRangePrecision]{@link module:datastax/search~dateRangePrecision} member.
 * @constructor
 * @memberOf module:datastax/search
 */
function DateRangeBound(date, precision) {
  /**
   * The timestamp portion of the boundary.
   * @type {Date}
   */
  this.date = date;
  /**
   * The precision portion of the boundary. Valid values are defined in the
   * [dateRangePrecision]{@link module:datastax/search~dateRangePrecision} member.
   * @type {Number}
   */
  this.precision = precision;
}

/**
 * Returns the string representation of the instance.
 * @return {String}
 */
DateRangeBound.prototype.toString = function () {
  if (this.precision === -1) {
    return '*';
  }
  let precision = 0;
  const isoString = this.date.toISOString();
  let i;
  let char;
  // The years take at least the first 4 characters
  for (i = 4; i < isoString.length && precision <= this.precision; i++) {
    char = isoString.charAt(i);
    if (precision === dateRangePrecision.day && char === 'T') {
      precision = dateRangePrecision.hour;
      continue;
    }
    if (precision >= dateRangePrecision.hour && char === ':' || char === '.') {
      precision++;
      continue;
    }
    if (precision < dateRangePrecision.day && char === '-') {
      precision++;
    }
  }
  let start = 0;
  const firstChar = isoString.charAt(0);
  let sign = '';
  let toRemoveIndex = 4;
  if (firstChar === '+' || firstChar === '-') {
    sign = firstChar;
    if (firstChar === '-') {
      // since we are retaining the -, don't remove as many zeros.
      toRemoveIndex = 3;
    }
    // Remove additional zeros
    for (start = 1; start < toRemoveIndex; start++) {
      if (isoString.charAt(start) !== '0') {
        break;
      }
    }
  }
  if (this.precision !== dateRangePrecision.millisecond) {
    // i holds the position of the first char that marks the end of a precision (ie: '-', 'T', ...),
    // we should not include it in the result, except its the 'Z' char for the complete representation
    i--;
  }
  return sign + isoString.substring(start, i);
};

/**
 * Returns true if the value of this DateRange instance and other are the same.
 * @param {DateRangeBound} other
 * @return {boolean}
 */
DateRangeBound.prototype.equals = function (other) {
  if (!(other instanceof DateRangeBound)) {
    return false;
  }
  if (other.precision !== this.precision) {
    return false;
  }
  return datesEqual(other.date, this.date);
};

function datesEqual(d1, d2) {
  const t1 = d1 ? d1.getTime() : null;
  const t2 = d2 ? d2.getTime() : null;
  return t1 === t2;
}

DateRangeBound.prototype.isUnbounded = function () {
  return (this.precision === -1);
};

/**
 * Parses a date string and returns a DateRangeBound.
 * @param {String} boundaryString
 * @return {DateRangeBound}
 */
DateRangeBound.fromString = function(boundaryString) {
  if (!boundaryString) {
    return null;
  }
  if (boundaryString === '*') {
    return unbounded;
  }
  const matches = dateRegex.exec(boundaryString);
  if (!matches) {
    throw TypeError('String provided is not a valid date ' + boundaryString);
  }
  if (matches[7] !== undefined && matches[5] === undefined) {
    // Due to a limitation in the regex, its possible to match dates like 2015T03:02.001, without the seconds
    // portion but with the milliseconds specified.
    throw new TypeError('String representation of the date contains the milliseconds portion but not the seconds: ' +
      boundaryString);
  }
  const builder = new BoundaryBuilder(boundaryString.charAt(0) === '-');
  for (let i = 1; i < matches.length; i++) {
    builder.set(i-1, matches[i], boundaryString);
  }
  return builder.build();
};

/**
 * The unbounded {@link DateRangeBound} instance. Unbounded bounds are syntactically represented by a <code>*</code>
 * (star) sign.
 * @type {DateRangeBound}
 */
DateRangeBound.unbounded = unbounded;

/**
 * Converts a {DateRangeBound} into a lower-bounded bound by rounding down its date
 * based on its precision.
 *
 * @param {DateRangeBound} bound The bound to round down.
 * @returns {DateRangeBound} with the date rounded down to the given precision.
 */
DateRangeBound.toLowerBound = function (bound) {
  if(bound === unbounded) {
    return bound;
  }
  const rounded = new Date(bound.date.getTime());
  // in this case we want to fallthrough
  /* eslint-disable no-fallthrough */
  switch (bound.precision) {
    case dateRangePrecision.year:
      rounded.setUTCMonth(0);
    case dateRangePrecision.month:
      rounded.setUTCDate(1);
    case dateRangePrecision.day:
      rounded.setUTCHours(0);
    case dateRangePrecision.hour:
      rounded.setUTCMinutes(0);
    case dateRangePrecision.minute:
      rounded.setUTCSeconds(0);
    case dateRangePrecision.second:
      rounded.setUTCMilliseconds(0);
  }
  /* eslint-enable no-fallthrough */
  return new DateRangeBound(rounded, bound.precision);
};

/**
 * Converts a {DateRangeBound} into a upper-bounded bound by rounding up its date
 * based on its precision.
 *
 * @param {DateRangeBound} bound The bound to round up.
 * @returns {DateRangeBound} with the date rounded up to the given precision.
 */
DateRangeBound.toUpperBound = function (bound) {
  if (bound === unbounded) {
    return bound;
  }
  const rounded = new Date(bound.date.getTime());
  // in this case we want to fallthrough
  /* eslint-disable no-fallthrough */
  switch (bound.precision) {
    case dateRangePrecision.year:
      rounded.setUTCMonth(11);
    case dateRangePrecision.month:
      // Advance to the beginning of next month and set day of month to 0
      // which sets the date to the last day of the previous month.
      // This gives us the effect of YYYY-MM-LastDayOfThatMonth
      rounded.setUTCMonth(rounded.getUTCMonth() + 1, 0);
    case dateRangePrecision.day:
      rounded.setUTCHours(23);
    case dateRangePrecision.hour:
      rounded.setUTCMinutes(59);
    case dateRangePrecision.minute:
      rounded.setUTCSeconds(59);
    case dateRangePrecision.second:
      rounded.setUTCMilliseconds(999);
  }
  /* eslint-enable no-fallthrough */
  return new DateRangeBound(rounded, bound.precision);
};

/** @private */
function BoundaryBuilder(isNegative) {
  this._sign = isNegative ? -1 : 1;
  this._index = 0;
  this._values = new Int32Array(7);
}

BoundaryBuilder.prototype.set = function (index, value, stringDate) {
  if (value === undefined) {
    return;
  }
  if (index > 6) {
    throw new TypeError('Index out of bounds: ' + index);
  }
  if (index > this._index) {
    this._index = index;
  }
  const numValue = +value;
  switch (index) {
    case dateRangePrecision.month:
      if (numValue < 1 || numValue > 12) {
        throw new TypeError('Month portion is not valid for date: ' + stringDate);
      }
      break;
    case dateRangePrecision.day:
      if (numValue < 1 || numValue > 31) {
        throw new TypeError('Day portion is not valid for date: ' + stringDate);
      }
      break;
    case dateRangePrecision.hour:
      if (numValue > 23) {
        throw new TypeError('Hour portion is not valid for date: ' + stringDate);
      }
      break;
    case dateRangePrecision.minute:
    case dateRangePrecision.second:
      if (numValue > 59) {
        throw new TypeError('Minute/second portion is not valid for date: ' + stringDate);
      }
      break;
    case dateRangePrecision.millisecond:
      if (numValue > 999) {
        throw new TypeError('Millisecond portion is not valid for date: ' + stringDate);
      }
      break;
  }
  this._values[index] = numValue;
};

/** @return {DateRangeBound} */
BoundaryBuilder.prototype.build = function () {
  const date = new Date(0);
  let month = this._values[1];
  if (month) {
    // ES Date months are represented from 0 to 11
    month--;
  }
  date.setUTCFullYear(this._sign * this._values[0], month, this._values[2] || 1);
  date.setUTCHours(this._values[3], this._values[4], this._values[5], this._values[6]);
  return new DateRangeBound(date, this._index);
};

exports.unbounded = unbounded;
exports.dateRangePrecision = dateRangePrecision;
exports.DateRange = DateRange;
exports.DateRangeBound = DateRangeBound;