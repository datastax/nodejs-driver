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
/** @module types */

/**
 * @const
 * @private
 * */
const maxNanos = Long.fromString('86399999999999');
/**
 * Nanoseconds in a second
 * @const
 * @private
 * */
const nanoSecInSec = Long.fromNumber(1000000000);
/**
 * Nanoseconds in a millisecond
 * @const
 * @private
 * */
const nanoSecInMillis = Long.fromNumber(1000000);
/**
 * Milliseconds in day
 * @const
 * @private
 * */
const millisInDay = 86400000;
/**
 *
 * Creates a new instance of LocalTime.
 * @class
 * @classdesc A time without a time-zone in the ISO-8601 calendar system, such as 10:30:05.
 * <p>
 *   LocalTime is an immutable date-time object that represents a time, often viewed as hour-minute-second. Time is represented to nanosecond precision. For example, the value "13:45.30.123456789" can be stored in a LocalTime.
 * </p>
 * @param {Long} totalNanoseconds Total nanoseconds since midnight.
 * @constructor
 */
function LocalTime(totalNanoseconds) {
  if (!(totalNanoseconds instanceof Long)) {
    throw new Error('You must specify a Long value as totalNanoseconds');
  }
  if (totalNanoseconds.lessThan(Long.ZERO) || totalNanoseconds.greaterThan(maxNanos)) {
    throw new Error('Total nanoseconds out of range');
  }
  this.value = totalNanoseconds;
  
  /**
   * Gets the hour component of the time represented by the current instance, a number from 0 to 23.
   * @type Number
   */
  this.hour = this._getParts()[0];
  /**
   * Gets the minute component of the time represented by the current instance, a number from 0 to 59.
   * @type Number
   */
  this.minute = this._getParts()[1];
  /**
   * Gets the second component of the time represented by the current instance, a number from 0 to 59.
   * @type Number
   */
  this.second = this._getParts()[2];
  /**
   * Gets the nanoseconds component of the time represented by the current instance, a number from 0 to 999999999.
   * @type Number
   */
  this.nanosecond = this._getParts()[3];
}

/**
 * Parses an string representation and returns a new LocalDate.
 * @param {String} value
 * @returns {LocalTime}
 */
LocalTime.fromString = function (value) {
  if (typeof value !== 'string') {
    throw new Error('Argument type invalid: ' + util.inspect(value));
  }
  const parts = value.split(':');
  let millis = parseInt(parts[0], 10) * 3600000 + parseInt(parts[1], 10) * 60000;
  let nanos;
  if (parts.length === 3) {
    const secParts = parts[2].split('.');
    millis += parseInt(secParts[0], 10) * 1000;
    if (secParts.length === 2) {
      nanos = secParts[1];
      //add zeros at the end
      nanos = nanos + utils.stringRepeat('0', 9 - nanos.length);
    }
  }
  return LocalTime.fromMilliseconds(millis, parseInt(nanos, 10) || 0);
};

/**
 * Uses the current local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
 * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the time nanosecond portion.
 * @returns {LocalTime}
 */
LocalTime.now = function (nanoseconds) {
  return LocalTime.fromDate(new Date(), nanoseconds);
};

/**
 * Uses the provided local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
 * @param {Date} date Local date portion to extract the time passed since midnight.
 * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the nanosecond time portion.
 * @returns {LocalTime}
 */
LocalTime.fromDate = function (date, nanoseconds) {
  if (!(date instanceof Date)) {
    throw new Error('Not a valid date');
  }
  //Use the local representation, only the milliseconds portion
  const millis = (date.getTime() + date.getTimezoneOffset() * -60000) % millisInDay;
  return LocalTime.fromMilliseconds(millis, nanoseconds);
};

/**
 * Uses the provided local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
 * @param {Number} milliseconds A Number from 0 to 86,399,999.
 * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the time nanosecond portion.
 * @returns {LocalTime}
 */
LocalTime.fromMilliseconds = function (milliseconds, nanoseconds) {
  if (typeof nanoseconds !== 'number') {
    nanoseconds = 0;
  }
  return new LocalTime(Long
    .fromNumber(milliseconds)
    .multiply(nanoSecInMillis)
    .add(Long.fromNumber(nanoseconds)));
};

/**
 * Creates a new instance of LocalTime from the bytes representation.
 * @param {Buffer} value
 * @returns {LocalTime}
 */
LocalTime.fromBuffer = function (value) {
  if (!(value instanceof Buffer)) {
    throw new TypeError('Expected Buffer, obtained ' + util.inspect(value));
  }
  return new LocalTime(new Long(value.readInt32BE(4), value.readInt32BE(0)));
};

/**
 * Compares this LocalTime with the given one.
 * @param {LocalTime} other time to compare against.
 * @return {number} 0 if they are the same, 1 if the this is greater, and -1
 * if the given one is greater.
 */
LocalTime.prototype.compare = function (other) {
  return this.value.compare(other.value);
};

/**
 * Returns true if the value of the LocalTime instance and other are the same
 * @param {LocalTime} other
 * @returns {Boolean}
 */
LocalTime.prototype.equals = function (other) {
  return ((other instanceof LocalTime)) && this.compare(other) === 0;
};

/**
 * Gets the total amount of nanoseconds since midnight for this instance.
 * @returns {Long}
 */
LocalTime.prototype.getTotalNanoseconds = function () {
  return this.value;
};

LocalTime.prototype.inspect = function () {
  return this.constructor.name + ': ' + this.toString();
};

/**
 * Returns a big-endian bytes representation of the instance
 * @returns {Buffer}
 */
LocalTime.prototype.toBuffer = function () {
  const buffer = utils.allocBufferUnsafe(8);
  buffer.writeUInt32BE(this.value.getHighBitsUnsigned(), 0);
  buffer.writeUInt32BE(this.value.getLowBitsUnsigned(), 4);
  return buffer;
};

/**
 * Returns the string representation of the instance in the form of hh:MM:ss.ns
 * @returns {String}
 */
LocalTime.prototype.toString = function () {
  return formatTime(this._getParts());
};

/**
 * Gets the string representation of the instance in the form: hh:MM:ss.ns
 * @returns {String}
 */
LocalTime.prototype.toJSON = function () {
  return this.toString();
};

/**
 * @returns {Array.<Number>}
 * @ignore
 */
LocalTime.prototype._getParts = function () {
  if (!this._partsCache) {
    //hours, minutes, seconds and nanos
    const parts = [0, 0, 0, 0];
    const secs = this.value.div(nanoSecInSec);
    //faster modulo
    //total nanos
    parts[3] = this.value.subtract(secs.multiply(nanoSecInSec)).toNumber();
    //seconds
    parts[2] = secs.toNumber();
    if (parts[2] >= 60) {
      //minutes
      parts[1] = Math.floor(parts[2] / 60);
      parts[2] = parts[2] % 60;
    }
    if (parts[1] >= 60) {
      //hours
      parts[0] = Math.floor(parts[1] / 60);
      parts[1] = parts[1] % 60;
    }
    this._partsCache = parts;
  }
  return this._partsCache;
};

/**
 * @param {Array.<Number>} values
 * @private
 */
function formatTime(values) {
  let result;
  if (values[0] < 10) {
    result = '0' + values[0] + ':';
  }
  else {
    result = values[0] + ':';
  }
  if (values[1] < 10) {
    result += '0' + values[1] + ':';
  }
  else {
    result += values[1] + ':';
  }
  if (values[2] < 10) {
    result += '0' + values[2];
  }
  else {
    result += values[2];
  }
  if (values[3] > 0) {
    let nanos = values[3].toString();
    //nine digits
    if (nanos.length < 9) {
      nanos = utils.stringRepeat('0', 9 - nanos.length) + nanos;
    }
    let lastPosition;
    for (let i = nanos.length - 1; i > 0; i--) {
      if (nanos[i] !== '0') {
        break;
      }
      lastPosition = i;
    }
    if (lastPosition) {
      nanos = nanos.substring(0, lastPosition);
    }
    result += '.' + nanos;
  }
  return result;
}

module.exports = LocalTime;