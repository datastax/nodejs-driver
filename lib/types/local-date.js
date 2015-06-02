"use strict";
var utils = require('../utils');
/** @module types */

/**
 * @private
 * @const
 */
var millisecondsPerDay = 86400000;
/**
 * @private
 */
var dateCenter = Math.pow(2,31);
/**
 *
 * Creates a new instance of LocalDate.
 * @class
 * @classdesc A date without a time-zone in the ISO-8601 calendar system, such as 2010-08-05.
 * <p>
 *   LocalDate is an immutable object that represents a date, often viewed as year-month-day. For example, the value "1st October 2014" can be stored in a LocalDate.
 * </p>
 * <p>
 *   This class does not store or represent a time or time-zone. Instead, it is a description of the date, as used for birthdays. It cannot represent an instant on the time-line without additional information such as an offset or time-zone.
 * </p>
 * <p>
 *   Note that this type can represent dates in the range [-5877641-06-23; 5881580-07-17] while the ES5 date type can only represent values in the range of [-271821-04-20; 275760-09-13].
 *   In the event that year, month, day parameters do not fall within the ES5 date range an Error will be thrown.  If you wish to represent a date outside of this range, pass a single
 *   parameter indicating the days since epoch.  For example, -1 represents 1969-12-31.
 * </p>
 * @param {Number} year The year or days since epoch.  If days since epoch, month and day should not be provided.
 * @param {Number} month Between 1 and 12 inclusive.
 * @param {Number} day Between 1 and the number of days in the given month of the given year.
 *
 * @property {Date} date The date representation if falls within a range of an ES5 data type, otherwise an invalid date.
 *
 * @constructor
 */
function LocalDate(year, month, day) {
  //implementation detail: internally uses a UTC based date
  if (typeof year === 'number' && typeof month === 'number' && typeof day === 'number') {
    //Use setUTCFullYear as if there is a 2 digit year, Date.UTC() assumes
    //that is the 20th century.  Thanks ECMAScript!
    //noinspection JSValidateTypes
    this.date = new Date();
    this.date.setUTCHours(0, 0, 0, 0);
    this.date.setUTCFullYear(year, month-1, day);
    if(isNaN(this.date.getTime())) {
      throw new Error(util.format('%d-%d-%d does not form a valid ES5 date!',
        year, month, day));
    }
  }
  else if (typeof month === 'undefined' && typeof day === 'undefined') {
    if (typeof year === 'number') {
      //in days since epoch.
      if(year < -2147483648 || year > 2147483647) {
        throw new Error('You must provide a valid value for days since epoch (-2147483648 <= value <= 2147483647).')
      }
      //noinspection JSValidateTypes
      this.date = new Date(year * millisecondsPerDay);
    }
  }

  if (typeof this.date === 'undefined') {
    throw new Error('You must provide a valid year, month and day');
  }

  //If date cannot be represented yet given a valid days since epoch, track
  //it internally.
  var value = isNaN(this.date.getTime()) ? year : null;
  Object.defineProperty(this, '_value', { enumerable: false, value: value });

  var self = this;

  /**
   * A number representing the year.  May return NaN if cannot be represented as
   * a Date.
   * @name year
   * @type Number
   * @memberof module:types~LocalDate#
   */
  /**
   * A number between 1 and 12 inclusive representing the month.  May return
   * NaN if cannot be represented as a Date.
   * @name month
   * @type Number
   * @memberof module:types~LocalDate#
   */
  /**
   * A number between 1 and the number of days in the given month of the given year (28, 29, 30, 31).
   * May return NaN if cannot be represented as a Date.
   * @name day
   * @type Number
   * @memberof module:types~LocalDate#
   */
  Object.defineProperties(this, {
    'year': { enumerable: true, get: function () {
      return self.date.getUTCFullYear();
    }},
    'month': { enumerable: true, get: function () {
      return self.date.getUTCMonth() + 1;
    }},
    'day': { enumerable: true, get: function () {
      return self.date.getUTCDate();
    }}
  });
}

/**
 * Creates a new instance of LocalDate using the current year, month and day from the system clock in the default time-zone.
 */
LocalDate.now = function () {
  //noinspection JSCheckFunctionSignatures
  return LocalDate.fromDate(new Date());
};

/**
 * Creates a new instance of LocalDate using the current date from the system clock at UTC.
 */
LocalDate.utcNow = function () {
  //noinspection JSCheckFunctionSignatures
  return new LocalDate(Date.now());
};


/**
 * Creates a new instance of LocalDate using the year, month and day from the provided local date time.
 * @param {Date} date
 */
LocalDate.fromDate = function (date) {
  if (isNaN(date.getTime())) {
    throw new TypeError('Invalid date: ' + date);
  }
  return new LocalDate(date.getFullYear(), date.getMonth() + 1, date.getDate());
};

/**
 * Creates a new instance of LocalDate using the year, month and day provided in the form: yyyy-mm-dd or
 * days since epoch (i.e. -1 for Dec 31, 1969).
 * @param {String} value
 */
LocalDate.fromString = function (value) {
  var dashCount = (value.match(/-/g) || []).length;
  if(dashCount >= 2) {
    var multiplier = 1;
    if (value[0] === '-') {
      value = value.substring(1);
      multiplier = -1;
    }
    var parts = value.split('-');
    return new LocalDate(multiplier * parseInt(parts[0], 10), parseInt(parts[1], 10), parseInt(parts[2], 10));
  } else if(value.match(/^-?\d+$/)) {
    // Parse as days since epoch.
    //noinspection JSCheckFunctionSignatures
    return new LocalDate(parseInt(value, 10));
  } else {
    throw new Error("Invalid input '" + value + "'.");
  }
};

/**
 * Creates a new instance of LocalDate using the bytes representation.
 * @param {Buffer} buffer
 */
LocalDate.fromBuffer = function (buffer) {
  //move to unix epoch: 0.
  //noinspection JSCheckFunctionSignatures
  return new LocalDate((buffer.readUInt32BE(0) - dateCenter));
};

/**
 * Compares this LocalDate with the given one.
 * @param {LocalDate} other date to compare against.
 * @return {number} 0 if they are the same, 1 if the this is greater, and -1
 * if the given one is greater.
 */
LocalDate.prototype.compare = function (other) {
  var thisValue = isNaN(this.date.getTime()) ? this._value * millisecondsPerDay : this.date.getTime();
  var otherValue = isNaN(other.date.getTime()) ? other._value * millisecondsPerDay : other.date.getTime();
  var diff = thisValue - otherValue;
  if (diff < 0) {
    return -1;
  }
  if (diff > 0) {
    return 1;
  }
  return 0;
};

/**
 * Returns true if the value of the LocalDate instance and other are the same
 * @param {LocalDate} other
 * @returns {Boolean}
 */
LocalDate.prototype.equals = function (other) {
  return ((other instanceof LocalDate)) && this.compare(other) === 0;
};

LocalDate.prototype.inspect = function () {
  return this.constructor.name + ': ' + this.toString();
};

/**
 * Gets the bytes representation of the instance.
 * @returns {Buffer}
 */
LocalDate.prototype.toBuffer = function () {
  //days since unix epoch
  var daysSinceEpoch = isNaN(this.date.getTime()) ? this._value : Math.floor(this.date.getTime() / millisecondsPerDay);
  var value = daysSinceEpoch + dateCenter;
  var buf = new Buffer(4);
  buf.writeUInt32BE(value, 0);
  return buf;
};

/**
 * Gets the string representation of the instance in the form: yyyy-mm-dd if
 * the value can be parsed as a Date, otherwise days since epoch.
 * @returns {String}
 */
LocalDate.prototype.toString = function () {
  var result;
  //if cannot be parsed as date, return days since epoch representation.
  if(isNaN(this.date.getTime())) {
    return this._value.toString();
  }
  else {
    var year = this.date.getUTCFullYear();
    var month = this.date.getUTCMonth() + 1;
    var day = this.date.getUTCDate();
    if (year < 0) {
      result = '-' + fillZeros((year * -1).toString(), 4);
    }
    else {
      result = fillZeros(year.toString(), 4);
    }
    result += '-' + fillZeros(month.toString(), 2) + '-' + fillZeros(day.toString(), 2);
    return result;
  }
};

/**
 * Gets the string representation of the instance in the form: yyyy-mm-dd, valid for JSON.
 * @returns {String}
 */
LocalDate.prototype.toJSON = function () {
  return this.toString();
};

/**
 * @param {String} value
 * @param {Number} amount
 * @private
 */
function fillZeros(value, amount) {
  if (value.length >= amount) {
    return value;
  }
  return utils.stringRepeat('0', amount - value.length) + value;
}

module.exports = LocalDate;