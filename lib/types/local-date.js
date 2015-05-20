"use strict";
var utils = require('../utils');
/** @module types */

/** @const */
var millisecondsPerDay = 86400000;
/** @const */
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
 * @param {Number} year The year.
 * @param {Number} month Between 1 and 12 inclusive.
 * @param {Number} day Between 1 and the number of days in the given month of the given year.
 * @constructor
 */
function LocalDate(year, month, day) {
  //implementation detail: internally uses a UTC based date
  if (typeof year === 'number' && typeof month === 'number' && typeof day === 'number') {
    this.value = new Date(Date.UTC(year, month - 1, day));
  }
  else if (typeof month === 'undefined' && typeof day === 'undefined') {
    if (typeof year === 'number') {
      //in milliseconds since unix epoch
      this.value = new Date(year);
    }
  }
  if (!this.value) {
    throw new Error('You must provide a valid year, month and day');
  }
  var self = this;
  /**
   * A number representing the year.
   * @name year
   * @type Number
   * @memberof module:types~LocalDate#
   */
  /**
   * A number between 1 and 12 inclusive representing the month.
   * @name month
   * @type Number
   * @memberof module:types~LocalDate#
   */
  /**
   * A number between 1 and the number of days in the given month of the given year (28, 29, 30, 31).
   * @name day
   * @type Number
   * @memberof module:types~LocalDate#
   */
  Object.defineProperties(this, {
    'year': { enumerable: true, get: function () {
      return self.value.getUTCFullYear();
    }},
    'month': { enumerable: true, get: function () {
      return self.value.getMonth() + 1;
    }},
    'day': { enumerable: true, get: function () {
      return self.value.getDate();
    }}
  });
}

/**
 * Creates a new instance of LocalDate using the current year, month and day from the system clock in the default time-zone.
 */
LocalDate.now = function () {
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
  if (isNaN(date)) {
    throw new TypeError('Invalid date: ' + date);
  }
  return new LocalDate(date.getFullYear(), date.getMonth() + 1, date.getDate());
};

/**
 * Creates a new instance of LocalDate using the year, month and day provided in the form: yyyy-mm-dd.
 * @param {String} value
 */
LocalDate.fromString = function (value) {
  var multiplier = 1;
  if (value[0] === '-') {
    value = value.substring(1);
    multiplier = -1;
  }
  var parts = value.split('-');
  return new LocalDate(multiplier * parseInt(parts[0], 10), parseInt(parts[1], 10), parseInt(parts[2], 10));
};

/**
 * Creates a new instance of LocalDate using the bytes representation.
 * @param {Buffer} buffer
 */
LocalDate.fromBuffer = function (buffer) {
  //move to unix epoch: 0 and express it in milliseconds
  //noinspection JSCheckFunctionSignatures
  return new LocalDate((buffer.readUInt32BE(0) - dateCenter) * millisecondsPerDay);
};

/**
 * Compares this LocalDate with the given one.
 * @param {LocalDate} other date to compare against.
 * @return {number} 0 if they are the same, 1 if the this is greater, and -1
 * if the given one is greater.
 */
LocalDate.prototype.compare = function (other) {
  var diff = this.value.getTime() - other.value.getTime();
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
  var value = Math.floor(this.value.getTime() / millisecondsPerDay) + dateCenter;
  var buf = new Buffer(4);
  buf.writeUInt32BE(value, 0);
  return buf;
};

/**
 * Gets the string representation of the instance in the form: yyyy-mm-dd
 * @returns {String}
 */
LocalDate.prototype.toString = function () {
  var result;
  if (this.year < 0) {
    result = '-' + fillZeros((this.year * -1).toString(), 4);
  }
  else {
    result = fillZeros(this.year.toString(), 4);
  }
  result += '-' + fillZeros(this.month.toString(), 2) + '-' + fillZeros(this.day.toString(), 2);
  return result;
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