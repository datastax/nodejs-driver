/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

var assert = require('assert');
var util = require('util');
var helper = require('../../test-helper');
var dateRangeModule = require('../../../lib/search/date-range');
var DateRange = dateRangeModule.DateRange;
var DateRangeBound = dateRangeModule.DateRangeBound;
var unbounded = dateRangeModule.unbounded;
var precision = dateRangeModule.dateRangePrecision;

describe('DateRange', function () {
  var values = [
    [ '[2010 TO 2011-12]', getDateRange(getUtcDate(2010), precision.year, getUtcDate(2011, 12, 31, 23, 59, 59, 999), precision.month) ],
    [ '[* TO 2011-8]', getDateRange(null, null, getUtcDate(2011, 8, 31, 23, 59, 59, 999), precision.month), '[* TO 2011-08]' ],
    [ '[2015-01 TO *]', new DateRange(
      new DateRangeBound(getUtcDate(2015), precision.month), DateRangeBound.unbounded) ],
    [ '[2017-01 TO 2017-02]', getDateRange(getUtcDate(2017, 1), precision.month, getUtcDate(2017, 2, 28, 23, 59, 59, 999), precision.month), '[2017-01 TO 2017-02]' ],
    // leap year
    [ '[2016-01 TO 2016-02]', getDateRange(getUtcDate(2016, 1), precision.month, getUtcDate(2016, 2, 29, 23, 59, 59, 999), precision.month), '[2016-01 TO 2016-02]' ],
    [ '2012-1-2', getDateRange(getUtcDate(2012, 1, 2), precision.day), '2012-01-02'],
    [ '2012-1-2T', getDateRange(getUtcDate(2012, 1, 2), precision.day), '2012-01-02'],
    [ '1-2-3T23:5:7', getDateRange(getUtcDate(1, 2, 3, 23, 5, 7), precision.second), '0001-02-03T23:05:07'],
    [ '2015-01T03', getDateRange(getUtcDate(2015, 1, 1, 3), precision.hour), '2015-01-01T03'],
    [ '2015-04T03:02', getDateRange(getUtcDate(2015, 4, 1, 3, 2), precision.minute), '2015-04-01T03:02'],
    [ '2015-04T03:02:01.081', getDateRange(getUtcDate(2015, 4, 1, 3, 2, 1, 81), precision.millisecond),
      '2015-04-01T03:02:01.081Z'],
    [ '*', new DateRange(unbounded)],
    [ '[* TO *]', new DateRange(unbounded, unbounded) ],
    [ '0001-01-01', new DateRange(new DateRangeBound(getUtcDate(1, 1, 1), precision.day))],
    [ '-0001-01-01', new DateRange(new DateRangeBound(getUtcDate(-1, 1, 1), precision.day))],
    [ '-0009', new DateRange(new DateRangeBound(getUtcDate(-9, 1, 1), precision.year))],
    [ '0000', new DateRange(new DateRangeBound(getUtcDate(0, 1, 1), precision.year))]
  ];
  describe('fromString()', function () {
    it('should parse valid values', function () {
      values.forEach(function (item) {
        var actual = DateRange.fromString(item[0]);
        helper.assertInstanceOf(actual, DateRange);
        assert.ok(actual.equals(item[1]),
          util.format('Parsed value "%s" not equals to expected: %j, got: %j', item[0], item[1], actual));
      });
    });
    it('should throw when the string is not a valid date', function () {
      var invalidValues = [
        '2015-01T03:02.001',
        '2012-1-2T12:',
        '2015-01T03.001',
        '2015-01 TO',
        ' TO 2015-01',
        'TO 2015-01',
        '2015-01T03:04.001'
      ];
      invalidValues.forEach(function (stringDate) {
        assert.throws(function () {
          DateRange.fromString(stringDate, null);
        }, TypeError);
      });
    });
  });
  describe('#toString()', function () {
    it('should return the string representation', function () {
      values.forEach(function (item) {
        var expected = item[2] || item[0];
        assert.strictEqual(item[1].toString(), expected);
      });
    });
  });
  describe('#toBuffer() and fromBuffer()', function () {
    it('should serialize and deserialize the values', function () {
      values.forEach(function (item) {
        var dateRange = item[1];
        var serialized = dateRange.toBuffer();
        helper.assertInstanceOf(serialized, Buffer);
        var deserialized = DateRange.fromBuffer(serialized);
        helper.assertInstanceOf(deserialized, DateRange);
        assert.ok(deserialized.equals(dateRange),
          util.format('Serialization or deserialization failed for %j', dateRange));
      });
    });
  });
});
describe('DateRangeBound', function () {
  var date = getUtcDate(2017, 1, 20, 6, 54, 1, 578);
  var bcDate = getUtcDate(-2001, 11, 20, 16, 5, 1, 999);
  var values = [
    [ new DateRangeBound(date, precision.year), '2017' ],
    [ new DateRangeBound(date, precision.month), '2017-01' ],
    [ new DateRangeBound(date, precision.day), '2017-01-20' ],
    [ new DateRangeBound(date, precision.hour), '2017-01-20T06' ],
    [ new DateRangeBound(date, precision.minute), '2017-01-20T06:54' ],
    [ new DateRangeBound(date, precision.second), '2017-01-20T06:54:01' ],
    [ new DateRangeBound(date, precision.millisecond), '2017-01-20T06:54:01.578Z' ],
    [ new DateRangeBound(getUtcDate(142017, 1, 20, 6, 54, 1, 570), precision.millisecond),
      '+142017-01-20T06:54:01.570Z' ],
    [ new DateRangeBound(getUtcDate(12017, 3, 5, 6, 54, 1, 570), precision.millisecond),
      '+12017-03-05T06:54:01.570Z' ],
    [ new DateRangeBound(bcDate, precision.millisecond), '-2001-11-20T16:05:01.999Z' ],
  ];
  describe('#toString()', function () {
    it('should retrieve the iso string date up to the provided precision', function () {
      values.forEach(function (item) {
        assert.strictEqual(item[0].toString(), item[1]);
      });
    });
  });
});

/** @return {Date} */
function getUtcDate() {
  var month = arguments[1] || 1;
  var date = new Date(0);
  date.setUTCFullYear(arguments[0], month - 1, arguments[2] || 1);
  date.setUTCHours(arguments[3] || 0, arguments[4] || 0, arguments[5] || 0, arguments[6] || 0);
  return date;
}

/**
 * @param {Date} date1
 * @param {Number} precision1
 * @param {Date} [date2]
 * @param {Number} [precision2]
 * @return {module:search.DateRange}
 */
function getDateRange(date1, precision1, date2, precision2) {
  var lowerBound = date1 ? new DateRangeBound(date1, precision1) : DateRangeBound.unbounded;
  var upperBound = null;
  if (date2) {
    upperBound = new DateRangeBound(date2, precision2);
  }
  return new DateRange(lowerBound, upperBound);
}