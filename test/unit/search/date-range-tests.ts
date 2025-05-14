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
import assert from "assert";
import util from "util";
import helper from "../../test-helper";
import * as dateRangeModule from "../../../lib/datastax/search/date-range";


const DateRange = dateRangeModule.DateRange;
const DateRangeBound = dateRangeModule.DateRangeBound;
const unbounded = dateRangeModule.unbounded;
const precision = dateRangeModule.dateRangePrecision;

describe('DateRange', function () {
  const values = [
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
        const actual = DateRange.fromString(item[0]);
        helper.assertInstanceOf(actual, DateRange);
        assert.ok(actual.equals(item[1]),
          util.format('Parsed value "%s" not equals to expected: %j, got: %j', item[0], item[1], actual));
      });
    });
    it('should throw when the string is not a valid date', function () {
      const invalidValues = [
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
        const expected = item[2] || item[0];
        assert.strictEqual(item[1].toString(), expected);
      });
    });
  });
  describe('#toBuffer() and fromBuffer()', function () {
    it('should serialize and deserialize the values', function () {
      values.forEach(function (item) {
        const dateRange = item[1];
        const serialized = dateRange.toBuffer();
        helper.assertInstanceOf(serialized, Buffer);
        const deserialized = DateRange.fromBuffer(serialized);
        helper.assertInstanceOf(deserialized, DateRange);
        assert.ok(deserialized.equals(dateRange),
          util.format('Serialization or deserialization failed for %j', dateRange));
      });
    });
  });
});
describe('DateRangeBound', function () {
  const date = getUtcDate(2017, 1, 20, 6, 54, 1, 578);
  const bcDate = getUtcDate(-2001, 11, 20, 16, 5, 1, 999);
  const values = [
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
  const month = arguments[1] || 1;
  const date = new Date(0);
  date.setUTCFullYear(arguments[0], month - 1, arguments[2] || 1);
  date.setUTCHours(arguments[3] || 0, arguments[4] || 0, arguments[5] || 0, arguments[6] || 0);
  return date;
}

/**
 * @param {Date} date1
 * @param {Number} precision1
 * @param {Date} [date2]
 * @param {Number} [precision2]
 * @return {module:datastax/search~DateRange}
 */
function getDateRange(date1, precision1, date2, precision2) {
  const lowerBound = date1 ? new DateRangeBound(date1, precision1) : DateRangeBound.unbounded;
  let upperBound = null;
  if (date2) {
    upperBound = new DateRangeBound(date2, precision2);
  }
  return new DateRange(lowerBound, upperBound);
}