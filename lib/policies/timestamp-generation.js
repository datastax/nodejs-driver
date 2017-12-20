'use strict';

const util = require('util');
const Long = require('../types').Long;
const errors = require('../errors');

/** @module policies/timestampGeneration */

/**
 * Defines the maximum date in milliseconds that can be represented in microseconds using Number ((2 ^ 53) / 1000)
 * @const
 * @private
 */
const _maxSafeNumberDate = 9007199254740;

/**
 * A long representing the value 1000
 * @const
 * @private
 */
const _longOneThousand = Long.fromInt(1000);

/**
 * Creates a new instance of {@link TimestampGenerator}.
 * @classdesc
 * Generates client-side, microsecond-precision query timestamps.
 * <p>
 *   Given that Cassandra uses those timestamps to resolve conflicts, implementations should generate
 *   monotonically increasing timestamps for successive invocations of {@link TimestampGenerator.next()}.
 * </p>
 * @constructor
 */
function TimestampGenerator() {

}

/**
 * Returns the next timestamp.
 * <p>
 *   Implementors should enforce increasing monotonicity of timestamps, that is,
 *   a timestamp returned should always be strictly greater that any previously returned
 *   timestamp.
 * <p/>
 * <p>
 *   Implementors should strive to achieve microsecond precision in the best possible way,
 *   which is usually largely dependent on the underlying operating system's capabilities.
 * </p>
 * @param {Client} client The {@link Client} instance to generate timestamps to.
 * @returns {Long|Number|null} the next timestamp (in microseconds). If it's equals to <code>null</code>, it won't be
 * sent by the driver, letting the server to generate the timestamp.
 * @abstract
 */
TimestampGenerator.prototype.next = function (client) {
  throw new Error('next() must be implemented');
};

/**
 * A timestamp generator that guarantees monotonically increasing timestamps and logs warnings when timestamps
 * drift in the future.
 * <p>
 *   {@link Date} has millisecond precision and client timestamps require microsecond precision. This generator
 *   keeps track of the last generated timestamp, and if the current time is within the same millisecond as the last,
 *   it fills the microsecond portion of the new timestamp with the value of an incrementing counter.
 * </p>
 * @param {Number} [warningThreshold] Determines how far in the future timestamps are allowed to drift before a
 * warning is logged, expressed in milliseconds. Default: <code>1000</code>.
 * @param {Number} [minLogInterval] In case of multiple log events, it determines the time separation between log
 * events, expressed in milliseconds. Use 0 to disable. Default: <code>1000</code>.
 * @extends {TimestampGenerator}
 * @constructor
 */
function MonotonicTimestampGenerator(warningThreshold, minLogInterval) {
  if (warningThreshold < 0) {
    throw new errors.ArgumentError('warningThreshold can not be lower than 0');
  }
  this._warningThreshold = warningThreshold || 1000;
  this._minLogInterval = 1000;
  if (typeof minLogInterval === 'number') {
    // A value under 1 will disable logging
    this._minLogInterval = minLogInterval;
  }
  this._micros = -1;
  this._lastDate = 0;
  this._lastLogDate = 0;
}

util.inherits(MonotonicTimestampGenerator, TimestampGenerator);

/**
 * Returns the current time in milliseconds since UNIX epoch
 * @returns {Number}
 */
MonotonicTimestampGenerator.prototype.getDate = function () {
  return Date.now();
};

MonotonicTimestampGenerator.prototype.next = function (client) {
  let date = this.getDate();
  let drifted = 0;
  if (date > this._lastDate) {
    this._micros = 0;
    this._lastDate = date;
    return this._generateMicroseconds();
  }

  if (date < this._lastDate) {
    drifted = this._lastDate - date;
    date = this._lastDate;
  }
  if (++this._micros === 1000) {
    this._micros = 0;
    if (date === this._lastDate) {
      // Move date 1 millisecond into the future
      date++;
      drifted++;
    }
  }
  const lastDate = this._lastDate;
  this._lastDate = date;
  const result = this._generateMicroseconds();
  if (drifted >= this._warningThreshold) {
    // Avoid logging an unbounded amount of times within a clock-skew event or during an interval when more than 1
    // query is being issued by microsecond
    const currentLogDate = Date.now();
    if (this._minLogInterval > 0 && this._lastLogDate + this._minLogInterval <= currentLogDate){
      const message = util.format(
        'Timestamp generated using current date was %d milliseconds behind the last generated timestamp (which ' +
        'millisecond portion was %d), the returned value (%s) is being artificially incremented to guarantee ' +
        'monotonicity.',
        drifted, lastDate, result);
      this._lastLogDate = currentLogDate;
      client.log('warning', message);
    }
  }
  return result;
};

/**
 * @private
 * @returns {Number|Long}
 */
MonotonicTimestampGenerator.prototype._generateMicroseconds = function () {
  if (this._lastDate < _maxSafeNumberDate) {
    // We are safe until Jun 06 2255, its faster to perform this operations on Number than on Long
    // We hope to have native int64 by then :)
    return this._lastDate * 1000 + this._micros;
  }
  return Long
    .fromNumber(this._lastDate)
    .multiply(_longOneThousand)
    .add(Long.fromInt(this._micros));
};

exports.TimestampGenerator = TimestampGenerator;
exports.MonotonicTimestampGenerator = MonotonicTimestampGenerator;