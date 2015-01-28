var Integer = require('./integer');
var utils = require('../utils');
/** @module types */
/**
 * Constructs an immutable arbitrary-precision signed decimal number.
 * A {@code BigDecimal} consists of an [arbitrary precision integer]{@link Integer}
 * <i>unscaled value</i> and a 32-bit integer <i>scale</i>.  If zero
 * or positive, the scale is the number of digits to the right of the
 * decimal point.  If negative, the unscaled value of the number is
 * multiplied by ten to the power of the negation of the scale.  The
 * value of the number represented by the {@code BigDecimal} is
 * therefore <tt>(unscaledValue &times; 10<sup>-scale</sup>)</tt>.
 *
 * <p>The {@code BigDecimal} class provides operations for
 * arithmetic, scale manipulation, rounding, comparison and
 * format conversion.  The {@link #toString} method provides a
 * canonical representation of a {@code BigDecimal}
 * @param {Integer} unscaledValue
 * @param {Number} scale
 * @constructor
 */
function BigDecimal(unscaledValue, scale) {
  /**
   * @type {Integer}
   * @private
   */
  this._intVal = unscaledValue;
  /**
   * @type {Number}
   * @private
   */
  this._scale = scale;
}

/**
 * Returns the BigDecimal representation of a buffer composed of the scale (int32BE) and the unsigned value (varint BE)
 * @param {Buffer} buf
 * @returns {BigDecimal}
 */
BigDecimal.fromBuffer = function (buf) {
  var scale = buf.readInt32BE(0);
  var unscaledValue = Integer.fromBuffer(buf.slice(4));
  return new BigDecimal(unscaledValue, scale);
};

/**
 * Returns a buffer representation composed of the scale as a BE int 32 and the unsigned value as a BE varint
 * @param {BigDecimal} value
 * @returns {Buffer}
 */
BigDecimal.toBuffer = function (value) {
  var unscaledValueBuffer = Integer.toBuffer(value._intVal);
  var scaleBuffer = new Buffer(4);
  scaleBuffer.writeInt32BE(value._scale, 0);
  return Buffer.concat([scaleBuffer, unscaledValueBuffer], scaleBuffer.length + unscaledValueBuffer.length);
};

/**
 * Returns a BigDecimal representation of the string
 * @param {String} value
 * @returns {BigDecimal}
 */
BigDecimal.fromString = function (value) {
  if (!value) {
    throw new Error('Invalid null or undefined value');
  }
  value = value.trim();
  var scaleIndex = value.indexOf('.');
  var scale = 0;
  if (scaleIndex >= 0) {
    scale = value.length - 1 - scaleIndex;
    value = value.substr(0, scaleIndex) + value.substr(scaleIndex + 1);
  }
  return new BigDecimal(Integer.fromString(value), scale);
};

/**
 * Returns true if the value of the BigDecimal instance and other are the same
 * @param {BigDecimal} other
 * @returns {Boolean}
 */
BigDecimal.prototype.equals = function (other) {
  return ((other instanceof BigDecimal) && this.compare(other) === 0);
};

BigDecimal.prototype.inspect = function () {
  return this.constructor.name + ': ' + this.toString();
};

/**
 * @param {BigDecimal} other
 * @returns {boolean}
 */
BigDecimal.prototype.notEquals = function (other) {
  return !this.equals(other);
};

/**
 * Compares this BigDecimal with the given one.
 * @param {BigDecimal} other Integer to compare against.
 * @return {number} 0 if they are the same, 1 if the this is greater, and -1
 *     if the given one is greater.
 */
BigDecimal.prototype.compare = function (other) {
  var diff = this.subtract(other);
  if (diff.isNegative()) {
    return -1;
  } else if (diff.isZero()) {
    return 0;
  } else {
    return +1;
  }
};

/**
 * Returns the difference of this and the given BigDecimal.
 * @param {BigDecimal} other The BigDecimal to subtract from this.
 * @return {!BigDecimal} The BigDecimal result.
 */
BigDecimal.prototype.subtract = function (other) {
  var first = this;
  if (first._scale === other._scale) {
    return new BigDecimal(first._intVal.subtract(other._intVal), first._scale);
  }
  var diffScale;
  var unscaledValue;
  if (first._scale < other._scale) {
    //The scale of this is lower
    diffScale = other._scale - first._scale;
    //multiple this unScaledValue to compare in the same scale
    unscaledValue = first._intVal
      .multiply(Integer.fromNumber(Math.pow(10, diffScale)))
      .subtract(other._intVal);
    return new BigDecimal(unscaledValue, other._scale);
  }
  else {
    //The scale of this is higher
    diffScale = first._scale - other._scale;
    //multiple this unScaledValue to compare in the same scale
    unscaledValue = first._intVal
      .subtract(
        other._intVal.multiply(Integer.fromNumber(Math.pow(10, diffScale))));
    return new BigDecimal(unscaledValue, first._scale);
  }
};


/** @return {boolean} Whether this value is negative. */
BigDecimal.prototype.isNegative = function () {
  return this._intVal.isNegative();
};

/** @return {boolean} Whether this value is zero. */
BigDecimal.prototype.isZero = function () {
  return this._intVal.isZero();
};

/**
 * Returns the string representation of this {@code BigDecimal}
 * @returns {string}
 */
BigDecimal.prototype.toString = function () {
  var intString = this._intVal.toString();
  if (this._scale === 0) {
    return intString;
  }
  var signSymbol = '';
  if (intString.charAt(0) === '-') {
    signSymbol = '-';
    intString = intString.substr(1);
  }
  var separatorIndex = intString.length - this._scale;
  if (separatorIndex <= 0) {
    //add zeros at the beginning, plus an additional zero
    intString = utils.stringRepeat('0', (-separatorIndex) + 1) + intString;
    separatorIndex = intString.length - this._scale;
  }
  return signSymbol + intString.substr(0, separatorIndex) + '.' + intString.substr(separatorIndex);
};

module.exports = BigDecimal;