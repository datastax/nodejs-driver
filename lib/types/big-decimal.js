var Integer = require('./integer');
var utils = require('../utils');
/** @module types */
/**
 * Constructs an arbitrary-precision signed decimal number.
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
  return (
    (other instanceof BigDecimal) &&
    this._intVal.equals(other._intVal) &&
    this._scale === other._scale
  );
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