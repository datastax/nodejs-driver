var util = require('util');
var utils = require('../utils');
/** @module types */
/**
 * Creates a new instance of InetAddress
 * @class
 * @classdesc Represents an v4 or v6 Internet Protocol (IP) address.
 * @param {Buffer} buffer
 * @constructor
 */
function InetAddress(buffer) {
  if (!(buffer instanceof Buffer) || (buffer.length !== 4 && buffer.length !== 16)) {
    throw new Error('The ip address must contain 4 or 16 bytes');
  }
  this.buffer = buffer;
  /**
   * Returns the length of the underlying buffer
   * @name length
   * @type Number
   * @memberof module:types~InetAddress#
   */
  Object.defineProperty(this, 'length', {get: function () { return buffer.length; }, enumerable: true});
}

/**
 * Compares 2 addresses and returns true if the underlying bytes are the same
 * @param {InetAddress} other
 * @returns {Boolean}
 */
InetAddress.prototype.equals = function (other) {
  if (!(other instanceof InetAddress)) {
    return false;
  }
  return (this.buffer.length === other.buffer.length &&
    this.buffer.toString('hex') === other.buffer.toString('hex'));
};

/**
 * Returns the underlying buffer
 * @returns {Buffer}
 */
InetAddress.prototype.getBuffer = function () {
  return this.buffer;
};

/**
 * Provide the name of the constructor and the string representation
 * @returns {string}
 */
InetAddress.prototype.inspect = function () {
  return this.constructor.name + ': ' + this.toString();
};

/**
 * Returns the string representation of the IP address.
 * <p>For v4 IP addresses, a string in the form of d.d.d.d is returned.</p>
 * <p>
 *   For v6 IP addresses, a string in the form of x:x:x:x:x:x:x:x is returned, where the 'x's are the hexadecimal
 *   values of the eight 16-bit pieces of the address, according to rfc5952.
 *   In cases where there is more than one field of only zeros, it can be shortened. For example, 2001:0db8:0:0:0:1:0:1
 *   will be expressed as 2001:0db8::1:0:1.
 * </p>
 * @returns {String}
 */
InetAddress.prototype.toString = function () {
  if (this.buffer.length === 4) {
    return (this.buffer[0] + '.' +
    this.buffer[1] + '.' +
    this.buffer[2] + '.' +
    this.buffer[3])
  }
  var start = -1;
  var longest = { length: 0, start: -1};
  function checkLongest (i) {
    if (start >= 0) {
      //close the group
      var length = i - start;
      if (length > longest.length) {
        longest.length = length;
        longest.start = start;
        start = -1;
      }
    }
  }
  //get the longest 16-bit group of zeros
  for (var i = 0; i < this.buffer.length; i = i + 2) {
    if (this.buffer[i] === 0 && this.buffer[i + 1] === 0) {
      //its a group of zeros
      if (start < 0) {
        start = i;
      }
      continue;
    }
    //its a group of non-zeros
    checkLongest(i);
  }
  checkLongest(this.buffer.length);

  var address = '';
  for (var h = 0; h < this.buffer.length; h = h + 2) {
    if (h === longest.start) {
      address += ':';
      continue;
    }
    if (h < (longest.start + longest.length) && h > longest.start) {
      //its a group of zeros
      continue;
    }
    if (address.length > 0) {
      address += ':';
    }
    address += utils.toHex(this.buffer[h]) + utils.toHex(this.buffer[h+1]);
  }
  if (address.charAt(address.length-1) === ':') {
    address += ':';
  }
  return address;
};

module.exports = InetAddress;