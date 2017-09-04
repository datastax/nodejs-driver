"use strict";

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
    throw new TypeError('The ip address must contain 4 or 16 bytes');
  }
  this.buffer = buffer;
  /**
   * Returns the length of the underlying buffer
   * @name length
   * @type Number
   * @memberof module:types~InetAddress#
   */
  Object.defineProperty(this, 'length', {get: function () { return buffer.length; }, enumerable: true});
  /**
   * Returns the Ip version (4 or 6)
   * @name version
   * @type Number
   * @memberof module:types~InetAddress#
   */
  Object.defineProperty(this, 'version', {get: function () { return buffer.length === 4 ? 4 : 6; }, enumerable: true});
}

/**
 * Parses the string representation and returns an Ip address
 * @param {String} value
 */
InetAddress.fromString = function (value) {
  if (!value) {
    return new InetAddress(utils.allocBufferFromArray([0, 0, 0, 0]));
  }
  var ipv4Pattern = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/;
  var ipv6Pattern = /^[\da-f:.]+$/i;
  var parts;
  if (ipv4Pattern.test(value)) {
    parts = value.split('.');
    return new InetAddress(utils.allocBufferFromArray(parts));
  }
  if (!ipv6Pattern.test(value)) {
    throw new TypeError('Value could not be parsed as InetAddress: ' + value);
  }
  parts = value.split(':');
  if (parts.length < 3) {
    throw new TypeError('Value could not be parsed as InetAddress: ' + value);
  }
  var buffer = utils.allocBufferUnsafe(16);
  var filling = 8 - parts.length + 1;
  var applied = false;
  var offset = 0;
  var embeddedIp4 = ipv4Pattern.test(parts[parts.length - 1]);
  if (embeddedIp4) {
    // Its IPv6 address with an embedded IPv4 address:
    // subtract 1 from the potential empty filling as ip4 contains 4 bytes instead of 2 of a ipv6 section
    filling -= 1;
  }
  function writeItem(uIntValue) {
    buffer.writeUInt8(+uIntValue, offset++);
  }
  for (var i = 0; i < parts.length; i++) {
    var item = parts[i];
    if (item) {
      if (embeddedIp4 && i === parts.length - 1) {
        item.split('.').forEach(writeItem);
        break;
      }
      buffer.writeUInt16BE(parseInt(item, 16), offset);
      offset = offset + 2;
      continue;
    }
    //its an empty string
    if (applied) {
      //there could be 2 occurrences of empty string
      filling = 1;
    }
    applied = true;
    for (var j = 0; j < filling; j++) {
      buffer[offset++] = 0;
      buffer[offset++] = 0;
    }
  }
  if (embeddedIp4 && !isValidIPv4Mapped(buffer)) {
    throw new TypeError('Only IPv4-Mapped IPv6 addresses are allowed as IPv6 address with embedded IPv4 address');
  }
  return new InetAddress(buffer);
};

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
 * @param {String} [encoding]
 * @returns {String}
 */
InetAddress.prototype.toString = function (encoding) {
  if (encoding === 'hex') {
    //backward compatibility: behave in the same way as the buffer
    return this.buffer.toString('hex');
  }
  if (this.buffer.length === 4) {
    return (
      this.buffer[0] + '.' +
      this.buffer[1] + '.' +
      this.buffer[2] + '.' +
      this.buffer[3]
    );
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

      // at the end of the buffer, make a final call to checkLongest.
      if(i === this.buffer.length - 2) {
        checkLongest(i+2);
      }
      continue;
    }
    //its a group of non-zeros
    checkLongest(i);
  }

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
    address += ((this.buffer[h] << 8) | this.buffer[h+1]).toString(16);
  }
  if (address.charAt(address.length-1) === ':') {
    address += ':';
  }
  return address;
};

/**
 * Returns the string representation.
 * Method used by the native JSON.stringify() to serialize this instance.
 */
InetAddress.prototype.toJSON = function () {
  return this.toString();
};

/**
 * Validates for a IPv4-Mapped IPv6 according to https://tools.ietf.org/html/rfc4291#section-2.5.5
 * @private
 * @param {Buffer} buffer
 */
function isValidIPv4Mapped(buffer) {
  // check the form
  // |      80 bits   | 16 |   32 bits
  // +----------------+----+-------------
  // |0000........0000|FFFF| IPv4 address

  for (var i = 0; i < buffer.length - 6; i++) {
    if (buffer[i] !== 0) {
      return false;
    }
  }
  return !(buffer[10] !== 255 || buffer[11] !== 255);
}

module.exports = InetAddress;