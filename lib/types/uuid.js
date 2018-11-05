'use strict';

const crypto = require('crypto');
const utils = require('../utils');

/** @module types */

/**
 * Creates a new instance of Uuid based on a Buffer
 * @class
 * @classdesc Represents an immutable universally unique identifier (UUID). A UUID represents a 128-bit value.
 * @param {Buffer} buffer The 16-length buffer.
 * @constructor
 */
function Uuid(buffer) {
  if (!buffer || buffer.length !== 16) {
    throw new Error('You must provide a buffer containing 16 bytes');
  }
  this.buffer = buffer;
}

/**
 * Parses a string representation of a Uuid
 * @param {String} value
 * @returns {Uuid}
 */
Uuid.fromString = function (value) {
  //36 chars: 32 + 4 hyphens
  if (typeof value !== 'string' || value.length !== 36) {
    throw new Error('Invalid string representation of Uuid, it should be in the 00000000-0000-0000-0000-000000000000');
  }
  return new Uuid(utils.allocBufferFromString(value.replace(/-/g, ''), 'hex'));
};

/**
 * Creates a new random (version 4) Uuid.
 * @param {function} [callback] Optional callback to be invoked with the error as first parameter and the created Uuid as
 * second parameter.
 * @returns {Uuid}
 */
Uuid.random = function (callback) {
  if (callback) {
    getRandomBytes(function(err, buffer) {
      if (err) {
        return callback(err);
      }
      return callback(null, createUuidFromBuffer(buffer));
    });
  } else {
    const buffer = getRandomBytes();
    return createUuidFromBuffer(buffer);
  }
};

/**
 * Gets the bytes representation of a Uuid
 * @returns {Buffer}
 */
Uuid.prototype.getBuffer = function () {
  return this.buffer;
};
/**
 * Compares this object to the specified object.
 * The result is true if and only if the argument is not null, is a UUID object, and contains the same value, bit for bit, as this UUID.
 * @param {Uuid} other The other value to test for equality.
 */
Uuid.prototype.equals = function (other) {
  return other instanceof Uuid && this.buffer.equals(other.buffer);
};

/**
 * Returns a string representation of the value of this Uuid instance.
 * 32 hex separated by hyphens, in the form of 00000000-0000-0000-0000-000000000000.
 * @returns {String}
 */
Uuid.prototype.toString = function () {
  //32 hex representation of the Buffer
  const hexValue = getHex(this);
  return (
    hexValue.substr(0, 8) + '-' +
    hexValue.substr(8, 4) + '-' +
    hexValue.substr(12, 4) + '-' +
    hexValue.substr(16, 4) + '-' +
    hexValue.substr(20, 12));
};

/**
 * Provide the name of the constructor and the string representation
 * @returns {string}
 */
Uuid.prototype.inspect = function () {
  return this.constructor.name + ': ' + this.toString();
};

/**
 * Returns the string representation.
 * Method used by the native JSON.stringify() to serialize this instance.
 */
Uuid.prototype.toJSON = function () {
  return this.toString();
};


/**
 * Returns new Uuid
 * @private
 * @returns {Uuid}
 */
function createUuidFromBuffer (buffer) {
  //clear the version
  buffer[6] &= 0x0f;
  //set the version 4
  buffer[6] |= 0x40;
  //clear the variant
  buffer[8] &= 0x3f;
  //set the IETF variant
  buffer[8] |= 0x80;
  return new Uuid(buffer);
}

/**
 * @private
 * @returns {String} 32 hex representation of the instance, without separators
 */
function getHex (uuid) {
  return uuid.buffer.toString('hex');
}

/**
 * Gets a crypto generated 16 bytes
 * @private
 * @returns {Buffer}
 */
function getRandomBytes (cb) {
  return crypto.randomBytes(16, cb);
}

module.exports = Uuid;
