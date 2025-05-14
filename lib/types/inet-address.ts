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
import utils from "../utils";

/** @module types */
/**
 * @class
 * @classdesc Represents an v4 or v6 Internet Protocol (IP) address.
 */
class InetAddress {
  private buffer: Buffer;
  length: number;
  version: number;

  /**
   * Creates a new instance of InetAddress
   * @param {Buffer} buffer
   * @constructor
   */
  constructor(buffer: Buffer) {
    if (!(buffer instanceof Buffer) || (buffer.length !== 4 && buffer.length !== 16)) {
      throw new TypeError('The ip address must contain 4 or 16 bytes');
    }

    /**
     * Immutable buffer that represents the IP address 
     * @type Array
     */
    this.buffer = buffer;

    /**
     * Returns the length of the underlying buffer
     * @type Number
     */
    this.length = buffer.length;

    /**
     * Returns the Ip version (4 or 6)
     * @type Number
     */
    this.version = buffer.length === 4 ? 4 : 6;
  }

  /**
   * Parses the string representation and returns an Ip address
   * @param {String} value
   */
  static fromString(value: string): InetAddress {
    if (!value) {
      return new InetAddress(utils.allocBufferFromArray([0, 0, 0, 0]));
    }
    const ipv4Pattern = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/;
    const ipv6Pattern = /^[\da-f:.]+$/i;
    let parts;
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
    const buffer = utils.allocBufferUnsafe(16);
    let filling = 8 - parts.length + 1;
    let applied = false;
    let offset = 0;
    const embeddedIp4 = ipv4Pattern.test(parts[parts.length - 1]);
    if (embeddedIp4) {
      // Its IPv6 address with an embedded IPv4 address:
      // subtract 1 from the potential empty filling as ip4 contains 4 bytes instead of 2 of a ipv6 section
      filling -= 1;
    }
    function writeItem(uIntValue) {
      buffer.writeUInt8(+uIntValue, offset++);
    }
    for (let i = 0; i < parts.length; i++) {
      const item = parts[i];
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
      for (let j = 0; j < filling; j++) {
        buffer[offset++] = 0;
        buffer[offset++] = 0;
      }
    }
    if (embeddedIp4 && !InetAddress.isValidIPv4Mapped(buffer)) {
      throw new TypeError('Only IPv4-Mapped IPv6 addresses are allowed as IPv6 address with embedded IPv4 address');
    }
    return new InetAddress(buffer);
  }

  /**
   * Compares 2 addresses and returns true if the underlying bytes are the same
   * @param {InetAddress} other
   * @returns {Boolean}
   */
  equals(other: InetAddress): boolean {
    if (!(other instanceof InetAddress)) {
      return false;
    }
    return (this.buffer.length === other.buffer.length &&
      this.buffer.toString('hex') === other.buffer.toString('hex'));
  }

  /**
   * Returns the underlying buffer
   * @returns {Buffer}
   */
  getBuffer(): Buffer {
    return this.buffer;
  }

  /**
   * Provide the name of the constructor and the string representation
   * @returns {string}
   * @internal
   */
  inspect(): string {
    return this.constructor.name + ': ' + this.toString();
  }

  /**
   * Returns the string representation of the IP address.
   * <p>For v4 IP addresses, a string in the form of d.d.d.d is returned.</p>
   * <p>
   *   For v6 IP addresses, a string in the form of x:x:x:x:x:x:x:x is returned, where the 'x's are the hexadecimal
   *   values of the eight 16-bit pieces of the address, according to rfc5952.
   *   In cases where there is more than one field of only zeros, it can be shortened. For example, 2001:0db8:0:0:0:1:0:1
   *   will be expressed as 2001:0db8::1:0:1.
   * </p>
   * @param {String} [encoding] If set to 'hex', the hex representation of the buffer is returned.
   * @returns {String}
   */
  toString(encoding?: string): string {
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
    let start = -1;
    const longest = { length: 0, start: -1};
    function checkLongest (i) {
      if (start >= 0) {
        //close the group
        const length = i - start;
        if (length > longest.length) {
          longest.length = length;
          longest.start = start;
          start = -1;
        }
      }
    }
    //get the longest 16-bit group of zeros
    for (let i = 0; i < this.buffer.length; i = i + 2) {
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

    let address = '';
    for (let h = 0; h < this.buffer.length; h = h + 2) {
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
  }

  /**
   * Returns the string representation.
   * Method used by the native JSON.stringify() to serialize this instance.
   */
  toJSON(): string {
    return this.toString();
  }

  /**
   * Validates for a IPv4-Mapped IPv6 according to https://tools.ietf.org/html/rfc4291#section-2.5.5
   * @private
   * @param {Buffer} buffer
   */
  private static isValidIPv4Mapped(buffer: Buffer): boolean {
    // check the form
    // |      80 bits   | 16 |   32 bits
    // +----------------+----+-------------
    // |0000........0000|FFFF| IPv4 address

    for (let i = 0; i < buffer.length - 6; i++) {
      if (buffer[i] !== 0) {
        return false;
      }
    }
    return !(buffer[10] !== 255 || buffer[11] !== 255);
  }
}

export default InetAddress;