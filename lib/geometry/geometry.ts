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

const endianness = {
  '0': 'BE',
  '1': 'LE'
};

class Geometry {
  static types = {
    Point2D: 1,
    LineString: 2,
    Polygon: 3
  } as const;

  /**
   * @protected
   * @param {Number} code
   * @returns {String}
   * @ignore @internal
   */
  static getEndianness(code: number): string {
    const value = endianness[code.toString()];
    if (typeof value === 'undefined') {
      throw new TypeError('Invalid endianness with code ' + code);
    }
    return value;
  }

  /**
   * Reads an int32 from binary representation based on endianness.
   * @protected
   * @param {Buffer} buffer
   * @param {String} endianness
   * @param {Number} offset
   * @returns Number
   * @ignore @internal
   */
  static readInt32(buffer: Buffer, endianness: string, offset: number): number {
    if (endianness === 'BE') {
      // Old Node.js versions, e.g. v8, has buf.readInt32BE(offset[, noAssert])
      // Newer versions do not have the noAssert parameter anymore
      return buffer.readInt32BE(offset);
    }
    return buffer.readInt32LE(offset);
  }

  /**
   * Reads a 64-bit double from binary representation based on endianness.
   * @protected
   * @param {Buffer} buffer
   * @param {String} endianness
   * @param {Number} offset
   * @returns Number
   * @ignore @internal
   */
  static readDouble(buffer: Buffer, endianness: string, offset: number): number {
    if (endianness === 'BE') {
      return buffer.readDoubleBE(offset);
    }
    return buffer.readDoubleLE(offset);
  }

  /**
   * Writes a 32-bit integer to binary representation based on OS endianness.
   * @protected
   * @param {Number} val
   * @param {Buffer} buffer
   * @param {Number} offset
   * @ignore @internal
   */
  writeInt32(val: number, buffer: Buffer, offset: number): void {
    if (this.useBESerialization()) {
      buffer.writeInt32BE(val, offset);
    } else {
      buffer.writeInt32LE(val, offset);
    }
  }

  /**
   * Writes a 64-bit double to binary representation based on OS endianness.
   * @protected
   * @param {Number} val
   * @param {Buffer} buffer
   * @param {Number} offset
   * @ignore @internal
   */
  writeDouble(val: number, buffer: Buffer, offset: number): void {
    if (this.useBESerialization()) {
      buffer.writeDoubleBE(val, offset);
    } else {
      buffer.writeDoubleLE(val, offset);
    }
  }

  /**
   * Writes an 8-bit int that represents the OS endianness.
   * @protected
   * @param {Buffer} buffer
   * @param {Number} offset
   * @ignore @internal
   */
  writeEndianness(buffer: Buffer, offset: number): void {
    if (this.useBESerialization()) {
      buffer.writeInt8(0, offset);
    } else {
      buffer.writeInt8(1, offset);
    }
  }

  /**
   * Returns true if the serialization must be done in big-endian format.
   * Designed to allow injection of OS endianness.
   * @abstract
   * @ignore @internal
   */
  useBESerialization(): boolean {
    throw new Error('Not Implemented');
  }
}

export default Geometry;