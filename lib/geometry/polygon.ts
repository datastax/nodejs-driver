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
import Geometry from "./geometry";
import LineString from "./line-string";
import Point from "./point";

/**
 * @classdesc
 * Represents is a plane geometry figure that is bounded by a finite chain of straight line segments closing in a loop
 * to form a closed chain or circuit.
 * @example
 * new Polygon([ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ]);
 * @example
 * //polygon with a hole
 * new Polygon(
 *  [ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ],
 *  [ new Point(25, 20), new Point(30, 30), new Point(20, 20), new Point(25, 20) ]
 * );
 * @alias module:geometry~Polygon
 */
class Polygon extends Geometry {
  /** @internal */
  rings: ReadonlyArray<ReadonlyArray<Point>>;

  //TODO: exposed as constructor(...args: Point[]); but clearly constructor(...args: Point[][])
  /**
   * Creates a new {@link Polygon} instance.
   * @param {...Array.<Point>}[ringPoints] A sequence of Array of [Point]{@link module:geometry~Point} items as arguments
   * representing the rings of the polygon.
   * @example
   * new Polygon([ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ]);
   * @example
   * //polygon with a hole
   * new Polygon(
   *  [ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ],
   *  [ new Point(25, 20), new Point(30, 30), new Point(20, 20), new Point(25, 20) ]
   * );
   * @constructor
   */
  constructor(...ringPoints: Point[][]) {
    super();
    this.rings = Object.freeze(ringPoints);
  }

  /**
   * Creates a {@link Polygon} instance from
   * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
   * representation of a polygon.
   * @param {Buffer} buffer
   * @returns {Polygon}
   */
  static fromBuffer(buffer: Buffer): Polygon {
    if (!buffer || buffer.length < 9) {
      throw new TypeError("A Polygon buffer should contain at least 9 bytes");
    }
    const endianness = Geometry.getEndianness(buffer.readInt8(0));
    let offset = 1;
    if (Geometry.readInt32(buffer, endianness, offset) !== Geometry.types.Polygon) {
      throw new TypeError("Binary representation was not a Polygon");
    }
    offset += 4;
    const ringsLength = Geometry.readInt32(buffer, endianness, offset);
    offset += 4;
    const ringsArray : Point[][]= new Array(ringsLength);
    for (let ringIndex = 0; ringIndex < ringsLength; ringIndex++) {
      const pointsLength = Geometry.readInt32(buffer, endianness, offset);
      offset += 4;
      if (buffer.length < offset + pointsLength * 16) {
        throw new TypeError("Length of the buffer does not match");
      }
      const ring = new Array(pointsLength);
      for (let i = 0; i < pointsLength; i++) {
        ring[i] = new Point(
          Geometry.readDouble(buffer, endianness, offset),
          Geometry.readDouble(buffer, endianness, offset + 8)
        );
        offset += 16;
      }
      ringsArray[ringIndex] = ring;
    }
    return new Polygon(...ringsArray);
  }

  /**
   * Creates a {@link Polygon} instance from a Well-known Text (WKT) representation.
   * @param {String} textValue
   * @returns {Polygon}
   */
  static fromString(textValue: string): Polygon {
    const wktRegex = /^POLYGON ?\((\(.*\))\)$/g;
    const matches = wktRegex.exec(textValue);
    function validateWkt(condition) {
      if (condition) {
        throw new TypeError('Invalid WKT: ' + textValue);
      }
    }
    validateWkt(!matches || matches.length !== 2);
  
    const ringsText = matches[1];
    const ringsArray : string[] = [];
    let ringStart = null;
    for (let i = 0; i < ringsText.length; i++) {
      const c = ringsText[i];
      if (c === '(') {
        validateWkt(ringStart !== null);
        ringStart = i+1;
        continue;
      }
      if (c === ')') {
        validateWkt(ringStart === null);
        ringsArray.push(ringsText.substring(ringStart, i));
        ringStart = null;
        continue;
      }
      validateWkt(ringStart === null && c !== ' ' && c !== ',');
    }
    return new Polygon(...ringsArray.map(LineString.parseSegments));
  }

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
   * representation of this instance.
   * @returns {Buffer}
   */
  toBuffer(): Buffer {
    let totalRingsLength = 0;
    this.rings.forEach((ring) => {
      totalRingsLength += 4 + ring.length * 16;
    });
    const buffer = utils.allocBufferUnsafe(9 + totalRingsLength);
    this.writeEndianness(buffer, 0);
    let offset = 1;
    this.writeInt32(Geometry.types.Polygon, buffer, offset);
    offset += 4;
    this.writeInt32(this.rings.length, buffer, offset);
    offset += 4;
    this.rings.forEach((ring) => {
      this.writeInt32(ring.length, buffer, offset);
      offset += 4;
      ring.forEach((p) => {
        this.writeDouble(p.x, buffer, offset);
        this.writeDouble(p.y, buffer, offset + 8);
        offset += 16;
      });
    });
    return buffer;
  }

  /**
   * Returns true if the values of the polygons are the same, otherwise it returns false.
   * @param {Polygon} other
   * @returns {Boolean}
   */
  equals(other: Polygon): boolean {
    if (!(other instanceof Polygon)) {
      return false;
    }
    if (this.rings.length !== other.rings.length) {
      return false;
    }
    for (let i = 0; i < this.rings.length; i++) {
      const r1 = this.rings[i];
      const r2 = other.rings[i];
      if (r1.length !== r2.length) {
        return false;
      }
      for (let j = 0; j < r1.length; j++) {
        if (!r1[j].equals(r2[j])) {
          return false;
        }
      }
    }
    return true;
  }

  /** @internal */
  useBESerialization(): boolean {
    return false;
  }

  /**
   * Returns Well-known Text (WKT) representation of the geometry object.
   * @returns {String}
   */
  toString(): string {
    if (this.rings.length === 0) {
      return 'POLYGON EMPTY';
    }
    let ringStrings = '';
    this.rings.forEach(function (r, i) {
      if (i > 0) {
        ringStrings += ', ';
      }
      ringStrings += '(' +
        r.map(function (p) {
          return p.x + ' ' + p.y;
        }).join(', ')
        + ')';
    });
    return 'POLYGON (' + ringStrings + ')';
  }

  /**
   * Returns a JSON representation of this geo-spatial type.
   */
  toJSON(): object {
    return { type: 'Polygon', coordinates: this.rings.map(function (r) {
      return r.map(function (p) {
        return [ p.x, p.y ];
      });
    })};
  }
}

export default Polygon;