/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

export namespace geometry {
  class LineString {
    constructor(...args: Point[]);

    static fromBuffer(buffer: Buffer): LineString;

    static fromString(textValue: string): LineString;

    equals(other: LineString): boolean;

    toBuffer(): Buffer;

    toJSON(): string;

    toString(): string;

  }

  class Point {
    constructor(x: number, y: number);

    static fromBuffer(buffer: Buffer): Point;

    static fromString(textValue: string): Point;

    equals(other: Point): boolean;

    toBuffer(): Buffer;

    toJSON(): string;

    toString(): string;

  }

  class Polygon {
    constructor(...args: Point[]);

    static fromBuffer(buffer: Buffer): Polygon;

    static fromString(textValue: string): Polygon;

    equals(other: Polygon): boolean;

    toBuffer(): Buffer;

    toJSON(): string;

    toString(): string;
  }
}