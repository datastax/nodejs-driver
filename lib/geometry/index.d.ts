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