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

export namespace search {
  enum dateRangePrecision {
    year = 0,
    month,
    day,
    hour,
    minute,
    second,
    millisecond
  }

  class DateRange {
    lowerBound: DateRangeBound;
    upperBound: DateRangeBound;

    constructor(lowerBound: DateRangeBound, upperBound: DateRangeBound);

    equals(other: DateRangeBound): boolean;

    toString(): string;

    static fromString(value: string): DateRange;

    static fromBuffer(value: Buffer): DateRange;
  }

  class DateRangeBound {
    date: Date;

    precision: number;

    equals(other: DateRangeBound): boolean;

    toString(): string;

    static fromString(value: string): DateRangeBound;

    static toLowerBound(bound: DateRangeBound): DateRangeBound;

    static toUpperBound(bound: DateRangeBound): DateRangeBound;
  }
}