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

import _Long = require('long');
import * as stream from 'stream';
import { ValueCallback } from '../../';

export namespace types {
  class Long extends _Long {

  }

  enum consistencies {
    any = 0x00,
    one = 0x01,
    two = 0x02,
    three = 0x03,
    quorum = 0x04,
    all = 0x05,
    localQuorum = 0x06,
    eachQuorum = 0x07,
    serial = 0x08,
    localSerial = 0x09,
    localOne = 0x0a
  }

  enum dataTypes {
    custom = 0x0000,
    ascii = 0x0001,
    bigint = 0x0002,
    blob = 0x0003,
    boolean = 0x0004,
    counter = 0x0005,
    decimal = 0x0006,
    double = 0x0007,
    float = 0x0008,
    int = 0x0009,
    text = 0x000a,
    timestamp = 0x000b,
    uuid = 0x000c,
    varchar = 0x000d,
    varint = 0x000e,
    timeuuid = 0x000f,
    inet = 0x0010,
    date = 0x0011,
    time = 0x0012,
    smallint = 0x0013,
    tinyint = 0x0014,
    duration = 0x0015,
    list = 0x0020,
    map = 0x0021,
    set = 0x0022,
    udt = 0x0030,
    tuple = 0x0031,
  }

  enum distance {
    local = 0,
    remote,
    ignored
  }

  enum responseErrorCodes {
    serverError = 0x0000,
    protocolError = 0x000A,
    badCredentials = 0x0100,
    unavailableException = 0x1000,
    overloaded = 0x1001,
    isBootstrapping = 0x1002,
    truncateError = 0x1003,
    writeTimeout = 0x1100,
    readTimeout = 0x1200,
    readFailure = 0x1300,
    functionFailure = 0x1400,
    writeFailure = 0x1500,
    syntaxError = 0x2000,
    unauthorized = 0x2100,
    invalid = 0x2200,
    configError = 0x2300,
    alreadyExists = 0x2400,
    unprepared = 0x2500,
    clientWriteFailure = 0x8000
  }

  enum protocolVersion {
    v1 = 0x01,
    v2 = 0x02,
    v3 = 0x03,
    v4 = 0x04,
    v5 = 0x05,
    v6 = 0x06,
    dseV1 = 0x41,
    dseV2 = 0x42,
    maxSupported = dseV2,
    minSupported = v1
  }

  namespace protocolVersion {
    function isSupported(version: protocolVersion): boolean;
  }

  const unset: object;

  class BigDecimal {
    constructor(unscaledValue: number, scale: number);

    static fromBuffer(buf: Buffer): BigDecimal;

    static fromString(value: string): BigDecimal;

    static toBuffer(value: BigDecimal): Buffer;

    static fromNumber(value: number): BigDecimal;

    add(other: BigDecimal): BigDecimal;

    compare(other: BigDecimal): number;

    equals(other: BigDecimal): boolean;

    greaterThan(other: BigDecimal): boolean;

    isNegative(): boolean;

    isZero(): boolean;

    notEquals(other: BigDecimal): boolean;

    subtract(other: BigDecimal): BigDecimal;

    toNumber(): number;

    toString(): string;

    toJSON(): string;
  }

  class Duration {
    constructor(month: number, days: number, nanoseconds: number | Long);

    static fromBuffer(buffer: Buffer): Duration;

    static fromString(input: string): Duration;

    equals(other: Duration): boolean;

    toBuffer(): Buffer;

    toString(): string;
  }

  class InetAddress {
    length: number;

    version: number;

    constructor(buffer: Buffer);

    static fromString(value: string): InetAddress;

    equals(other: InetAddress): boolean;

    getBuffer(): Buffer;

    toString(): string;

    toJSON(): string;
  }

  class Integer {
    static ONE: Integer;
    static ZERO: Integer;

    constructor(bits: Array<number>, sign: number);

    static fromBits(bits: Array<number>): Integer;

    static fromBuffer(bits: Buffer): Integer;

    static fromInt(value: number): Integer;

    static fromNumber(value: number): Integer;

    static fromString(str: string, opt_radix?: number): Integer;

    static toBuffer(value: Integer): Buffer;

    abs(): Integer;

    add(other: Integer): Integer;

    compare(other: Integer): number;

    divide(other: Integer): Integer;

    equals(other: Integer): boolean;

    getBits(index: number): number;

    getBitsUnsigned(index: number): number;

    getSign(): number;

    greaterThan(other: Integer): boolean;

    greaterThanOrEqual(other: Integer): boolean;

    isNegative(): boolean;

    isOdd(): boolean;

    isZero(): boolean;

    lessThan(other: Integer): boolean;

    lessThanOrEqual(other: Integer): boolean;

    modulo(other: Integer): Integer;

    multiply(other: Integer): Integer;

    negate(): Integer;

    not(): Integer;

    notEquals(other: Integer): boolean;

    or(other: Integer): Integer;

    shiftLeft(numBits: number): Integer;

    shiftRight(numBits: number): Integer;

    shorten(numBits: number): Integer;

    subtract(other: Integer): Integer;

    toInt(): number;

    toJSON(): string;

    toNumber(): number;

    toString(opt_radix?: number): string;

    xor(other: Integer): Integer;
  }

  class LocalDate {
    year: number;
    month: number;
    day: number;

    constructor(year: number, month: number, day: number);

    static fromDate(date: Date): LocalDate;

    static fromString(value: string): LocalDate;

    static fromBuffer(buffer: Buffer): LocalDate;

    static now(): LocalDate;

    static utcNow(): LocalDate;

    equals(other: LocalDate): boolean;

    inspect(): string;

    toBuffer(): Buffer;

    toJSON(): string;

    toString(): string;
  }

  class LocalTime {
    hour: number;
    minute: number;
    nanosecond: number;
    second: number;

    constructor(totalNanoseconds: Long);

    static fromBuffer(value: Buffer): LocalTime;

    static fromDate(date: Date, nanoseconds: number): LocalTime;

    static fromMilliseconds(milliseconds: number, nanoseconds?: number): LocalTime;

    static fromString(value: string): LocalTime;

    static now(nanoseconds?: number): LocalTime;

    compare(other: LocalTime): boolean;

    equals(other: LocalTime): boolean;

    getTotalNanoseconds(): Long;

    inspect(): string;

    toBuffer(): Buffer;

    toJSON(): string;

    toString(): string;
  }

  interface ResultSet extends Iterable<Row>, AsyncIterable<Row> {
    info: {
      queriedHost: string,
      triedHosts: { [key: string]: any; },
      speculativeExecutions: number,
      achievedConsistency: consistencies,
      traceId: Uuid,
      warnings: string[],
      customPayload: any
    };

    columns: Array<{ name: string, type: { code: dataTypes, info: any } }>;
    nextPage: (() => void) | null;
    pageState: string;
    rowLength: number;
    rows: Row[];

    first(): Row;

    wasApplied(): boolean;
  }

  interface ResultStream extends stream.Readable {
    buffer: Buffer;
    paused: boolean;

    add(chunk: Buffer): void;
  }

  interface Row {
    get(columnName: string | number): any;

    keys(): string[];

    forEach(callback: (row: Row) => void): void;

    values(): any[];

    [key: string]: any;
  }

  class TimeUuid extends Uuid {
    static now(): TimeUuid;

    static now(nodeId: string | Buffer, clockId?: string | Buffer): TimeUuid;

    static now(nodeId: string | Buffer, clockId: string | Buffer, callback: ValueCallback<TimeUuid>): void;

    static now(callback: ValueCallback<TimeUuid>): void;

    static fromDate(date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer): TimeUuid;

    static fromDate(
      date: Date,
      ticks: number,
      nodeId: string | Buffer,
      clockId: string | Buffer,
      callback: ValueCallback<TimeUuid>): void;

    static fromString(value: string): TimeUuid;

    static max(date: Date, ticks: number): TimeUuid;

    static min(date: Date, ticks: number): TimeUuid;

    getDatePrecision(): { date: Date, ticks: number };

    getDate(): Date;
  }

  class Tuple {
    elements: any[];
    length: number;

    constructor(...args: any[]);

    static fromArray(elements: any[]): Tuple;

    get(index: number): any;

    toString(): string;

    toJSON(): string;

    values(): any[];
  }

  class Uuid {
    constructor(buffer: Buffer);

    static fromString(value: string): Uuid;

    static random(callback: ValueCallback<Uuid>): void;

    static random(): Uuid;

    equals(other: Uuid): boolean;

    getBuffer(): Buffer;

    toString(): string;

    toJSON(): string;
  }

  class Vector {
    static get [Symbol.species](): typeof Vector;
    /**
       *
       * @param {Float32Array | Array<any>} elements
       * @param {string?} subtype
       */
    constructor(elements: Float32Array | Array<any>, subtype?: string | null);
    elements: any[];
    /**
         * Returns the number of the elements.
         * @type Number
         */
    length: number;
    subtype: string;
    /**
       * Returns the string representation of the vector.
       * @returns {string}
       */
    toString(): string;
    /**
       *
       * @param {number} index
       */
    at(index: number): any;
    /**
       *
       * @param {(value: any, index: number, array: any[]) => void} callback
       */
    forEach(callback: (value: any, index: number, array: any[]) => void): void;
    /**
     * @returns {string | null} get the subtype string, e.g., "float", but it's optional so it can return null
     */
    getSubtype(): string | null;
    /**
       *
       * @returns {IterableIterator<any>}
       */
    [Symbol.iterator](): IterableIterator<any>;
  }
}