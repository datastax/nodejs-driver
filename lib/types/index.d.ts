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

// TypeScript Version: 2.2

import * as _Long from 'long';

export namespace types {
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
    unprepared = 0x2500
  }

  enum protocolVersion {
    v1 = 0x01,
    v2 = 0x02,
    v3 = 0x03,
    v4 = 0x04,
    v5 = 0x05
  }

  namespace protocolVersion {
    function isSupported(version: protocolVersion): boolean;
  }

  const unset: object;

  class Long extends _Long {

  }

  class TimeUuid extends Uuid {
    getDate(): Date;

    getDatePrecision: { date: Date, ticks: number };

    static now(): TimeUuid;

    static now(nodeId: string|Buffer, clockId?: string|Buffer): TimeUuid;

    static now(nodeId: string|Buffer, clockId: string|Buffer, callback: (err: Error, value: TimeUuid) => any): void;

    static now(callback: (err: Error, value: TimeUuid) => any): void;

    static fromDate(date: Date, ticks?: number, nodeId?: string|Buffer, clockId?: string|Buffer): TimeUuid;

    static fromDate(
      date: Date,
      ticks: number,
      nodeId: string|Buffer,
      clockId: string|Buffer,
      callback: (err: Error, value: TimeUuid) => any): void;

    static fromString(value: string): TimeUuid;

    static max(date: Date, ticks: number): TimeUuid;

    static min(date: Date, ticks: number): TimeUuid;
  }

  class Uuid {
    constructor(buffer: Buffer);

    equals(other: Uuid): boolean;

    getBuffer(): Buffer;

    toJSON(): string;

    toString(): string;

    static fromString(value: string): Uuid;

    static random(callback: (err: Error, value: Uuid) => any): void;

    static random(): Uuid;
  }
}