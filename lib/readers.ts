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
import util from "util";
import errors, { ResponseError } from "./errors";
import types, { FrameHeader, InetAddress, Uuid } from "./types/index";
import utils from "./utils";

/**
 * Information on the formatting of the returned rows
 */
const resultFlag = {
  globalTablesSpec:   0x0001,
  hasMorePages:       0x0002,
  noMetadata:         0x0004,
  metadataChanged:    0x0008,
  continuousPaging: 0x40000000,
  lastContinuousPage: 0x80000000,
};

// templates for derived error messages.
const _writeTimeoutQueryMessage = 'Server timeout during write query at consistency %s (%d peer(s) acknowledged the write over %d required)';
const _writeTimeoutBatchLogMessage = 'Server timeout during batchlog write at consistency %s (%d peer(s) acknowledged the write over %d required)';
const _writeFailureMessage = 'Server failure during write query at consistency %s (%d responses were required but only %d replicas responded, %d failed)';
const _unavailableMessage = 'Not enough replicas available for query at consistency %s (%d required but only %d alive)';
const _readTimeoutMessage = 'Server timeout during read query at consistency %s (%s)';
const _readFailureMessage = 'Server failure during read query at consistency %s (%d responses were required but only %d replicas responded, %d failed)';

type Meta = {
  resultId?: Buffer;
  flags?: number;
  partitionKeys?: number[];
  pageState?: Buffer;
  newResultId?: Buffer;
  continuousPageIndex?: number;
  lastContinuousPage?: boolean;
  global_tables_spec?: boolean;
  keyspace?: string;
  table?: string;
  columns?: Array<{
    ksname?: string;
    tablename?: string;
    name?: string;
    type?: { code: number; info: any | null }; 
  }>;
  columnsByName?: { [key: string]: number };
};

/**
 * Buffer forward reader of CQL binary frames
 * @param {FrameHeader} header
 * @param {Buffer} body
 * @param {Number} [offset]
 */
class FrameReader {
  header: FrameHeader;
  opcode: any;
  offset: number;
  buf: Buffer;

  /**
   * Creates a new instance of the reader
   * @param {FrameHeader} header
   * @param {Buffer} body
   * @param {Number} [offset]
   */
  constructor(header: FrameHeader, body: Buffer, offset: number) {
    this.header = header;
    this.opcode = header.opcode;
    this.offset = offset || 0;
    this.buf = body;
  }

  remainingLength() {
    return this.buf.length - this.offset;
  }

  getBuffer() {
    return this.buf;
  }

  /**
   * Slices the underlining buffer
   * @param {Number} begin
   * @param {Number} [end]
   * @returns {Buffer}
   */
  slice(begin: number, end?: number): Buffer {
    if (typeof end === 'undefined') {
      end = this.buf.length;
    }
    return this.buf.slice(begin, end);
  }

  /**
   * Modifies the underlying buffer, it concatenates the given buffer with the original (internalBuffer = concat(bytes, internalBuffer)
   */
  unshift(bytes) {
    if (this.offset > 0) {
      throw new Error('Can not modify the underlying buffer if already read');
    }
    this.buf = Buffer.concat([bytes, this.buf], bytes.length + this.buf.length);
  }

  /**
   * Reads any number of bytes and moves the offset.
   * if length not provided or it's larger than the remaining bytes, reads to end.
   * @param length
   * @returns {Buffer}
   */
  read(length): Buffer {
    let end = this.buf.length;
    if (typeof length !== 'undefined' && this.offset + length < this.buf.length) {
      end = this.offset + length;
    }
    const bytes = this.slice(this.offset, end);
    this.offset = end;
    return bytes;
  }

  /**
   * Moves the reader cursor to the end
   */
  toEnd() {
    this.offset = this.buf.length;
  }

  /**
   * Reads a BE Int and moves the offset
   * @returns {Number}
   */
  readInt(): number {
    const result = this.buf.readInt32BE(this.offset);
    this.offset += 4;
    return result;
  }

  /** @returns {Number} */
  readShort(): number {
    const result = this.buf.readUInt16BE(this.offset);
    this.offset += 2;
    return result;
  }

  readByte() {
    const result = this.buf.readUInt8(this.offset);
    this.offset += 1;
    return result;
  }

  readString() {
    const length = this.readShort();
    this.checkOffset(length);
    const result = this.buf.toString('utf8', this.offset, this.offset + length);
    this.offset += length;
    return result;
  }

  /**
   * Checks that the new length to read is within the range of the buffer length. Throws a RangeError if not.
   * @param {Number} newLength
   */
  checkOffset(newLength: number) {
    if (this.offset + newLength > this.buf.length) {
      const err = new RangeError('Trying to access beyond buffer length');
      // @ts-ignore
      err.expectedLength = newLength;
      throw err;
    }
  }

  /**
   * Reads a protocol string list
   * @returns {Array}
   */
  readStringList(): Array<any> {
    const length = this.readShort();
    const list = new Array(length);
    for (let i = 0; i < length; i++) {
      list[i] = this.readString();
    }
    return list;
  }

  /**
   * Reads the amount of bytes that the field has and returns them (slicing them).
   * @returns {Buffer}
   */
  readBytes(): Buffer {
    const length = this.readInt();
    if (length < 0) {
      return null;
    }
    this.checkOffset(length);
    return this.read(length);
  }

  readShortBytes() {
    const length = this.readShort();
    if (length < 0) {
      return null;
    }
    this.checkOffset(length);
    return this.read(length);
  }

  /**
   * Reads an associative array of strings as keys and bytes as values
   * @param {Number} length
   * @param {Function} keyFn
   * @param {Function} valueFn
   * @returns {Object}
   */
  readMap(length: number, keyFn: Function, valueFn: Function): object {
    if (length < 0) {
      return null;
    }
    const map = {};
    for (let i = 0; i < length; i++) {
      map[keyFn.call(this)] = valueFn.call(this);
    }
    return map;
  }

  /**
   * Reads an associative array of strings as keys and string lists as values
   * @returns {Object}
   */
  readStringMultiMap(): object {
    //A [short] n, followed by n pair <k><v> where <k> is a
    //[string] and <v> is a [string[]].
    const length = this.readShort();
    if (length < 0) {
      return null;
    }
    const map = {};
    for (let i = 0; i < length; i++) {
      map[this.readString()] = this.readStringList();
    }
    return map;
  }

  /**
   * Reads a data type definition
   * @returns {{code: Number, info: Object|null}} An array of 2 elements
   */
  readType(): {code: number, info: any|null} {
    let i;
    const type : {code: number, info: any|null} = {
      code: this.readShort(),
      // @ts-ignore
      type: null
    };
    switch (type.code) {
      case types.dataTypes.custom:
        type.info = this.readString();
        break;
      case types.dataTypes.list:
      case types.dataTypes.set:
        type.info = this.readType();
        break;
      case types.dataTypes.map:
        type.info = [this.readType(), this.readType()];
        break;
      case types.dataTypes.udt:
        type.info = {
          keyspace: this.readString(),
          name: this.readString(),
          fields: new Array(this.readShort())
        };
        for (i = 0; i < type.info.fields.length; i++) {
          type.info.fields[i] = {
            name: this.readString(),
            type: this.readType()
          };
        }
        break;
      case types.dataTypes.tuple:
        type.info = new Array(this.readShort());
        for (i = 0; i < type.info.length; i++) {
          type.info[i] = this.readType();
        }
        break;
    }
    return type;
  }

  /**
   * Reads an Ip address and port
   * @returns {{address: exports.InetAddress, port: Number}}
   */
  readInet(): { address: InetAddress; port: number; } {
    const length = this.readByte();
    const address = this.read(length);
    return { address: new types.InetAddress(address), port: this.readInt() };
  }

  /**
   * Reads an Ip address
   * @returns {InetAddress}
   */
  readInetAddress(): InetAddress {
    const length = this.readByte();
    return new types.InetAddress(this.read(length));
  }

  /**
   * Reads the body bytes corresponding to the flags
   * @returns {{traceId: Uuid, warnings: Array, customPayload}}
   * @throws {RangeError}
   */
  readFlagsInfo(): { traceId?: Uuid; warnings?: Array<any>; customPayload?; } {
    if (this.header.flags === 0) {
      return utils.emptyObject;
    }
    const result : { traceId?: Uuid; warnings?: Array<any>; customPayload?; } = {};
    if (this.header.flags & types.frameFlags.tracing) {
      this.checkOffset(16);
      result.traceId = new types.Uuid(utils.copyBuffer(this.read(16)));
    }
    if (this.header.flags & types.frameFlags.warning) {
      result.warnings = this.readStringList();
    }
    if (this.header.flags & types.frameFlags.customPayload) {
      // Custom payload is a Map<string, Buffer>
      result.customPayload = this.readMap(this.readShort(), this.readString, this.readBytes);
    }
    return result;
  }

  /**
   * Reads the metadata from a row or a prepared result response
   * @param {Number} kind
   * @returns {Object}
   * @throws {RangeError}
   */
  readMetadata(kind: number): Meta {
    let i;
    //Determines if its a prepared metadata
    const isPrepared = (kind === types.resultKind.prepared);
    const meta : Meta = {};
    if (types.protocolVersion.supportsResultMetadataId(this.header.version) && isPrepared) {
      meta.resultId = utils.copyBuffer(this.readShortBytes());
    }
    //as used in Rows and Prepared responses
    const flags = this.readInt();
    const columnLength = this.readInt();
    if (types.protocolVersion.supportsPreparedPartitionKey(this.header.version) && isPrepared) {
      //read the pk columns
      meta.partitionKeys = new Array(this.readInt());
      for (i = 0; i < meta.partitionKeys.length; i++) {
        meta.partitionKeys[i] = this.readShort();
      }
    }
    if (flags & resultFlag.hasMorePages) {
      meta.pageState = utils.copyBuffer(this.readBytes());
    }
    if (flags & resultFlag.metadataChanged) {
      meta.newResultId = utils.copyBuffer(this.readShortBytes());
    }
    if (flags & resultFlag.continuousPaging) {
      meta.continuousPageIndex = this.readInt();
      meta.lastContinuousPage = !!(flags & resultFlag.lastContinuousPage);
    }
    if (flags & resultFlag.globalTablesSpec) {
      meta.global_tables_spec = true;
      meta.keyspace = this.readString();
      meta.table = this.readString();
    }
    meta.columns = new Array(columnLength);
    meta.columnsByName = utils.emptyObject;
    if (isPrepared) {
      //for prepared metadata, we will need a index of the columns (param) by name
      meta.columnsByName = {};
    }
    for (i = 0; i < columnLength; i++) {
      const col: {
        ksname?: string; tablename?: string;
        name?: string; type?: { code: number; info: any | null };} = {};
      if (!meta.global_tables_spec) {
        col.ksname = this.readString();
        col.tablename = this.readString();
      }
      col.name = this.readString();
      col.type = this.readType();
      meta.columns[i] = col;
      if (isPrepared) {
        meta.columnsByName[col.name] = i;
      }
    }
    return meta;
  }

  /**
   * Reads the error from the frame
   * @throws {RangeError}
   * @returns {ResponseError}
   */
  readError(): ResponseError {
    const code = this.readInt();
    const message = this.readString();
    const err = new errors.ResponseError(code, message);
    //read extra info
    switch (code) {
      case types.responseErrorCodes.unavailableException:
        err.consistencies = this.readShort();
        err.required = this.readInt();
        err.alive = this.readInt();
        err.message = util.format(_unavailableMessage, types.consistencyToString[err.consistencies], err.required, err.alive);
        break;
      case types.responseErrorCodes.readTimeout:
      case types.responseErrorCodes.readFailure:
        err.consistencies = this.readShort();
        err.received = this.readInt();
        err.blockFor = this.readInt();
        if (code === types.responseErrorCodes.readFailure) {
          if (types.protocolVersion.supportsFailureReasonMap(this.header.version)) {
            err.failures = this.readInt();
            err.reasons = this.readMap(err.failures, this.readInetAddress, this.readShort);
          }
          else {
            err.failures = this.readInt();
          }
        }
        err.isDataPresent = this.readByte();
        if (code === types.responseErrorCodes.readTimeout) {
          let details;
          if (err.received < err.blockFor) {
            details = util.format('%d replica(s) responded over %d required', err.received, err.blockFor);
          }
          else if (!err.isDataPresent) {
            details = 'the replica queried for the data didn\'t respond';
          }
          else {
            details = 'timeout while waiting for repair of inconsistent replica';
          }
          err.message = util.format(_readTimeoutMessage, types.consistencyToString[err.consistencies], details);
        }
        else {
          err.message = util.format(_readFailureMessage, types.consistencyToString[err.consistencies], err.blockFor, err.received, err.failures);
        }
        break;
      case types.responseErrorCodes.writeTimeout:
      case types.responseErrorCodes.writeFailure:
        err.consistencies = this.readShort();
        err.received = this.readInt();
        err.blockFor = this.readInt();
        if (code === types.responseErrorCodes.writeFailure) {
          if (types.protocolVersion.supportsFailureReasonMap(this.header.version)) {
            err.failures = this.readInt();
            err.reasons = this.readMap(err.failures, this.readInetAddress, this.readShort);
          }
          else {
            err.failures = this.readInt();
          }
        }
        err.writeType = this.readString();
        if (code === types.responseErrorCodes.writeTimeout) {
          const template = err.writeType === 'BATCH_LOG' ? _writeTimeoutBatchLogMessage : _writeTimeoutQueryMessage;
          err.message = util.format(template, types.consistencyToString[err.consistencies], err.received, err.blockFor);
        }
        else {
          err.message = util.format(_writeFailureMessage, types.consistencyToString[err.consistencies], err.blockFor, err.received, err.failures);
        }
        break;
      case types.responseErrorCodes.unprepared:
        err.queryId = utils.copyBuffer(this.readShortBytes());
        break;
      case types.responseErrorCodes.functionFailure:
        err.keyspace = this.readString();
        err.functionName = this.readString();
        err.argTypes = this.readStringList();
        break;
      case types.responseErrorCodes.alreadyExists: {
        err.keyspace = this.readString();
        const table = this.readString();
        if (table.length > 0) {
          err.table = table;
        }
        break;
      }
    }
    return err;
  }

  /**
   * Reads an event from Cassandra and returns the detail
   * @returns {{eventType: String, inet: {address: InetAddress, port: Number}}, *}
   */
  readEvent(): { eventType: string; inet?: { address: InetAddress; port: number; }; added?: boolean; up?: boolean } {
    const eventType = this.readString();
    switch (eventType) {
      case types.protocolEvents.topologyChange:
        return {
          added: this.readString() === 'NEW_NODE',
          inet: this.readInet(),
          eventType: eventType
        };
      case types.protocolEvents.statusChange:
        return {
          up: this.readString() === 'UP',
          inet: this.readInet(),
          eventType: eventType
        };
      case types.protocolEvents.schemaChange:
        return this.parseSchemaChange();
    }
    //Forward compatibility
    return { eventType: eventType };
  }

  parseSchemaChange() {
    let result;
    if (!types.protocolVersion.supportsSchemaChangeFullMetadata(this.header.version)) {
      //v1/v2: 3 strings, the table value can be empty
      result = {
        eventType: types.protocolEvents.schemaChange,
        schemaChangeType: this.readString(),
        keyspace: this.readString(),
        table: this.readString()
      };
      result.isKeyspace = !result.table;
      return result;
    }
    //v3+: 3 or 4 strings: change_type, target, keyspace and (table, type, functionName or aggregate)
    result = {
      eventType: types.protocolEvents.schemaChange,
      schemaChangeType: this.readString(),
      target: this.readString(),
      keyspace: this.readString(),
      table: null,
      udt: null,
      signature: null
    };
    result.isKeyspace = result.target === 'KEYSPACE';
    switch (result.target) {
      case 'TABLE':
        result.table = this.readString();
        break;
      case 'TYPE':
        result.udt = this.readString();
        break;
      case 'FUNCTION':
        result.functionName = this.readString();
        result.signature = this.readStringList();
        break;
      case 'AGGREGATE':
        result.aggregate = this.readString();
        result.signature = this.readStringList();
    }
    return result;
  }
}

export { FrameReader };
export default { FrameReader };