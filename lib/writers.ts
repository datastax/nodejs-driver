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
import events from "events";
import types, { Long } from "./types/index";
import utils from "./utils";
import { Socket } from "net";
import Encoder from "./encoder";
import { ClientOptions } from "./client";
import OperationState from "./operation-state";
import errors, { DriverError } from "./errors";


const FrameHeader = types.FrameHeader;

/**
 * Contains the logic to write all the different types to the frame.
 */
class FrameWriter {
  bodyLength: any;
  buffers: any[];
  opcode: number;
  /**
   * Creates a new instance of FrameWriter.
   * @param {Number} opcode
   */
  constructor(opcode: number) {
    if (!opcode) {
      throw new Error('Opcode not provided');
    }
    this.buffers = [];
    this.opcode = opcode;
    this.bodyLength = 0;
  }

  add(buf) {
    this.buffers.push(buf);
    this.bodyLength += buf.length;
  }

  writeShort(num) {
    const buf = utils.allocBufferUnsafe(2);
    buf.writeUInt16BE(num, 0);
    this.add(buf);
  }

  writeInt(num) {
    const buf = utils.allocBufferUnsafe(4);
    buf.writeInt32BE(num, 0);
    this.add(buf);
  }

  /** @param {Long} num */
  writeLong(num: Long) {
    this.add(Long["toBuffer"](num));
  }

  /**
   * Writes bytes according to Cassandra <int byteLength><bytes>
   * @param {Buffer|null|types.unset} bytes
   */
  writeBytes(bytes: Buffer | null | typeof types.unset) {
    if (bytes === null) {
      //Only the length buffer containing -1
      this.writeInt(-1);
      return;
    }
    if (bytes === types.unset) {
      this.writeInt(-2);
      return;
    }
    //Add the length buffer
    this.writeInt((bytes as Buffer).length);
    //Add the actual buffer
    this.add(bytes);
  }

  /**
   * Writes a buffer according to Cassandra protocol: bytes.length (2) + bytes
   * @param {Buffer} bytes
   */
  writeShortBytes(bytes: Buffer) {
    if(bytes === null) {
      //Only the length buffer containing -1
      this.writeShort(-1);
      return;
    }
    //Add the length buffer
    this.writeShort(bytes.length);
    //Add the actual buffer
    this.add(bytes);
  }

  /**
   * Writes a single byte
   * @param {Number} num Value of the byte, a number between 0 and 255.
   */
  writeByte(num: number) {
    this.add(utils.allocBufferFromArray([num]));
  }

  writeString(str) {
    if (typeof str === "undefined") {
      throw new Error("can not write undefined");
    }
    const len = Buffer.byteLength(str, 'utf8');
    const buf = utils.allocBufferUnsafe(2 + len);
    buf.writeUInt16BE(len, 0);
    buf.write(str, 2, buf.length-2, 'utf8');
    this.add(buf);
  }

  writeLString(str) {
    const len = Buffer.byteLength(str, 'utf8');
    const buf = utils.allocBufferUnsafe(4 + len);
    buf.writeInt32BE(len, 0);
    buf.write(str, 4, buf.length-4, 'utf8');
    this.add(buf);
  }

  writeStringList(values) {
    this.writeShort(values.length);
    values.forEach(this.writeString, this);
  }

  writeCustomPayload(payload) {
    const keys = Object.keys(payload);
    this.writeShort(keys.length);
    keys.forEach(k => {
      this.writeString(k);
      this.writeBytes(payload[k]);
    });
  }

  writeStringMap(map) {
    const keys = [];
    for (const k in map) {
      if (map.hasOwnProperty(k)) {
        keys.push(k);
      }
    }

    this.writeShort(keys.length);

    for(let i = 0; i < keys.length; i++) {
      const key = keys[i];
      this.writeString(key);
      this.writeString(map[key]);
    }
  }

  /**
   * @param {Number} version
   * @param {Number} streamId
   * @param {Number} [flags] Header flags
   * @returns {Buffer}
   * @throws {TypeError}
   */
  write(version: number, streamId: number, flags?: number): Buffer {
    const header = new FrameHeader(version, flags || 0, streamId, this.opcode, this.bodyLength);
    const headerBuffer = header.toBuffer();
    this.buffers.unshift(headerBuffer);
    return Buffer.concat(this.buffers, headerBuffer.length + this.bodyLength);
  }
}

/**
 * Represents a queue that process one write at a time (FIFO).
 * @extends {EventEmitter}
 */
class WriteQueue extends events.EventEmitter {
  netClient: Socket;
  encoder: Encoder;
  isRunning: boolean;
  queue: any[];
  coalescingThreshold: any;
  error: DriverError;
  canWrite: boolean;
  /**
   * Creates a new WriteQueue instance.
   * @param {Socket} netClient
   * @param {Encoder} encoder
   * @param {ClientOptions} options
   */
  constructor(netClient: Socket, encoder: Encoder, options: ClientOptions) {
    super();
    this.netClient = netClient;
    this.encoder = encoder;
    this.isRunning = false;
    /** @type {Array<{operation: OperationState, callback: Function}>} */
    this.queue = [];
    this.coalescingThreshold = options.socketOptions.coalescingThreshold;
    this.error = null;
    this.canWrite = true;

    // Listen to drain event that is going to be fired once
    // the underlying buffer is empty
    netClient.on('drain', () => {
      this.canWrite = true;
      this.run();
    });
  }

  /**
   * Enqueues a new request
   * @param {OperationState} operation
   * @param {Function} callback The write callback.
   */
  push(operation: OperationState, callback: Function) {
    const self = this;

    if (this.error) {
      // There was a write error, there is no point in further trying to write to the socket.
      return process.nextTick(function writePushError() {
        callback(self.error);
      });
    }

    this.queue.push({ operation: operation, callback: callback});
    this.run();
  }

  run() {
    if (!this.isRunning && this.canWrite) {
      this.isRunning = true;
      // Use nextTick to allow the queue to build up on the current phase
      process.nextTick(() => this.process());
    }
  }

  process() {
    if (this.error) {
      return;
    }

    const buffers = [];
    const callbacks = [];
    let totalLength = 0;

    while (this.queue.length > 0 && totalLength < this.coalescingThreshold) {
      const writeItem = this.queue.shift();
      if (!writeItem.operation.canBeWritten()) {
        // Invoke the write callback with an error that is not going to be yielded to user
        // as the operation has timed out or was cancelled.
        writeItem.callback(new Error('The operation was already cancelled or timeout elapsed'));
        continue;
      }
      let data;
      try {
        data = writeItem.operation.request.write(this.encoder, writeItem.operation.streamId);
      }
      catch (err) {
        writeItem.callback(err);
        continue;
      }
      totalLength += data.length;
      buffers.push(data);
      callbacks.push(writeItem.callback);
    }

    if (totalLength === 0) {
      this.isRunning = false;
      return;
    }

    // We have to invoke the callbacks to avoid race conditions.
    // There is a performance benefit from executing all of them in a loop
    for (let i = 0; i < callbacks.length; i++) {
      callbacks[i]();
    }

    // Concatenate buffers and write it to the socket
    // Further writes will be throttled until flushed
    this.canWrite = this.netClient.write(Buffer.concat(buffers, totalLength), err => {
      if (err) {
        this.setWriteError(err);
        return;
      }

      if (this.queue.length === 0 || !this.canWrite) {
        // It will start running once we get the next message or has drained
        this.isRunning = false;
        return;
      }

      // Allow IO between writes
      setImmediate(() => this.process());
    });
  }

  /**
   * Emits the 'error' event and callbacks items that haven't been written and clears them from the queue.
   * @param err
   */
  setWriteError(err) {
    err.isSocketError = true;
    this.error = new types.DriverError('Socket was closed');
    this.error.isSocketError = true;
    // Use an special flag for items that haven't been written
    this.error.requestNotWritten = true;
    this.error.innerError = err;
    const q = this.queue;
    // Not more items can be added to the queue.
    this.queue = utils.emptyArray as any[];
    for (let i = 0; i < q.length; i++) {
      const item = q[i];
      // Use the error marking that it was not written
      item.callback(this.error);
    }
  }
}

export { FrameWriter, WriteQueue };