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

'use strict';
const util = require('util');
const stream = require('stream');

/** @module types */
/**
 * Readable stream using to yield data from a result or a field
 * @constructor
 */
function ResultStream(opt) {
  stream.Readable.call(this, opt);
  this.buffer = [];
  this.paused = true;
}

util.inherits(ResultStream, stream.Readable);

ResultStream.prototype._read = function() {
  this.paused = false;
  if (this.buffer.length === 0) {
    this._readableState.reading = false;
  }
  while (!this.paused && this.buffer.length > 0) {
    this.paused = !this.push(this.buffer.shift());
  }
  if ( !this.paused && !this.buffer.length && this._readNext ) {
    this._readNext();
    this._readNext = null;
  }
};

/**
 * Allows for throttling, helping nodejs keep the internal buffers reasonably sized.
 * @param {function} readNext function that triggers reading the next result chunk
 */
ResultStream.prototype._valve = function( readNext ) {
  this._readNext = null;
  if ( !readNext ) {
    return;
  }
  if ( this.paused || this.buffer.length ) {
    this._readNext = readNext;
  }
  else {
    readNext();
  }
};

ResultStream.prototype.add = function (chunk) {
  this.buffer.push(chunk);
  this.read(0);
};

module.exports = ResultStream;