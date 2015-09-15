var util = require('util');
var stream = require('stream');
// var assert = require('assert');

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
    // console.log( "_read", this.paused, this.buffer.length );
    this._readNext();
    this._readNext = null;
  }
};

/**
 * Allows for throttling, helping nodejs keep the internal buffers reasonably sized.
 * @param {function} function that triggers reading the next result chunk
 * @constructor
 */
ResultStream.prototype._valve = function( readNext ) {
  // assert( !this._readNext );
  this._readNext = null;
  if ( !readNext ) {
    return;
  }
  if ( this.paused || this.buffer.length ) {
    this._readNext = readNext;
  }
  else {
    // console.log( "_valve", this.buffer.length );
    readNext();
  }
};

ResultStream.prototype.add = function (chunk) {
  this.buffer.push(chunk);
  this.read(0);
};

module.exports = ResultStream;