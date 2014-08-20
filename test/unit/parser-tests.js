var assert = require('assert');
var util = require('util');
var async = require('async');

var streams = require('../../lib/streams.js');
var types = require('../../lib/types.js');

/**
 * Tests for the transform streams that are involved in the reading of a response
 */
describe('Parser', function () {
  describe('#_transform()', function () {
    it('should read a READY opcode', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 0);
        assert.strictEqual(item.header.opcode, types.opcodes.ready);
        done();
      });
      parser._transform({header: getFrameHeader(0, types.opcodes.ready), chunk: new Buffer([])}, null, doneIfError(done));
    });

    it('should read a AUTHENTICATE opcode', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authenticate);
        assert.ok(item.mustAuthenticate, 'it should return a mustAuthenticate return flag');
        done();
      });
      parser._transform({header: getFrameHeader(0, types.opcodes.authenticate), chunk: new Buffer([])}, null, doneIfError(done));
    });

    it('should read a VOID result', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: new Buffer([0, 0, 0, types.resultKind.voidResult])
      }, null, doneIfError(done));
    });

    it('should read a buffer until there is enough data', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: new Buffer([0])
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: new Buffer([0, 0, types.resultKind.voidResult])
      }, null, doneIfError(done));
    });

    it('should emit empty result one column no rows', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.result && item.result.rows && item.result.rows.length === 0);
        done();
      });
      //kind
      parser._transform(getBodyChunks(1, 0, 0, 4), null, doneIfError(done));
      //metadata
      parser._transform(getBodyChunks(1, 0, 4, 12), null, doneIfError(done));
      //column names and rows
      parser._transform(getBodyChunks(1, 0, 12, null), null, doneIfError(done));
    });

    it('should emit empty result two columns no rows', function (done) {
      var parser = new streams.Parser({objectMode:true});
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.result && item.result.rows && item.result.rows.length === 0);
        done();
      });
      //2 columns, no rows, in one chunk
      parser._transform(getBodyChunks(2, 0, 0, null), null, doneIfError(done));
    });

    it('should emit row when rows present', function (done) {
      var parser = new streams.Parser({objectMode:true});
      var rowLength = 2;
      var rowCounter = 0;
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.row);
        if ((++rowCounter) === rowLength) {
          done();
        }
      });
      //2 columns, 1 rows
      parser._transform(getBodyChunks(3, rowLength, 0, 10), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 10, 32), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 32, 37), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 37, null), null, doneIfError(done));
    });
  });
});

/**
 * Test Helper method to get a frame header
 * @returns {FrameHeader}
 */
function getFrameHeader(bodyLength, opcode) {
  var header = new types.FrameHeader();
  header.bufferLength = bodyLength + 8;
  header.isResponse = true;
  header.version = 1;
  header.flags = 0;
  header.streamId = 12;
  header.opcode = opcode;
  header.bodyLength = bodyLength;
  return header;
}

function getBodyChunks(columnLength, rowLength, fromIndex, toIndex) {
  var i;
  var fullChunk = [
    //kind
    0, 0, 0, types.resultKind.rows,
    //flags and column count
    0, 0, 0, 1, 0, 0, 0, columnLength,
    //column names
    0, 1, 97, //string 'a' as ksname
    0, 1, 98 //string 'b' as tablename
  ];
  for (i = 0; i < columnLength; i++) {
    fullChunk = fullChunk.concat([
      0, 1, 99 + i, //string name, starting by 'c' as column name
      0, types.dataTypes.text //short datatype
    ]);
  }
  //rows length
  fullChunk = fullChunk.concat([0, 0, 0, rowLength || 0]);
  for (i = 0; i < rowLength; i++) {
    var rowChunk = [];
    for (var j = 0; j < columnLength; j++) {
      //4 bytes length + bytes of each column value
      rowChunk.push(0);
      rowChunk.push(0);
      rowChunk.push(0);
      rowChunk.push(1);
      //value
      rowChunk.push(j);
    }
    fullChunk = fullChunk.concat(rowChunk);
  }

  return {
    header: getFrameHeader(fullChunk.length, types.opcodes.result),
    chunk: new Buffer(fullChunk.slice(fromIndex, toIndex || undefined))
  };
}

/**
 * Calls done in case there is an error
 */
function doneIfError(done) {
  return function (err) {
    if (err) done(err);
  };
}