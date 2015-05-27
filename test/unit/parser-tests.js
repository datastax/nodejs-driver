var assert = require('assert');
var util = require('util');
var async = require('async');

var Encoder = require('../../lib/encoder');
var streams = require('../../lib/streams');
var types = require('../../lib/types');
var helper = require('../test-helper');

/**
 * Tests for the transform streams that are involved in the reading of a response
 */
describe('Parser', function () {
  describe('#_transform()', function () {
    it('should read a READY opcode', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 0);
        assert.strictEqual(item.header.opcode, types.opcodes.ready);
        done();
      });
      parser._transform({header: getFrameHeader(0, types.opcodes.ready), chunk: new Buffer([])}, null, doneIfError(done));
    });
    it('should read a AUTHENTICATE opcode', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authenticate);
        assert.ok(item.mustAuthenticate, 'it should return a mustAuthenticate return flag');
        done();
      });
      parser._transform({header: getFrameHeader(0, types.opcodes.authenticate), chunk: new Buffer([])}, null, doneIfError(done));
    });
    it('should read a VOID result', function (done) {
      var parser = newInstance();
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
    it('should read a VOID result with trace id', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        helper.assertInstanceOf(item.flags.traceId, types.Uuid);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result, 2, true),
        chunk: Buffer.concat([
          new Buffer(16), //uuid
          new Buffer([0, 0, 0, types.resultKind.voidResult])
        ])
      }, null, doneIfError(done));
    });
    it('should read a VOID result with trace id in chunks', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        helper.assertInstanceOf(item.flags.traceId, types.Uuid);
        assert.strictEqual(item.flags.traceId.getBuffer().slice(0, 6).toString('hex'), 'fffffffffafa');
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result, 2, true),
        chunk: new Buffer('fffffffffafa', 'hex') //first part of the uuid
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result, 2, true),
        chunk: Buffer.concat([
          new Buffer(10), //second part uuid
          new Buffer([0, 0, 0, types.resultKind.voidResult])
        ])
      }, null, doneIfError(done));
    });
    it('should read a SET_KEYSPACE result', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.strictEqual(item.keyspaceSet, 'ks1');
        done();
      });
      //kind + stringLength + string
      var bodyLength = 4 + 2 + 3;
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: new Buffer([0, 0, 0, types.resultKind.setKeyspace])
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: new Buffer([0, 3])
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: new Buffer('ks1')
      }, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE UP EVENT response', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, true);
        done();
      });

      var eventData = getEventData('STATUS_CHANGE', 'UP');
      parser._transform(eventData, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE DOWN EVENT response', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, false);
        done();
      });

      var eventData = getEventData('STATUS_CHANGE', 'DOWN');
      parser._transform(eventData, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE DOWN EVENT response chunked', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, false);
        done();
      });

      var eventData = getEventData('STATUS_CHANGE', 'DOWN');
      var chunk1 = eventData.chunk.slice(0, 5);
      var chunk2 = eventData.chunk.slice(5);
      parser._transform({header: eventData.header, chunk: chunk1}, null, doneIfError(done));
      parser._transform({header: eventData.header, chunk: chunk2}, null, doneIfError(done));
    });
    it('should read a buffer until there is enough data', function (done) {
      var parser = newInstance();
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
      var parser = newInstance();
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
      var parser = newInstance();
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
      var parser = newInstance();
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
      //3 columns, 2 rows
      parser._transform(getBodyChunks(3, rowLength, 0, 10), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 10, 32), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 32, 37), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 37, null), null, doneIfError(done));
    });
    it('should emit row with large row values', function (done) {
      //3mb value
      var cellValue = helper.fillArray(3 * 1024 * 1024, 74);
      //Add the length 0x00300000 of the value
      cellValue = [0, 30, 0, 0].concat(cellValue);
      var rowLength = 1;
      async.series([function (next) {
        var parser = newInstance();
        var rowCounter = 0;
        parser.on('readable', function () {
          var item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //1 columns, 1 row, 1 chunk
        parser._transform(getBodyChunks(1, rowLength, 0, null, cellValue), null, doneIfError(done));
      }, function (next) {
        var parser = newInstance();
        var rowCounter = 0;
        parser.on('readable', function () {
          var item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //1 columns, 1 row, 2 chunks
        parser._transform(getBodyChunks(1, rowLength, 0, 50, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 50, null, cellValue), null, doneIfError(done));
      }, function (next) {
        var parser = newInstance();
        var rowCounter = 0;
        parser.on('readable', function () {
          var item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //1 columns, 1 row, 6 chunks
        parser._transform(getBodyChunks(1, rowLength, 0, 50, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 50, 60, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 60, 120, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 120, 195, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 195, 1501, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 1501, null, cellValue), null, doneIfError(done));
      }, function (next) {
        var cellValue = helper.fillArray(256, 74);
        //Add the length 256 of the value
        cellValue = [0, 0, 1, 0].concat(cellValue);
        var parser = newInstance();
        var rowCounter = 0;
        parser.on('readable', function () {
          var item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //1 columns, 1 row, 6 small chunks
        parser._transform(getBodyChunks(1, rowLength, 0, 50, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 50, 100, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 100, 150, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 150, 200, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(1, rowLength, 200, null, cellValue), null, doneIfError(done));
      }, function (next) {
        var cellValue = helper.fillArray(256, 74);
        //Add the length 256 of the value
        cellValue = [0, 0, 1, 0].concat(cellValue);
        var parser = newInstance();
        var rowCounter = 0;
        parser.on('readable', function () {
          var item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //2 columns, 1 row, small and large chunks
        parser._transform(getBodyChunks(2, rowLength, 0, 19, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(2, rowLength, 19, 20, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(2, rowLength, 20, 24, cellValue), null, doneIfError(done));
        parser._transform(getBodyChunks(2, rowLength, 24, null, cellValue), null, doneIfError(done));
      }], done);
    });
    it('should read a AUTH_CHALLENGE response', function (done) {
      var parser = newInstance();
      parser.on('readable', function () {
        var item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authChallenge);
        helper.assertValueEqual(item.token, new Buffer([100, 100]));
        assert.strictEqual(item.authChallenge, true);
        done();
      });
      //Length + buffer
      var bodyLength = 4 + 2;
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.authChallenge),
        chunk: new Buffer([0, 0, 0, 2])
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.authChallenge),
        chunk: new Buffer([100, 100])
      }, null, doneIfError(done));
    });
  });
});

/**
 * @param {Number} [protocolVersion]
 * @returns {exports.Parser}
 */
function newInstance(protocolVersion) {
  if (!protocolVersion) {
    protocolVersion = 2;
  }
  return new streams.Parser({objectMode:true}, new Encoder(protocolVersion, {}));
}

/**
 * Test Helper method to get a frame header
 * @returns {exports.FrameHeader}
 */
function getFrameHeader(bodyLength, opcode, version, trace) {
  return new types.FrameHeader(version || 2, trace ? 0x02 : 0, 12, opcode, bodyLength);
}

function getBodyChunks(columnLength, rowLength, fromIndex, toIndex, cellValue) {
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
      if (!cellValue) {
        rowChunk.push(0);
        rowChunk.push(0);
        rowChunk.push(0);
        rowChunk.push(1);
        //value
        rowChunk.push(j);
      }
      else {
        rowChunk = rowChunk.concat(cellValue);
      }
    }
    fullChunk = fullChunk.concat(rowChunk);
  }

  return {
    header: getFrameHeader(fullChunk.length, types.opcodes.result),
    chunk: new Buffer(fullChunk.slice(fromIndex, toIndex || undefined))
  };
}

function getEventData(eventType, value) {
  var bodyArray = [];
  //EVENT TYPE
  bodyArray.push(new Buffer([0, eventType.length]));
  bodyArray.push(new Buffer(eventType));
  //STATUS CHANGE DESCRIPTION
  bodyArray.push(new Buffer([0, value.length]));
  bodyArray.push(new Buffer(value));
  //Address
  bodyArray.push(new Buffer([4, 127, 0, 0, 1]));
  //Port
  bodyArray.push(new Buffer([0, 0, 0, 200]));

  var body = Buffer.concat(bodyArray);
  var header = new types.FrameHeader(2, 0, -1, types.opcodes.event, body.length);
  return {header: header, chunk: body};
}

/**
 * Calls done in case there is an error
 */
function doneIfError(done) {
  return function (err) {
    if (err) done(err);
  };
}
