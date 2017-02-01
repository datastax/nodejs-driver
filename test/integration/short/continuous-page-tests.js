/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var helper = require('../../test-helper');
var util = require('util');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');
var types = require('../../../lib/types');
var vdescribe = helper.vdescribe;

vdescribe('dse-5.1', 'Continuous paging', function () {
  var totalRows = 100000;
  this.timeout(120000);
  var setupInfo = helper.setup(1, {
    queries: [
      'CREATE TABLE t1 (id int PRIMARY KEY, value blob)',
      'CREATE TABLE t2 (id int PRIMARY KEY, value blob)',
      'CREATE TABLE empty_table (id int PRIMARY KEY, value blob)',
    ]
  });
  var client = setupInfo.client;
  before(insertData(client, 't1', totalRows, 16));
  context('with #stream()', function () {
    afterEach(executeSomeQueries(client));
    it('should emit end when no rows', function (done) {
      var stream = client.stream('SELECT id FROM empty_table', null, { prepare: true, continuousPaging: {} });
      stream
        .on('end', done)
        .on('readable', function () {
          // if there is a event emitted (Node.js 0.12), stream.read() is null
          assert.strictEqual(stream.read(), null);
        })
        .on('error', helper.throwop);
    });
    it('should emit error when there is a syntax error', function (done) {
      var stream = client.stream('SELECT WITH SYNTAX ERROR', null, { continuousPaging: {} });
      var errorCalled = 0;
      stream
        .on('end', function () {
          assert.strictEqual(errorCalled, 1);
          done();
        })
        .on('readable', function () {
          // Node.js 0.12, it emits a null value, causing the rest of the events to chain
          assert.strictEqual(stream.read(), null);
        })
        .on('error', function (err) {
          assert.ok(err, 'It should yield an error');
          helper.assertInstanceOf(err, errors.ResponseError);
          errorCalled++;
        });
    });
    var options = [
      // a nice even number
      ['pageSize=10000', { pageSize: 10000 }],
      // large enough value to cause hitting watermark
      ['pageSize=20000', { pageSize: 20000 }],
      // an uneven number to cause a small amount of left over rows for the last page.
      ['pageSize=4999', { pageSize: 4999 }],
      // limit pages per second to 5, with 100000 rows, this should take 100000/5000/5 (4) seconds.
      ['pageSize=5000, maxPagesPerSecond=5', { pageSize: 5000, maxPagesPerSecond: 5 }, totalRows, 4000],
      // 15 * 1000 = 15000 rows
      ['pageSize=1000, pageUnit=rows, maxPages=15', { pageSize: 1000, pageUnit: 'rows', maxPages: 15}, 15000],
      // 4 (int len) + 16 byte blob = 20 bytes per row.
      // 1000/20 (500) rows per page.
      ['pageSize=1000, pageUnit=bytes', { pageSize: 1000, pageUnit: 'bytes'}],
      // 80/20 (4) rows in a single page.
      ['pageSize=80, pageUnit=bytes, maxPages=1', { pageSize: 80, pageUnit: 'bytes', maxPages: 1}, 4],
      // 2000/20 (100) rows for 10 total pages = 1000.
      ['pageSize=2000, pageUnit=bytes, maxPages=10', { pageSize: 2000, pageUnit: 'bytes', maxPages: 10}, 1000],
      // 1 page
      ['pageSize=totalRows', { pageSize: totalRows }],
      // 1 remaining row.
      ['pageSize=totalRows-1', { pageSize: totalRows-1 }],
    ];
    describe('should retrieve all the requested pages with continuousPaging options', function() {
      options.forEach(function (config) {
        var title = config[0];
        var cOpt = config[1];
        var expectedRows = config[2] || totalRows;
        var expectedElapsed = config[3];
        it(title, function (done) {
          var start;
          if (expectedElapsed) {
            start = process.hrtime();
          }
          var rowCount = 0;
          client.stream('SELECT value FROM t1', null, { prepare: true, continuousPaging: cOpt })
            .on('end', function () {
              assert.strictEqual(rowCount, expectedRows);
              if (expectedElapsed) {
                var hr = process.hrtime(start);
                var elapsed = (hr[0] * 1000) + (hr[1] / 1000000);
                assert.ok(elapsed > expectedElapsed, 'Expected to take ' + expectedElapsed + 'ms, took ' + elapsed + 'ms.');
              }
              done();
            })
            .on('data', function (row) {
              rowCount++;
              helper.assertInstanceOf(row, types.Row);
            })
            .on('error', helper.throwop);
        });
      });
    });
    it('should allow sync calls to cancel', function (done) {
      var rowCount = 0;
      var wasCancelled;
      client.stream('SELECT id, value FROM t1', null, { prepare: true, continuousPaging: { pageSize: 100 }})
        .on('end', function () {
          assert.strictEqual(typeof wasCancelled, 'boolean');
          if (wasCancelled) {
            // The CANCEL request was sent after the query, the continuous paging was cancelled
            assert.ok(rowCount < totalRows);
          }
          else {
            // The CANCEL request was not effective, all the rows should be retrieved
            assert.ok(rowCount, totalRows);
          }
          done();
        })
        .on('data', function (row) {
          rowCount++;
          helper.assertInstanceOf(row, types.Row);
        })
        .on('error', helper.throwop)
        .cancel(function (err, cancelled) {
          assert.ifError(err);
          wasCancelled = cancelled;
        });
    });
    it('should allow calls to cancel after the first row has been received', function (done) {
      var rowCount = 0;
      var stream = client.stream('SELECT id, value FROM t1', null, { prepare: true, continuousPaging: { pageSize: 10 }})
        .on('end', function () {
          assert.ok(rowCount > 0);
          assert.ok(rowCount < totalRows);
          done();
        })
        .on('data', function (row) {
          if (rowCount++ === 0) {
            stream.cancel();
          }
          helper.assertInstanceOf(row, types.Row);
        })
        .on('error', helper.throwop);
    });
    it('should allow calls to cancel after all rows have been received', function (done) {
      var rowCount = 0;
      var stream = client.stream('SELECT id, value FROM t1', null, { prepare: true, continuousPaging: { }})
        .on('end', function () {
          assert.strictEqual(rowCount, totalRows);
          assert.strictEqual(stream.cancel, utils.callbackNoop);
          done();
        })
        .on('data', function () {
          rowCount++;
        })
        .on('error', helper.throwop);
    });
    it('should pause reading from the socket when not reading', function (done) {
      var highWaterMarkRows = 10000;
      var options = {
        prepare: true,
        continuousPaging: { pageSize: 100, highWaterMarkRows: highWaterMarkRows }
      };
      var limit = 6000;
      var rowCount = 0;
      var stream;
      function checkInternalBuffer() {
        if (!stream || stream.buffer.length < highWaterMarkRows) {
          // Check again in a few ms
          return setTimeout(checkInternalBuffer, 50);
        }
        if (stream.buffer.length >= highWaterMarkRows) {
          return setTimeout(function checkBufferAfterAWhile() {
            // After a second, the internal buffer should not grow outside bounds
            var aboveHighWaterMark = stream.buffer.length - highWaterMarkRows;
            // Under normal circumstances and without implementing stream pausing on ResultStream, all the
            // rows should be buffered, a few thousand rows can overflow since socket.pause() and parsing
            assert.ok(aboveHighWaterMark < limit, 'Expected below ' + limit + ' rows, obtained ' + aboveHighWaterMark);
            stream.resume();
          }, 1000);
        }
        assert.fail(stream.buffer.length, highWaterMarkRows, null, '<=');
      }
      stream = client.stream('SELECT id, value FROM t1', null, options)
        .on('end', function () {
          assert.strictEqual(rowCount, totalRows);
          done();
        })
        .on('data', function (row) {
          assert.ok(row);
          rowCount++;
        })
        .on('error', helper.throwop);
      stream.pause();
      checkInternalBuffer();
    });
    it('should error when server raises client write exception during paging', function (done) {
      var test = this;
      var highWaterMarkRows = 10000;
      var options = {
        prepare: true,
        continuousPaging: { pageSize: 100, highWaterMarkRows: highWaterMarkRows }
      };
      var rowCount = 0;
      var stream;
      function checkInternalBuffer() {
        if (!stream || stream.buffer.length < highWaterMarkRows) {
          // Check again in a few ms
          return setTimeout(checkInternalBuffer, 50);
        }
        if (stream.buffer.length >= highWaterMarkRows) {
          // defer streaming for 10 seconds, which should cause DSE to sent a client write exception.
          return setTimeout(function resumeStream() {
            stream.resume();
          }, 10000);
        }
        assert.fail(stream.buffer.length, highWaterMarkRows, null, '<=');
      }
      var error;
      stream = client.stream('SELECT id, value FROM t1', null, options)
        .on('end', function () {
          if (error) {
            helper.trace('Error was encountered as expected, verifying partial results');
            assert.ok(rowCount >= highWaterMarkRows, "Got " + rowCount + " expected at least " + highWaterMarkRows);
            assert.ok(rowCount < totalRows, "Got " + rowCount + " expected less than " + totalRows);
            assert.strictEqual(error.message, 'Timed out adding page to output queue');
            assert.strictEqual(error.code, types.responseErrorCodes.clientWriteFailure);
            done();
          }
          else {
            helper.trace('Error was not raised, TCP window scaling is enabled or window ' +
              'is larger than default, marking test as skipped.');
            assert.strictEqual(rowCount, totalRows);
            test.skip();
          }
        })
        .on('data', function (row) {
          assert.ok(row);
          rowCount++;
        })
        .on('error', function (err) {
          error = err;
        });
      stream.pause();
      checkInternalBuffer();
    });
  });
  context('with #eachRow()', function () {
    it('should not allow continuousPaging options', function (done) {
      client.eachRow('SELECT id FROM empty_table', null, { continuousPaging: {} }, helper.throwop, function (err) {
        helper.assertInstanceOf(err, errors.NotSupportedError);
        helper.assertContains(err.message, 'Continuous paging');
        done();
      });
    });
  });
  context('with #execute()', function () {
    it('should not allow continuousPaging options', function (done) {
      var options = { continuousPaging: { pageSize: 10 }, fetchSize: 5};
      client.execute('SELECT id, value FROM t1', null, options, function (err) {
        helper.assertInstanceOf(err, errors.NotSupportedError);
        helper.assertContains(err.message, 'Continuous paging');
        done();
      });
    });
  });
});

function executeSomeQueries(client) {
  // Insert few rows in a dummy table
  return insertData(client, 't2', 20, 10);
}

function insertData(client, tableName, rowLength, valueLength) {
  return (function insertDataAsync(done) {
    var query = util.format('INSERT INTO %s (id, value) VALUES (?, ?)', tableName);
    utils.timesLimit(rowLength, 100, function (n, next) {
      client.execute(query, [ n, utils.allocBufferUnsafe(valueLength) ], { prepare: true }, next);
    }, done);
  });
}