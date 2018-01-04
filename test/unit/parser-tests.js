/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');

const Encoder = require('../../lib/encoder');
const streams = require('../../lib/streams');
const errors = require('../../lib/errors');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const helper = require('../test-helper');

/**
 * Tests for the transform streams that are involved in the reading of a response
 */
describe('Parser', function () {
  describe('#_transform()', function () {
    it('should read a READY opcode', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.bodyLength, 0);
        assert.strictEqual(item.header.opcode, types.opcodes.ready);
        done();
      });
      parser._transform({header: getFrameHeader(0, types.opcodes.ready), chunk: utils.allocBufferFromArray([])}, null, doneIfError(done));
    });
    it('should read a AUTHENTICATE response', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authenticate);
        assert.ok(item.mustAuthenticate, 'it should return a mustAuthenticate return flag');
        done();
      });
      parser._transform({ header: getFrameHeader(2, types.opcodes.authenticate), chunk: utils.allocBufferFromArray([0, 0])}, null, doneIfError(done));
    });
    it('should buffer a AUTHENTICATE response until complete', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authenticate);
        assert.ok(item.mustAuthenticate, 'it should return a mustAuthenticate return flag');
        assert.strictEqual(item.authenticatorName, 'abc');
        //mocha will fail if done is called multiple times
        done();
      });
      const header = getFrameHeader(5, types.opcodes.authenticate);
      parser._transform({ header: header, chunk: utils.allocBufferFromArray([0, 3]), offset: 0}, null, doneIfError(done));
      parser._transform({ header: header, chunk: utils.allocBufferFromString('a'), offset: 0}, null, doneIfError(done));
      parser._transform({ header: header, chunk: utils.allocBufferFromString('bc'), offset: 0}, null, doneIfError(done));
    });
    it('should read a VOID result', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: utils.allocBufferFromArray([0, 0, 0, types.resultKind.voidResult])
      }, null, doneIfError(done));
    });
    it('should read a VOID result with trace id', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        helper.assertInstanceOf(item.flags.traceId, types.Uuid);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result, 2, true),
        chunk: Buffer.concat([
          utils.allocBufferUnsafe(16), //uuid
          utils.allocBufferFromArray([0, 0, 0, types.resultKind.voidResult])
        ])
      }, null, doneIfError(done));
    });
    it('should read a VOID result with trace id chunked', function (done) {
      const parser = newInstance();
      let responseCounter = 0;
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        responseCounter++;
      });

      const body = Buffer.concat([
        utils.allocBufferUnsafe(16), //uuid
        utils.allocBufferFromArray([0, 0, 0, types.resultKind.voidResult])
      ]);
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result, 2, true),
        chunk: body
      }, null, doneIfError(done));
      assert.strictEqual(responseCounter, 1);
      parser.setOptions(88, { byRow: true });
      for (let i = 0; i < body.length; i++) {
        parser._transform({
          header: getFrameHeader(4, types.opcodes.result, 2, true, 88),
          chunk: body.slice(i, i + 1),
          offset: 0
        }, null, doneIfError(done));
      }
      assert.strictEqual(responseCounter, 2);
      done();
    });
    it('should read a RESULT result with trace id chunked', function (done) {
      const parser = newInstance();
      let responseCounter = 0;
      var byRowCompleted = false;
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        byRowCompleted = item.byRowCompleted;
        responseCounter++;
      });

      const body = Buffer.concat([
        utils.allocBufferUnsafe(16), //uuid
        getBodyChunks(3, 1, 0, undefined, null).chunk
      ]);
      parser._transform({
        header: getFrameHeader(body.length, types.opcodes.result, 2, true),
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
      assert.strictEqual(responseCounter, 1);
      assert.notEqual(byRowCompleted, true);
      parser.setOptions(88, { byRow: true });
      for (let i = 0; i < body.length; i++) {
        parser._transform({
          header: getFrameHeader(4, types.opcodes.result, 2, true, 88),
          chunk: body.slice(i, i + 1),
          offset: 0
        }, null, doneIfError(done));
      }
      assert.strictEqual(responseCounter, 3);
      assert.ok(byRowCompleted);
      done();
    });
    it('should read a VOID result with warnings and custom payload', function (done) {
      const parser = newInstance();

      const body = Buffer.concat([
        // 2 string list of warnings containing 'Hello', 'World'
        utils.allocBufferFromString('0002000548656c6c6f0005576f726c64', 'hex'),
        // Custom payload byte map of {a: 1, b: 2}
        utils.allocBufferFromString('000200016100000001010001620000000102', 'hex'),
        // void result indicator
        utils.allocBufferFromArray([0, 0, 0, types.resultKind.voidResult])
      ]);

      parser.on('readable', function () {
        const item = parser.read();
        assert.ok(!item.error);
        assert.strictEqual(item.header.bodyLength, body.length);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.flags);
        assert.ok(item.flags.warnings);
        assert.deepEqual(item.flags.warnings, ['Hello', 'World']);
        assert.ok(item.flags.customPayload);
        assert.deepEqual(item.flags.customPayload, {a: utils.allocBufferFromArray([0x01]), b: utils.allocBufferFromArray([0x02])});
        done();
      });

      const header = getFrameHeader(body.length, types.opcodes.result, 4, false, 12, true, true);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a SET_KEYSPACE result', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.strictEqual(item.keyspaceSet, 'ks1');
        done();
      });
      //kind + stringLength + string
      const bodyLength = 4 + 2 + 3;
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: utils.allocBufferFromArray([0, 0, 0, types.resultKind.setKeyspace]),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: utils.allocBufferFromArray([0, 3]),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: utils.allocBufferFromString('ks1'),
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a PREPARE result', function (done) {
      const parser = newInstance();
      const id = types.Uuid.random();
      parser.on('readable', function () {
        const item = parser.read();
        assert.ifError(item.error);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        helper.assertInstanceOf(item.id, Buffer);
        assert.strictEqual(item.id.toString('hex'), id.getBuffer().toString('hex'));
        done();
      });
      //kind +
      // id length + id
      // metadata (flags + columnLength + ksname + tblname + column name + column type) +
      // result metadata (flags + columnLength + ksname + tblname + column name + column type)
      const body = Buffer.concat([
        utils.allocBufferFromArray([0, 0, 0, types.resultKind.prepared]),
        utils.allocBufferFromArray([0, 16]),
        id.getBuffer(),
        utils.allocBufferFromArray([0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 62, 0, 1, 63, 0, 1, 61, 0, types.dataTypes.text]),
        utils.allocBufferFromArray([0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 62, 0, 1, 63, 0, 1, 61, 0, types.dataTypes.text])
      ]);
      const bodyLength = body.length;
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: body.slice(0, 22),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: body.slice(22, 41),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.result),
        chunk: body.slice(41),
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE UP EVENT response', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, true);
        done();
      });

      const eventData = getEventData('STATUS_CHANGE', 'UP');
      parser._transform(eventData, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE DOWN EVENT response', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, false);
        done();
      });

      const eventData = getEventData('STATUS_CHANGE', 'DOWN');
      parser._transform(eventData, null, doneIfError(done));
    });
    it('should read a STATUS_CHANGE DOWN EVENT response chunked', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.event);
        assert.ok(item.event, 'it should return the details of the event');
        assert.strictEqual(item.event.up, false);
        done();
      });

      const eventData = getEventData('STATUS_CHANGE', 'DOWN');
      const chunk1 = eventData.chunk.slice(0, 5);
      const chunk2 = eventData.chunk.slice(5);
      parser._transform({header: eventData.header, chunk: chunk1, offset: 0}, null, doneIfError(done));
      parser._transform({header: eventData.header, chunk: chunk2, offset: 0}, null, doneIfError(done));
    });
    it('should read an ERROR response that includes warnings', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.error);
        assert.ok(item.error);
        helper.assertInstanceOf(item.error, errors.ResponseError);
        assert.strictEqual(item.error.message, "Fail");
        assert.strictEqual(item.error.code, 0); // Server Error
        done();
      });

      const body = Buffer.concat([
        utils.allocBufferFromString('0002000548656c6c6f0005576f726c64', 'hex'), // 2 string list of warnings containing 'Hello', 'World'
        utils.allocBufferFromString('0000000000044661696c', 'hex') // Server Error Code (0x0000) with 4 length message 'Fail'
      ]);
      const bodyLength = body.length;
      const header = getFrameHeader(bodyLength, types.opcodes.error, 4, false, 12, true);
      parser._transform({
        header: header,
        chunk: body.slice(0, 4),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: header,
        chunk: body.slice(4, 10),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: header,
        chunk: body.slice(10),
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read an UNAVAILABLE', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.unavailableException);
        assert.strictEqual(msg.error.consistencies, types.consistencies.localQuorum);
        assert.strictEqual(msg.error.required, 5);
        assert.strictEqual(msg.error.alive, 4);
        assert.strictEqual(msg.error.message, 'Not enough replicas available for query at consistency LOCAL_QUORUM (5 required but only 4 alive)');
        done();
      });

      // Unavailable at LOCAL_QUORUM with 5 required and 4 alive.
      const bodyArray = [];
      // Unavailable (0x1000)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x10, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // LOCAL_QUORUM, with 5 required and 4 alive.
      bodyArray.push(utils.allocBufferFromArray([0, 6, 0, 0, 0, 5, 0, 0, 0, 4]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a READ_TIMEOUT with not enough received', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.readTimeout);
        assert.strictEqual(msg.error.consistencies, types.consistencies.two);
        assert.strictEqual(msg.error.received, 1);
        assert.strictEqual(msg.error.blockFor, 2);
        assert.strictEqual(msg.error.isDataPresent, 0);
        assert.strictEqual(msg.error.message, 'Server timeout during read query at consistency TWO (1 replica(s) responded over 2 required)');
        done();
      });

      // Read Timeout at TWO with 1 received 2 block for and no data present
      const bodyArray = [];
      // Read Timeout (0x1200)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x12, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // TWO, 1 received, 2 block for, no data
      bodyArray.push(utils.allocBufferFromArray([0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a READ_TIMEOUT with no data present', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.readTimeout);
        assert.strictEqual(msg.error.consistencies, types.consistencies.two);
        assert.strictEqual(msg.error.received, 2);
        assert.strictEqual(msg.error.blockFor, 2);
        assert.strictEqual(msg.error.isDataPresent, 0);
        assert.strictEqual(msg.error.message, 'Server timeout during read query at consistency TWO (the replica queried for the data didn\'t respond)');
        done();
      });

      // Read Timeout at TWO with 2 received 2 block for and no data present
      const bodyArray = [];
      // Read Timeout (0x1200)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x12, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // TWO, 2 received, 2 block for, no data
      bodyArray.push(utils.allocBufferFromArray([0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a READ_TIMEOUT with repair timeout', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.readTimeout);
        assert.strictEqual(msg.error.consistencies, types.consistencies.two);
        assert.strictEqual(msg.error.received, 2);
        assert.strictEqual(msg.error.blockFor, 2);
        assert.strictEqual(msg.error.isDataPresent, 1);
        assert.strictEqual(msg.error.message, 'Server timeout during read query at consistency TWO (timeout while waiting for repair of inconsistent replica)');
        done();
      });

      // Read Timeout at TWO with 2 received 2 block for and no data present
      const bodyArray = [];
      // Read Timeout (0x1200)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x12, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // TWO, 2 received, 2 block for, data present
      bodyArray.push(utils.allocBufferFromArray([0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 1]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a READ_FAILURE', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.readFailure);
        assert.strictEqual(msg.error.consistencies, types.consistencies.eachQuorum);
        assert.strictEqual(msg.error.received, 3);
        assert.strictEqual(msg.error.blockFor, 5);
        assert.strictEqual(msg.error.failures, 2);
        assert.strictEqual(msg.error.isDataPresent, 1);
        assert.strictEqual(msg.error.message, 'Server failure during read query at consistency EACH_QUORUM (5 responses were required but only 3 replicas responded, 2 failed)');
        done();
      });

      // Read Timeout at TWO with 2 received 2 block for and no data present
      const bodyArray = [];
      // Read Failure (0x1300)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x13, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // EACH_QUORUM, 3 received, 5 block for, 2 failures, data present
      bodyArray.push(utils.allocBufferFromArray([0, 7, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 2, 1]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a SIMPLE WRITE_TIMEOUT', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.writeTimeout);
        assert.strictEqual(msg.error.consistencies, types.consistencies.quorum);
        assert.strictEqual(msg.error.received, 1);
        assert.strictEqual(msg.error.blockFor, 3);
        assert.strictEqual(msg.error.writeType, 'SIMPLE');
        assert.strictEqual(msg.error.message, 'Server timeout during write query at consistency QUORUM (1 peer(s) acknowledged the write over 3 required)');
        done();
      });

      // write timeout at consistency quorum with 1 of 3 replicas responding.
      const bodyArray = [];
      // Write Timeout (0x1100)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x11, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // Quorum, with 1 received and 3 block for
      bodyArray.push(utils.allocBufferFromArray([0, 4, 0, 0, 0, 1, 0, 0, 0, 3]));
      // Write Type 'SIMPLE'
      bodyArray.push(utils.allocBufferFromArray([0, 'SIMPLE'.length]));
      bodyArray.push(utils.allocBufferFromString('SIMPLE'));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a BATCH_LOG WRITE_TIMEOUT', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.writeTimeout);
        assert.strictEqual(msg.error.consistencies, types.consistencies.one);
        assert.strictEqual(msg.error.received, 0);
        assert.strictEqual(msg.error.blockFor, 1);
        assert.strictEqual(msg.error.writeType, 'BATCH_LOG');
        assert.strictEqual(msg.error.message, 'Server timeout during batchlog write at consistency ONE (0 peer(s) acknowledged the write over 1 required)');
        done();
      });

      // batchlog write timeout at consistency quorum with 0 of 1 replicas responding.
      const bodyArray = [];
      // Write Timeout (0x1100)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x11, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // ONE, with 0 received and 1 block for
      bodyArray.push(utils.allocBufferFromArray([0, 1, 0, 0, 0, 0, 0, 0, 0, 1]));
      // Write Type 'BATCH_LOG'
      bodyArray.push(utils.allocBufferFromArray([0, 'BATCH_LOG'.length]));
      bodyArray.push(utils.allocBufferFromString('BATCH_LOG'));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a WRITE_FAILURE', function (done) {
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.writeFailure);
        assert.strictEqual(msg.error.consistencies, types.consistencies.three);
        assert.strictEqual(msg.error.received, 2);
        assert.strictEqual(msg.error.blockFor, 3);
        assert.strictEqual(msg.error.failures, 1);
        assert.strictEqual(msg.error.writeType, 'COUNTER');
        assert.strictEqual(msg.error.message, 'Server failure during write query at consistency THREE (3 responses were required but only 2 replicas responded, 1 failed)');
        done();
      });

      // batchlog write timeout at consistency quorum with 0 of 1 replicas responding.
      const bodyArray = [];
      // Write Timeout (0x1500)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x15, 0]));
      // No Message
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      // THREE, with 2 received, 3 block for, 1 failures
      bodyArray.push(utils.allocBufferFromArray([0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1]));
      // Write Type 'COUNTER'
      bodyArray.push(utils.allocBufferFromArray([0, 'COUNTER'.length]));
      bodyArray.push(utils.allocBufferFromString('COUNTER'));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read an UNPREPARED', function (done) {
      const message = 'No query prepared with ID 0x8675';
      const id = utils.allocBufferFromArray([0x86, 0x75]);
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.unprepared);
        assert.deepEqual(msg.error.queryId, id);
        assert.strictEqual(msg.error.message, message);
        done();
      });

      // Unprepared with ID 0x8675
      const bodyArray = [];
      // Unprepared (0x2500)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x25, 0]));
      // ID 0x8675 was Not Prepared
      bodyArray.push(utils.allocBufferFromArray([0, message.length]));
      bodyArray.push(utils.allocBufferFromString(message));
      // 0x8675
      bodyArray.push(utils.allocBufferFromArray([0, 2]));
      bodyArray.push(id);
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a FUNCTION_FAILURE', function (done) {
      const message = "Could not execute function";
      const keyspace = 'myks';
      const functionName = 'foo';
      const argTypes = ['int', 'varchar', 'blob'];
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.functionFailure);
        assert.strictEqual(msg.error.keyspace, keyspace);
        assert.strictEqual(msg.error.functionName, functionName);
        assert.deepEqual(msg.error.argTypes, argTypes);
        assert.strictEqual(msg.error.message, message);
        done();
      });

      // Unprepared with ID 0x8675
      const bodyArray = [];
      // Function Failure 0x1400)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x14, 0]));
      // Error Message
      bodyArray.push(utils.allocBufferFromArray([0, message.length]));
      bodyArray.push(utils.allocBufferFromString(message));
      // Keyspace
      bodyArray.push(utils.allocBufferFromArray([0, keyspace.length]));
      bodyArray.push(utils.allocBufferFromString(keyspace));
      // Function Name
      bodyArray.push(utils.allocBufferFromArray([0, functionName.length]));
      bodyArray.push(utils.allocBufferFromString(functionName));
      // Arguments
      bodyArray.push(utils.allocBufferFromArray([0, argTypes.length]));
      argTypes.forEach(function (arg) {
        bodyArray.push(utils.allocBufferFromArray([0, arg.length]));
        bodyArray.push(utils.allocBufferFromString(arg));
      });
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read an ALREADY_EXISTS for Table', function (done) {
      const message = 'Table already exists!';
      const keyspace = 'myks';
      const table = 'tbl';
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.alreadyExists);
        assert.strictEqual(msg.error.keyspace, keyspace);
        assert.strictEqual(msg.error.table, table);
        assert.strictEqual(msg.error.message, message);
        done();
      });

      // Already Exists for Table
      const bodyArray = [];
      // Already Exists 0x2400)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x24, 0]));
      // Error Message
      bodyArray.push(utils.allocBufferFromArray([0, message.length]));
      bodyArray.push(utils.allocBufferFromString(message));
      // Keyspace
      bodyArray.push(utils.allocBufferFromArray([0, keyspace.length]));
      bodyArray.push(utils.allocBufferFromString(keyspace));
      // Table
      bodyArray.push(utils.allocBufferFromArray([0, table.length]));
      bodyArray.push(utils.allocBufferFromString(table));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read an ALREADY_EXISTS for Keyspace', function (done) {
      const message = 'Keyspace already exists!';
      const keyspace = 'myks';
      const parser = buildParserAndExpect(function (msg) {
        assert.ok(msg.error);
        helper.assertInstanceOf(msg.error, errors.ResponseError);
        assert.strictEqual(msg.error.code, types.responseErrorCodes.alreadyExists);
        assert.strictEqual(msg.error.keyspace, keyspace);
        assert.ifError(msg.error.table); // table should not be present.
        assert.strictEqual(msg.error.message, message);
        done();
      });

      // Already Exists for Keyspace
      const bodyArray = [];
      // Already Exists 0x2400)
      bodyArray.push(utils.allocBufferFromArray([0, 0, 0x24, 0]));
      // Error Message
      bodyArray.push(utils.allocBufferFromArray([0, message.length]));
      bodyArray.push(utils.allocBufferFromString(message));
      // Keyspace
      bodyArray.push(utils.allocBufferFromArray([0, keyspace.length]));
      bodyArray.push(utils.allocBufferFromString(keyspace));
      // Table (empty string)
      bodyArray.push(utils.allocBufferFromArray([0, 0]));
      const body = Buffer.concat(bodyArray);
      const header = getFrameHeader(body.length, types.opcodes.error, 4);
      parser._transform({
        header: header,
        chunk: body,
        offset: 0
      }, null, doneIfError(done));
    });
    it('should read a buffer until there is enough data', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.bodyLength, 4);
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        done();
      });
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: utils.allocBufferFromArray([ 0 ]),
        offset: 0
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(4, types.opcodes.result),
        chunk: utils.allocBufferFromArray([ 0, 0, types.resultKind.voidResult ]),
        offset: 0
      }, null, doneIfError(done));
    });
    it('should emit empty result one column no rows', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
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
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.result && item.result.rows && item.result.rows.length === 0);
        done();
      });
      //2 columns, no rows, in one chunk
      parser._transform(getBodyChunks(2, 0, 0, null), null, doneIfError(done));
    });
    it('should emit row when rows present', function (done) {
      const parser = newInstance();
      const rowLength = 2;
      let rowCounter = 0;
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        assert.ok(item.row);
        if ((++rowCounter) === rowLength) {
          done();
        }
      });
      parser.setOptions(33, { byRow: true });
      //3 columns, 2 rows
      parser._transform(getBodyChunks(3, rowLength, 0, 10), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 10, 32), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 32, 37), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 37, null), null, doneIfError(done));
    });
    describe('with multiple chunk lengths', function () {
      const parser = newInstance();
      let result;
      var byRowCompleted;
      parser.on('readable', function () {
        let item;
        while ((item = parser.read())) {
          if (!item.row && item.frameEnded) {
            continue;
          }
          byRowCompleted = item.byRowCompleted;
          if (byRowCompleted) {
            continue;
          }
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          result[item.header.streamId] = result[item.header.streamId] || [];
          result[item.header.streamId].push(item.row);
        }
      });
      [1, 3, 5, 13].forEach(function (chunkLength) {
        it('should emit rows chunked with chunk length of ' + chunkLength, function () {
          byRowCompleted = 'hello';
          result = {};
          const expected = [
            { columnLength: 3, rowLength: 10 },
            { columnLength: 5, rowLength: 5 },
            { columnLength: 6, rowLength: 15 },
            { columnLength: 6, rowLength: 5 },
            { columnLength: 1, rowLength: 20 }
          ];
          const items = expected.map(function (item, index) {
            parser.setOptions(index, { byRow: true });
            return getBodyChunks(item.columnLength, item.rowLength, 0, null, null, index);
          });
          function transformChunkedItem(i) {
            const item = items[i];
            const chunkedItem = {
              header: item.header,
              offset: 0
            };
            for (let j = 0; j < item.chunk.length; j = j + chunkLength) {
              let end = j + chunkLength;
              if (end >= item.chunk.length) {
                end = item.chunk.length;
                chunkedItem.frameEnded = true;
              }
              const start = j;
              if (start === 0) {
                //sum a few bytes
                chunkedItem.chunk = Buffer.concat([ utils.allocBufferUnsafe(9), item.chunk.slice(start, end) ]);
                chunkedItem.offset = 9;
              }
              else {
                chunkedItem.chunk = item.chunk.slice(start, end);
                chunkedItem.offset = 0;
              }
              parser._transform(chunkedItem, null, helper.throwop);
            }
          }
          for (let i = 0; i < items.length; i++) {
            transformChunkedItem(i);
          }

          assert.ok(byRowCompleted);
          //assert result
          expected.forEach(function (expectedItem, index) {
            assert.ok(result[index], 'Result not found for index ' + index);
            assert.strictEqual(result[index].length, expectedItem.rowLength);
          });
        });
      });
    });
    describe('with multiple chunk lengths piped', function () {
      const protocol = new streams.Protocol({ objectMode: true });
      const parser = newInstance();
      protocol.pipe(parser);
      let result;
      var byRowCompleted;
      parser.on('readable', function () {
        let item;
        while ((item = parser.read())) {
          if (!item.row && item.frameEnded) {
            continue;
          }
          byRowCompleted = item.byRowCompleted;
          if (byRowCompleted) {
            continue;
          }
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          result[item.header.streamId] = result[item.header.streamId] || [];
          result[item.header.streamId].push(item.row);
        }
      });
      const expected = [
        { columnLength: 3, rowLength: 10 },
        { columnLength: 5, rowLength: 5 },
        { columnLength: 6, rowLength: 15 },
        { columnLength: 6, rowLength: 15 },
        { columnLength: 1, rowLength: 20 }
      ];
      [1, 2, 7, 11].forEach(function (chunkLength) {
        it('should emit rows chunked with chunk length of ' + chunkLength, function () {
          result = {};
          const buffer = Buffer.concat(expected.map(function (expectedItem, index) {
            parser.setOptions(index, { byRow: true });
            const item = getBodyChunks(expectedItem.columnLength, expectedItem.rowLength, 0, null, null, index);
            return Buffer.concat([ item.header.toBuffer(), item.chunk ]);
          }));

          for (let j = 0; j < buffer.length; j = j + chunkLength) {
            let end = j + chunkLength;
            if (end >= buffer.length) {
              end = buffer.length;
            }
            protocol._transform(buffer.slice(j, end), null, helper.throwop);
          }
          assert.ok(byRowCompleted);
          //assert result
          expected.forEach(function (expectedItem, index) {
            assert.ok(result[index], 'Result not found for index ' + index);
            assert.strictEqual(result[index].length, expectedItem.rowLength);
            assert.strictEqual(result[index][0].keys().length, expectedItem.columnLength);
          });
        });
      });
    });
    it('should emit row with large row values', function (done) {
      this.timeout(20000);
      //3mb value
      let cellValue = helper.fillArray(3 * 1024 * 1024, 74);
      //Add the length 0x00300000 of the value
      cellValue = [0, 30, 0, 0].concat(cellValue);
      const rowLength = 1;
      utils.series([function (next) {
        const parser = newInstance();
        let rowCounter = 0;
        parser.on('readable', function () {
          const item = parser.read();
          assert.strictEqual(item.header.opcode, types.opcodes.result);
          assert.ok(item.row);
          if ((++rowCounter) === rowLength) {
            next();
          }
        });
        //1 columns, 1 row, 1 chunk
        parser._transform(getBodyChunks(1, rowLength, 0, null, cellValue), null, doneIfError(done));
      }, function (next) {
        const parser = newInstance();
        let rowCounter = 0;
        parser.on('readable', function () {
          const item = parser.read();
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
        const parser = newInstance();
        let rowCounter = 0;
        parser.on('readable', function () {
          const item = parser.read();
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
        let cellValue = helper.fillArray(256, 74);
        //Add the length 256 of the value
        cellValue = [0, 0, 1, 0].concat(cellValue);
        const parser = newInstance();
        let rowCounter = 0;
        parser.on('readable', function () {
          const item = parser.read();
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
        let cellValue = helper.fillArray(256, 74);
        //Add the length 256 of the value
        cellValue = [0, 0, 1, 0].concat(cellValue);
        const parser = newInstance();
        let rowCounter = 0;
        parser.on('readable', function () {
          const item = parser.read();
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
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.authChallenge);
        helper.assertValueEqual(item.token, utils.allocBufferFromArray([100, 100]));
        assert.strictEqual(item.authChallenge, true);
        done();
      });
      //Length + buffer
      const bodyLength = 4 + 2;
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.authChallenge),
        chunk: utils.allocBufferFromArray([255, 254, 0, 0, 0, 2]),
        offset: 2
      }, null, doneIfError(done));
      parser._transform({
        header: getFrameHeader(bodyLength, types.opcodes.authChallenge),
        chunk: utils.allocBufferFromArray([100, 100]),
        offset: 0
      }, null, doneIfError(done));
    });
    it('should buffer ERROR response until complete', function (done) {
      const parser = newInstance();
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.error);
        helper.assertInstanceOf(item.error, errors.ResponseError);
        assert.strictEqual(item.error.message, 'ERR');
        //mocha will fail if done is called multiple times
        assert.strictEqual(parser.read(), null);
        done();
      });
      //streamId 33
      const header = new types.FrameHeader(4, 0, 33, types.opcodes.error, 9);
      parser.setOptions(33, { byRow: true });
      assert.strictEqual(parser.frameState({ header: header}).byRow, true);
      parser._transform({ header: header, chunk: utils.allocBufferFromArray([255, 0, 0, 0, 0]), offset: 1}, null, doneIfError(done));
      parser._transform({ header: header, chunk: utils.allocBufferFromArray([0, 3]), offset: 0}, null, doneIfError(done));
      parser._transform({ header: header, chunk: utils.allocBufferFromString('ERR'), offset: 0}, null, doneIfError(done));
    });
    it('should not buffer RESULT ROWS response when byRow is enabled', function (done) {
      const parser = newInstance();
      const rowLength = 2;
      let rowCounter = 0;
      var byRowCompleted = false;
      parser.on('readable', function () {
        const item = parser.read();
        assert.strictEqual(item.header.opcode, types.opcodes.result);
        byRowCompleted = item.byRowCompleted;
        if (!item.byRowCompleted) {
          assert.ok(item.row);
          rowCounter++;
        }
      });
      //12 is the stream id used by the header helper by default
      parser.setOptions(12, { byRow: true });
      //3 columns, 2 rows
      parser._transform(getBodyChunks(3, rowLength, 0, 10), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 10, 32), null, doneIfError(done));
      parser._transform(getBodyChunks(3, rowLength, 32, 55), null, doneIfError(done));
      assert.strictEqual(rowCounter, 1);
      assert.notEqual(byRowCompleted, true);
      parser._transform(getBodyChunks(3, rowLength, 55, null), null, doneIfError(done));
      assert.strictEqual(rowCounter, 2);
      assert.ok(byRowCompleted);
      done();
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
 * Test Helper method to get a frame header with stream id 12
 * @returns {exports.FrameHeader}
 */
function getFrameHeader(bodyLength, opcode, version, trace, streamId, warnings, customPayload) {
  if (typeof streamId === 'undefined') {
    streamId = 12;
  }
  let flags = 0;
  flags += (trace ? 0x2 : 0x0);
  flags += (customPayload ? 0x4 : 0x0);
  flags += (warnings ? 0x8 : 0x0);
  return new types.FrameHeader(version || 2, flags, streamId, opcode, bodyLength);
}

/**
 * @returns {{header: FrameHeader, chunk: Buffer, offset: number}}
 */
function getBodyChunks(columnLength, rowLength, fromIndex, toIndex, cellValue, streamId) {
  let i;
  let fullChunk = [
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
    let rowChunk = [];
    for (let j = 0; j < columnLength; j++) {
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
    header: getFrameHeader(fullChunk.length, types.opcodes.result, null, null, streamId),
    chunk: utils.allocBufferFromArray(fullChunk.slice(fromIndex, toIndex || undefined)),
    offset: 0
  };
}

function getEventData(eventType, value) {
  const bodyArray = [];
  //EVENT TYPE
  bodyArray.push(utils.allocBufferFromArray([0, eventType.length]));
  bodyArray.push(utils.allocBufferFromString(eventType));
  //STATUS CHANGE DESCRIPTION
  bodyArray.push(utils.allocBufferFromArray([0, value.length]));
  bodyArray.push(utils.allocBufferFromString(value));
  //Address
  bodyArray.push(utils.allocBufferFromArray([4, 127, 0, 0, 1]));
  //Port
  bodyArray.push(utils.allocBufferFromArray([0, 0, 0, 200]));

  const body = Buffer.concat(bodyArray);
  const header = new types.FrameHeader(2, 0, -1, types.opcodes.event, body.length);
  return {header: header, chunk: body};
}

function buildParserAndExpect(validationFn) {
  const parser = newInstance();
  parser.on('readable', () => validationFn(parser.read()));
  return parser;
}

/**
 * Calls done in case there is an error
 */
function doneIfError(done) {
  return function doneIfErrorCallback(err) {
    if (err) {
      done(err);
    }
  };
}
