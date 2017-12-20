'use strict';

const assert = require('assert');
const requests = require('../../lib/requests');
const Encoder = require('../../lib/encoder');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
var QueryRequest = requests.QueryRequest;
var ExecuteRequest = requests.ExecuteRequest;
var BatchRequest = requests.BatchRequest;
const encoder = new Encoder(types.protocolVersion.maxSupported, {});

describe('QueryRequest', function () {
  describe('#clone()', function () {
    var request = new QueryRequest('Q1', [ 1, 2 ], { consistency: 1, hints: [] });
    testClone(request);
  });
});

describe('ExecuteRequest', function () {
  describe('#clone()', function () {
    var request = new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [ 1, 2], { });
    testClone(request);
  });
});

describe('BatchRequest', function () {
  describe('#clone()', function () {
    var request = new BatchRequest([
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], { logged: false, consistency: 1 });
    testClone(request);
  });
});

function testClone(request) {
  it('should return a new instance with the same properties', function () {
    var cloned = request.clone();
    assert.notStrictEqual(request, cloned);
    Object.keys(request).forEach(function (key) {
      assert.strictEqual(request[key], cloned[key]);
    });
  });
  it('should generate the same buffer', function () {
    var cloned = request.clone();
    assert.strictEqual(
      request.write(encoder, 1).toString(),
      cloned.write(encoder, 1).toString()
    );
  });
}