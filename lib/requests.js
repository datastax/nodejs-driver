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

const { FrameWriter } = require('./writers');
const types = require('./types');
const utils = require('./utils');
const { ExecutionOptions } = require('./execution-options');
const packageInfo = require('../package.json');

/**
 * Options for the execution of the query / prepared statement
 * @private
 */
const queryFlag = {
  values:                 0x01,
  skipMetadata:           0x02,
  pageSize:               0x04,
  withPagingState:        0x08,
  withSerialConsistency:  0x10,
  withDefaultTimestamp:   0x20,
  withNameForValues:      0x40,
  withKeyspace:           0x80,
  withPageSizeBytes: 0x40000000,
  withContinuousPaging: 0x80000000
};

/**
 * Options for the execution of a batch request from protocol v3 and above
 * @private
 */
const batchFlag = {
  withSerialConsistency:  0x10,
  withDefaultTimestamp:   0x20,
  withNameForValues:      0x40,
  withKeyspace:           0x80
};

/**
 * Options for execution of a prepare request from protocol DSE_V2 and above
 * @private
 */
const prepareFlag = {
  withKeyspace:           0x01
};

const batchType = {
  logged: 0,
  unlogged: 1,
  counter: 2
};

/**
 * Abstract class Request
 */
class Request {
  constructor() {
    this.length = 0;
  }

  /**
   * @abstract
   * @param {Encoder} encoder
   * @param {Number} streamId
   * @throws {TypeError}
   * @returns {Buffer}
   */
  write(encoder, streamId) {
    throw new Error('Method must be implemented');
  }

  /**
   * Creates a new instance using the same constructor as the current instance, copying the properties.
   * @return {Request}
   */
  clone() {
    const newRequest = new (this.constructor)();
    const keysArray = Object.keys(this);
    for (let i = 0; i < keysArray.length; i++) {
      const key = keysArray[i];
      newRequest[key] = this[key];
    }
    return newRequest;
  }
}

/**
 * Writes a execute query (given a prepared queryId)
 * @param {String} query
 * @param {Buffer} queryId
 * @param {Array} params
 * @param options
 */
class ExecuteRequest extends Request {
  /**
   * @param {String} query
   * @param queryId
   * @param params
   * @param {ExecutionOptions} execOptions
   * @param meta
   */
  constructor(query, queryId, params, execOptions, meta) {
    super();

    this.query = query;
    this.queryId = queryId;
    this.params = params;
    this.meta = meta;
    this.options = execOptions || ExecutionOptions.empty();
    this.consistency = this.options.getConsistency() || types.consistencies.one;
    // Only QUERY request parameters are encoded as named parameters
    // EXECUTE request parameters are always encoded as positional parameters
    this.namedParameters = false;
  }

  getParamType(index) {
    const columnInfo = this.meta.columns[index];
    return columnInfo ? columnInfo.type : null;
  }

  write(encoder, streamId) {
    //v1: <queryId>
    //      <n><value_1>....<value_n><consistency>
    //v2: <queryId>
    //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
    //v3: <queryId>
    //      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
    const frameWriter = new FrameWriter(types.opcodes.execute);
    let headerFlags = this.options.isQueryTracing() ? types.frameFlags.tracing : 0;
    if (this.options.getCustomPayload()) {
      //The body may contain the custom payload
      headerFlags |= types.frameFlags.customPayload;
      frameWriter.writeCustomPayload(this.options.getCustomPayload());
    }
    frameWriter.writeShortBytes(this.queryId);
    if(types.protocolVersion.supportsResultMetadataId(encoder.protocolVersion)) {
      frameWriter.writeShortBytes(this.meta.resultId);
    }
    this.writeQueryParameters(frameWriter, encoder);

    // Record the length of the body of the request before writing it
    this.length = frameWriter.bodyLength;

    return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
  }

  /**
   * Writes v1 and v2 execute query parameters
   * @param {FrameWriter} frameWriter
   * @param {Encoder} encoder
   * @param {Boolean} [isQuery] True if query, otherwise assumed to be execute request.
   */
  writeQueryParameters(frameWriter, encoder, isQuery) {
    //v1: <n><value_1>....<value_n><consistency>
    //v2: <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
    //v3: <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
    //dse_v1: <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>]
    //          [<serial_consistency>][<timestamp>][continuous_paging_options]
    //dse_v2: <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>]
    //          [<serial_consistency>][<timestamp>][keyspace][continuous_paging_options]
    let flags = 0;

    const timestamp = this.options.getOrGenerateTimestamp();

    if (types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
      flags |= (this.params && this.params.length) ? queryFlag.values : 0;
      flags |= (this.options.getFetchSize() > 0) ? queryFlag.pageSize : 0;
      flags |= this.options.getPageState() ? queryFlag.withPagingState : 0;
      flags |= this.options.getSerialConsistency() ? queryFlag.withSerialConsistency : 0;
      flags |= timestamp !== null && timestamp !== undefined ? queryFlag.withDefaultTimestamp : 0;
      flags |= this.namedParameters ? queryFlag.withNameForValues : 0;

      // Don't inject keyspace for EXECUTE requests as inherited from prepared statement.
      const supportsKeyspace = isQuery && types.protocolVersion.supportsKeyspaceInRequest(encoder.protocolVersion);
      flags |= supportsKeyspace && this.options.getKeyspace() ? queryFlag.withKeyspace : 0;

      frameWriter.writeShort(this.consistency);
      if (types.protocolVersion.uses4BytesQueryFlags(encoder.protocolVersion)) {
        frameWriter.writeInt(flags);
      }
      else {
        frameWriter.writeByte(flags);
      }
    }

    if (this.params && this.params.length) {
      frameWriter.writeShort(this.params.length);
      for (let i = 0; i < this.params.length; i++) {
        let paramValue = this.params[i];
        if (flags & queryFlag.withNameForValues) {
          //parameter is composed by name / value
          frameWriter.writeString(paramValue.name);
          paramValue = paramValue.value;
        }
        frameWriter.writeBytes(encoder.encode(paramValue, this.getParamType(i)));
      }
    }

    if (!types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
      if (!this.params || !this.params.length) {
        //zero parameters
        frameWriter.writeShort(0);
      }
      frameWriter.writeShort(this.consistency);
      return;
    }
    if (flags & queryFlag.pageSize) {
      frameWriter.writeInt(this.options.getFetchSize());
    }
    if (flags & queryFlag.withPagingState) {
      frameWriter.writeBytes(this.options.getPageState());
    }
    if (flags & queryFlag.withSerialConsistency) {
      frameWriter.writeShort(this.options.getSerialConsistency());
    }
    if (flags & queryFlag.withDefaultTimestamp) {
      frameWriter.writeLong(timestamp);
    }
    if (flags & queryFlag.withKeyspace) {
      frameWriter.writeString(this.options.getKeyspace());
    }
  }
}

class QueryRequest extends ExecuteRequest {
  /**
   * @param {String} query
   * @param params
   * @param {ExecutionOptions} [execOptions]
   * @param {Boolean} [namedParameters]
   */
  constructor(query, params, execOptions, namedParameters) {
    super(query, null, params, execOptions, null);
    this.hints = this.options.getHints() || utils.emptyArray;
    this.namedParameters = namedParameters;
  }

  getParamType(index) {
    return this.hints[index];
  }

  write(encoder, streamId) {
    //v1: <query><consistency>
    //v2: <query>
    //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
    //v3: <query>
    //      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
    const frameWriter = new FrameWriter(types.opcodes.query);
    let headerFlags = this.options.isQueryTracing() ? types.frameFlags.tracing : 0;
    if (this.options.getCustomPayload()) {
      //The body may contain the custom payload
      headerFlags |= types.frameFlags.customPayload;
      frameWriter.writeCustomPayload(this.options.getCustomPayload());
    }

    frameWriter.writeLString(this.query);

    if (!types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
      frameWriter.writeShort(this.consistency);
    } else {
      //Use the same fields as the execute writer
      this.writeQueryParameters(frameWriter, encoder, true);
    }

    // Record the length of the body of the request before writing it
    this.length = frameWriter.bodyLength;

    return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
  }
}

class PrepareRequest extends Request {
  constructor(query, keyspace) {
    super();
    this.query = query;
    this.keyspace = keyspace;
  }

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.prepare);
    frameWriter.writeLString(this.query);
    if (types.protocolVersion.supportsPrepareFlags(encoder.protocolVersion)) {
      const flags = this.keyspace && types.protocolVersion.supportsKeyspaceInRequest(encoder.protocolVersion) ? prepareFlag.withKeyspace : 0;
      frameWriter.writeInt(flags);
      if (flags & prepareFlag.withKeyspace) {
        frameWriter.writeString(this.keyspace);
      }
    }
    return frameWriter.write(encoder.protocolVersion, streamId);
  }
}

class StartupRequest extends Request {

  /**
   * Creates a new instance of {@link StartupRequest}.
   * @param {Object} [options]
   * @param [options.cqlVersion]
   * @param [options.noCompact]
   * @param [options.clientId]
   * @param [options.applicationName]
   * @param [options.applicationVersion]
   */
  constructor(options) {
    super();
    this.options = options || {};
  }

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.startup);

    const startupOptions = {
      CQL_VERSION: this.options.cqlVersion || '3.0.0',
      DRIVER_NAME: packageInfo.description,
      DRIVER_VERSION: packageInfo.version
    };

    if(this.options.noCompact) {
      startupOptions['NO_COMPACT'] = 'true';
    }

    if (this.options.clientId) {
      startupOptions['CLIENT_ID'] = this.options.clientId.toString();
    }

    if (this.options.applicationName) {
      startupOptions['APPLICATION_NAME'] = this.options.applicationName;
    }

    if (this.options.applicationVersion) {
      startupOptions['APPLICATION_VERSION'] = this.options.applicationVersion;
    }

    frameWriter.writeStringMap(startupOptions);
    return frameWriter.write(encoder.protocolVersion, streamId);
  }
}

class RegisterRequest extends Request {
  constructor(events) {
    super();
    this.events = events;
  }

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.register);
    frameWriter.writeStringList(this.events);
    return frameWriter.write(encoder.protocolVersion, streamId);
  }
}

/**
 * Represents an AUTH_RESPONSE request
 * @param {Buffer} token
 */
class AuthResponseRequest extends Request {
  constructor(token) {
    super();
    this.token = token;
  }

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.authResponse);
    frameWriter.writeBytes(this.token);
    return frameWriter.write(encoder.protocolVersion, streamId);
  }
}

/**
 * Represents a protocol v1 CREDENTIALS request message
 */
class CredentialsRequest extends Request {
  constructor(username, password) {
    super();
    this.username = username;
    this.password = password;
  }

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.credentials);
    frameWriter.writeStringMap({ username:this.username, password:this.password });
    return frameWriter.write(encoder.protocolVersion, streamId);
  }
}

class BatchRequest extends Request {
  /**
   * Creates a new instance of BatchRequest.
   * @param {Array.<{query, params, [info]}>} queries Array of objects with the properties query and params
   * @param {ExecutionOptions} execOptions
   */
  constructor(queries, execOptions) {
    super();
    this.queries = queries;
    this.options = execOptions;
    this.hints = execOptions.getHints() || utils.emptyArray;
    this.type = batchType.logged;

    if (execOptions.isBatchCounter()) {
      this.type = batchType.counter;
    } else if (!execOptions.isBatchLogged()) {
      this.type = batchType.unlogged;
    }
  }

  /**
  * Writes a batch request
  */
  write(encoder, streamId) {
    //v2: <type><n><query_1>...<query_n><consistency>
    //v3: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
    //dseV1+: similar to v3/v4, flags is an int instead of a byte
    if (!this.queries || !(this.queries.length > 0)) {
      throw new TypeError(util.format('Invalid queries provided %s', this.queries));
    }
    const frameWriter = new FrameWriter(types.opcodes.batch);
    let headerFlags = this.options.isQueryTracing() ? types.frameFlags.tracing : 0;
    if (this.options.getCustomPayload()) {
      //The body may contain the custom payload
      headerFlags |= types.frameFlags.customPayload;
      frameWriter.writeCustomPayload(this.options.getCustomPayload());
    }
    frameWriter.writeByte(this.type);
    frameWriter.writeShort(this.queries.length);
    const self = this;
    this.queries.forEach(function eachQuery(item, i) {
      const hints = self.hints[i];
      const params = item.params || utils.emptyArray;
      let getParamType;
      if (item.queryId) {
        // Contains prepared queries
        frameWriter.writeByte(1);
        frameWriter.writeShortBytes(item.queryId);
        getParamType = i => item.meta.columns[i].type;
      }
      else {
        // Contains string queries
        frameWriter.writeByte(0);
        frameWriter.writeLString(item.query);
        getParamType = hints ? (i => hints[i]) : (() => null);
      }

      frameWriter.writeShort(params.length);
      params.forEach((param, index) => frameWriter.writeBytes(encoder.encode(param, getParamType(index))));
    }, this);

    frameWriter.writeShort(this.options.getConsistency());

    if (types.protocolVersion.supportsTimestamp(encoder.protocolVersion)) {
      // Batch flags
      let flags = this.options.getSerialConsistency() ? batchFlag.withSerialConsistency : 0;
      const timestamp = this.options.getOrGenerateTimestamp();
      flags |= timestamp !== null && timestamp !== undefined ? batchFlag.withDefaultTimestamp : 0;

      flags |= this.options.getKeyspace() && types.protocolVersion.supportsKeyspaceInRequest(encoder.protocolVersion)
        ? batchFlag.withKeyspace : 0;

      if (types.protocolVersion.uses4BytesQueryFlags(encoder.protocolVersion)) {
        frameWriter.writeInt(flags);
      }
      else {
        frameWriter.writeByte(flags);
      }

      if (flags & batchFlag.withSerialConsistency) {
        frameWriter.writeShort(this.options.getSerialConsistency());
      }

      if (flags & batchFlag.withDefaultTimestamp) {
        frameWriter.writeLong(timestamp);
      }

      if (flags & batchFlag.withKeyspace) {
        frameWriter.writeString(this.options.getKeyspace());
      }
    }

    // Set the length of the body of the request before writing it
    this.length = frameWriter.bodyLength;

    return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
  }

  clone() {
    return new BatchRequest(this.queries, this.options);
  }
}

function CancelRequest(operationId) {
  this.streamId = null;
  this.operationId = operationId;
}

util.inherits(CancelRequest, Request);

CancelRequest.prototype.write = function (encoder, streamId) {
  const frameWriter = new FrameWriter(types.opcodes.cancel);
  frameWriter.writeInt(1);
  frameWriter.writeInt(this.operationId);
  return frameWriter.write(encoder.protocolVersion, streamId);
};

class OptionsRequest extends Request {

  write(encoder, streamId) {
    const frameWriter = new FrameWriter(types.opcodes.options);
    return frameWriter.write(encoder.protocolVersion, streamId, 0);
  }

  clone() {
    // since options has no unique state, simply return self.
    return this;
  }
}

const options = new OptionsRequest();

exports.AuthResponseRequest = AuthResponseRequest;
exports.BatchRequest = BatchRequest;
exports.CancelRequest = CancelRequest;
exports.CredentialsRequest = CredentialsRequest;
exports.ExecuteRequest = ExecuteRequest;
exports.PrepareRequest = PrepareRequest;
exports.QueryRequest = QueryRequest;
exports.Request = Request;
exports.RegisterRequest = RegisterRequest;
exports.StartupRequest = StartupRequest;
exports.options = options;
