'use strict';

const utils = require('./utils');
const types = require('./types');
const errors = require('./errors');

/**
 * Represents a wrapper around the user provided query options, with getter methods and defaults from the client
 * options.
 * @interface
 * TODO: Document each method
 * TODO: Reorder methods
 * TODO: Go one by one queryOptions' properties and search for uses
 * TODO: Adapt LBP, Retry policies, request tracker
 * TODO: Remove client-options.js code
 * TODO: Test exported interface
 */
class ExecutionInfo {
  /**
   * Creates an empty instance, where all methods return undefined, used internally.
   * @ignore
   * @return {ExecutionInfo}
   */
  static empty() {
    return new ExecutionInfo();
  }

  getIsAutoPage() {

  }

  getCaptureStackTrace() {

  }

  getConsistency() {

  }

  getCustomPayload() {

  }

  /**
   * Gets the execution profile instance.
   * @return {ExecutionProfile}
   */
  getExecutionProfile() {

  }

  getFetchSize() {

  }

  /**
   * When a fixed host is set on the query options and the query plan for the load-balancing policy is not used, it
   * gets the host that should handle the query.
   * @returns {Host}
   */
  getFixedHost() {

  }

  getHints() {

  }

  /**
   * @param {Array} hints
   */
  setHints(hints) {

  }

  getIsIdempotent() {

  }

  getKeyspace() {

  }

  /**
   * @param {String} keyspace
   */
  setKeyspace(keyspace) {

  }

  getIsBatchLogged() {

  }

  getIsBatchCounter() {

  }

  getPageState() {

  }

  setPageState() {

  }

  /**
   * Determines if the query execution must be prepared beforehand.
   * @return {Boolean}
   */
  getIsPrepared() {

  }

  getReadTimeout() {

  }

  getRetryPolicy() {

  }

  /**
   * Internal method to obtain the row callback, for "by row" results.
   * @ignore
   */
  getRowCallback() {

  }

  getSerialConsistency() {

  }

  /**
   * Internal method to get or generate a timestamp for the request execution.
   * @ignore
   * @returns {Long|null}
   */
  getOrGenerateTimestamp() {

  }

  /**
   * @returns {Number|Long|undefined|null}
   */
  getTimestamp() {

  }

  getIsQueryTracing() {

  }

  getRoutingIndexes() {

  }

  /**
   * @param {Array} routingIndexes
   */
  setRoutingIndexes(routingIndexes) {

  }

  getRoutingKey() {

  }

  getRoutingNames() {

  }

  setRoutingKey(value) {

  }
}

class DefaultExecutionInfo extends ExecutionInfo {
  /**
   * Creates a new instance of {@link ExecutionInfo}.
   * @param {QueryOptions} queryOptions
   * @param {Client} client
   * @param {Function|null} rowCallback
   */
  constructor(queryOptions, client, rowCallback) {
    super();
    this._queryOptions = queryOptions;
    this._rowCallback = rowCallback;
    this._routingKey = this._queryOptions.routingKey;
    this._hints = this._queryOptions.hints;
    this._keyspace = this._queryOptions.keyspace;
    this._routingIndexes = this._queryOptions.routingIndexes;
    this._pageState = typeof this._queryOptions.pageState === 'string' ?
      utils.allocBufferFromString(this._queryOptions.pageState, 'hex') : this._queryOptions.pageState;

    this._client = client;
    this._defaultQueryOptions = client.options.queryOptions;
    this._profile = client.profileManager.getProfile(this._queryOptions.executionProfile);

    if (!this._profile) {
      throw new errors.ArgumentError(`Execution profile "${this._queryOptions.executionProfile}" not found`);
    }
  }

  /**
   * Creates a new instance {@link ExecutionInfo}.
   * @param {QueryOptions|null} queryOptions
   * @param {Client} client
   * @param {Function|null} [rowCallback]
   * @ignore
   * @return {ExecutionInfo}
   */
  static create(queryOptions, client, rowCallback) {
    if (!queryOptions || typeof queryOptions === 'function') {
      // queryOptions can be null/undefined and could be of type function when is an optional parameter
      queryOptions = utils.emptyObject;
    }
    return new DefaultExecutionInfo(queryOptions, client, rowCallback);
  }

  getIsAutoPage() {
    return ifUndefined(this._queryOptions.autoPage, this._defaultQueryOptions.autoPage);
  }

  getCaptureStackTrace() {
    return ifUndefined(this._queryOptions.captureStackTrace, this._defaultQueryOptions.captureStackTrace);
  }

  getConsistency() {
    return ifUndefined3(this._queryOptions.consistency, this._profile.consistency,
      this._defaultQueryOptions.consistency);
  }

  getCustomPayload() {
    return ifUndefined(this._queryOptions.customPayload, this._defaultQueryOptions.customPayload);
  }

  /**
   * Gets the execution profile instance.
   * @return {ExecutionProfile}
   */
  getExecutionProfile() {
    return this._profile;
  }

  getFetchSize() {
    return ifUndefined(this._queryOptions.fetchSize, this._defaultQueryOptions.fetchSize);
  }

  getFixedHost() {
    return this._queryOptions.host;
  }

  getHints() {
    return this._hints;
  }

  /**
   * @param {Array} hints
   */
  setHints(hints) {
    this._hints = hints;
  }

  getIsIdempotent() {
    return ifUndefined(this._queryOptions.isIdempotent, this._defaultQueryOptions.isIdempotent);
  }

  getKeyspace() {
    return this._keyspace;
  }

  /**
   * @param {String} keyspace
   */
  setKeyspace(keyspace) {
    this._keyspace = keyspace;
  }

  getIsBatchLogged() {
    return ifUndefined3(this._queryOptions.logged, this._defaultQueryOptions.logged, true);
  }

  getIsBatchCounter() {
    return ifUndefined(this._queryOptions.counter, false);
  }

  getPageState() {
    return this._pageState;
  }

  /**
   * @param {Buffer} pageState
   */
  setPageState(pageState) {
    this._pageState = pageState;
  }

  /**
   * Determines if the query execution must be prepared beforehand.
   * @return {Boolean}
   */
  getIsPrepared() {
    return ifUndefined(this._queryOptions.prepare, this._defaultQueryOptions.prepare);
  }

  getReadTimeout() {
    return ifUndefined3(this._queryOptions.readTimeout, this._profile.readTimeout,
      this._client.options.socketOptions.readTimeout);
  }

  getRetryPolicy() {
    return ifUndefined3(this._queryOptions.retry, this._profile.retry, this._client.options.policies.retry);
  }

  /**
   * Internal method to obtain the row callback, for "by row" results.
   * @ignore
   */
  getRowCallback() {
    return this._rowCallback;
  }

  getSerialConsistency() {
    return ifUndefined3(
      this._queryOptions.serialConsistency, this._profile.serialConsistency, this._defaultQueryOptions.serialConsistency);
  }

  getOrGenerateTimestamp() {
    let result = this.getTimestamp();

    if (result === undefined) {
      const generator = this._client.options.policies.timestampGeneration;

      if ( types.protocolVersion.supportsTimestamp(this._client.controlConnection.protocolVersion) && generator) {
        result = generator.next(this._client);
      } else {
        result = null;
      }
    }

    return typeof result === 'number' ? types.Long.fromNumber(result) : result;
  }

  getTimestamp() {
    return this._queryOptions.timestamp;
  }

  getIsQueryTracing() {
    return ifUndefined(this._queryOptions.traceQuery, this._defaultQueryOptions.traceQuery);
  }

  getRoutingIndexes() {
    return this._routingIndexes;
  }

  /**
   * @param {Array} routingIndexes
   */
  setRoutingIndexes(routingIndexes) {
    this._routingIndexes = routingIndexes;
  }

  getRoutingKey() {
    return this._routingKey;
  }

  getRoutingNames() {
    return this._queryOptions.routingNames;
  }

  setRoutingKey(value) {
    this._routingKey = value;
  }
}

function ifUndefined(v1, v2) {
  return v1 !== undefined ? v1 : v2;
}

function ifUndefined3(v1, v2, v3) {
  if (v1 !== undefined) {
    return v1;
  }
  return v2 !== undefined ? v2 : v3;
}

module.exports = { ExecutionInfo, DefaultExecutionInfo };