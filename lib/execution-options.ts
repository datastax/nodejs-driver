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
/* eslint-disable @typescript-eslint/no-unused-vars */
import type Client from "./client";
import type { QueryOptions } from "./client";
import errors from "./errors";
import { ExecutionProfile } from "./execution-profile";
import type { Host } from "./host";
import { LoadBalancingPolicy } from "./policies/load-balancing";
import { RetryPolicy } from "./policies/retry";
import types, { type consistencies, Long } from "./types/index";
import utils from "./utils";


const proxyExecuteKey = 'ProxyExecute';

/**
 * A base class that represents a wrapper around the user provided query options with getter methods and proper
 * default values.
 * <p>
 *   Note that getter methods might return <code>undefined</code> when not set on the query options or default
 *  {@link Client} options.
 * </p>
 */
class ExecutionOptions {

  /**
   * Creates a new instance of {@link ExecutionOptions}.
   */
  constructor() {
  }

  /**
   * Creates an empty instance, where all methods return undefined, used internally.
   * @ignore @internal
   * @return {ExecutionOptions}
   */
  static empty(): ExecutionOptions {
    return new ExecutionOptions();
  }

  /**
   * Determines if the stack trace before the query execution should be maintained.
   * @abstract
   * @returns {Boolean}
   */
  getCaptureStackTrace(): boolean {
    return undefined;
  }

  /**
   * Gets the [Consistency level]{@link module:types~consistencies} to be used for the execution.
   * @abstract
   * @returns {Number}
   */
  getConsistency(): consistencies {
    return undefined;
  }

  /**
   * Key-value payload to be passed to the server. On the server side, implementations of QueryHandler can use
   * this data.
   * @abstract
   * @returns {{ [key: string]: any }}
   */
  getCustomPayload(): { [key: string]: any } {
    return undefined;
  }

  /**
   * Gets the amount of rows to retrieve per page.
   * @abstract
   * @returns {Number}
   */
  getFetchSize(): number {
    return undefined;
  }

  /**
   * When a fixed host is set on the query options and the query plan for the load-balancing policy is not used, it
   * gets the host that should handle the query.
   * @returns {Host}
   */
  getFixedHost(): Host {
    return undefined;
  }

  /**
   * Gets the type hints for parameters given in the query, ordered as for the parameters.
   * @abstract
   * @returns {string[] | string[][]}
   */
  getHints(): string[] | string[][] {
    return undefined;
  }

  /**
   * Determines whether the driver must retrieve the following result pages automatically.
   * <p>
   *   This setting is only considered by the [Client#eachRow()]{@link Client#eachRow} method.
   * </p>
   * @abstract
   * @returns {Boolean}
   */
  isAutoPage(): boolean {
    return undefined;
  }

  /**
   * Determines whether its a counter batch. Only valid for [Client#batch()]{@link Client#batch}, it will be ignored by
   * other methods.
   * @abstract
   * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
   */
  isBatchCounter(): boolean {
    return undefined;
  }

  /**
   * Determines whether the batch should be written to the batchlog. Only valid for
   * [Client#batch()]{@link Client#batch}, it will be ignored by other methods.
   * @abstract
   * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
   */
  isBatchLogged(): boolean {
    return undefined;
  }

  /**
   * Determines whether the query can be applied multiple times without changing the result beyond the initial
   * application.
   * @abstract
   * @returns {Boolean}
   */
  isIdempotent(): boolean {
    return undefined;
  }

  /**
   * Determines whether the query must be prepared beforehand.
   * @abstract
   * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
   */
  isPrepared(): boolean {
    return undefined;
  }

  /**
   * Determines whether query tracing is enabled for the execution.
   * @abstract
   * @returns {Boolean}
   */
  isQueryTracing(): boolean {
    return undefined;
  }

  /**
   * Gets the keyspace for the query when set at query options level.
   * <p>
   *   Note that this method will return <code>undefined</code> when the keyspace is not set at query options level.
   *   It will only return the keyspace name when the user provided a different keyspace than the current
   *   {@link Client} keyspace.
   * </p>
   * @abstract
   * @returns {String}
   */
  getKeyspace(): string {
    return undefined;
  }

  /**
   * Gets the load balancing policy used for this execution.
   * @returns {LoadBalancingPolicy} A <code>LoadBalancingPolicy</code> instance, it can't be <code>undefined</code>.
   */
  getLoadBalancingPolicy(): LoadBalancingPolicy {
    return undefined;
  }

  /**
   * Gets the Buffer representing the paging state.
   * @abstract
   * @returns {Buffer}
   */
  getPageState(): Buffer {
    return undefined;
  }

  /**
   * Internal method that gets the preferred host.
   * @abstract
   * @ignore @internal
   */
  getPreferredHost(): Host {
    return undefined;
  }

  /**
   * Gets the query options as provided to the execution method without setting the default values.
   * @returns {QueryOptions}
   */
  getRawQueryOptions(): QueryOptions {
    return undefined;
  }

  /**
   * Gets the timeout in milliseconds to be used for the execution per coordinator.
   * <p>
   *   A value of <code>0</code> disables client side read timeout for the execution. Default: <code>undefined</code>.
   * </p>
   * @abstract
   * @returns {Number}
   */
  getReadTimeout(): number {
    return undefined;
  }

  /**
   * Gets the [retry policy]{@link module:policies/retry} to be used.
   * @abstract
   * @returns {RetryPolicy} A <code>RetryPolicy</code> instance, it can't be <code>undefined</code>.
   */
  getRetryPolicy(): RetryPolicy {
    return undefined;
  }

  /**
   * Internal method to obtain the row callback, for "by row" results.
   * @abstract
   * @ignore @internal
   */
  getRowCallback() {
    return undefined;
  }

  /**
   * Internal method to get or generate a timestamp for the request execution.
   * @ignore @internal
   * @returns {Long|null}
   */
  getOrGenerateTimestamp(): Long | null {
    return undefined;
  }

  /**
   * Gets the index of the parameters that are part of the partition key to determine the routing.
   * @abstract
   * @ignore @internal
   * @returns {Array}
   */
  getRoutingIndexes(): Array<any> {
    return undefined;
  }

  /**
   * Gets the partition key(s) to determine which coordinator should be used for the query.
   * @abstract
   * @returns {Buffer|Array<Buffer>}
   */
  getRoutingKey(): Buffer | Array<Buffer> {
    return undefined;
  }

  /**
   * Gets the array of the parameters names that are part of the partition key to determine the
   * routing. Only valid for non-prepared requests.
   * @abstract
   * @ignore @internal
   */
  getRoutingNames() {
    return undefined;
  }

  /**
   * Gets the the consistency level to be used for the serial phase of conditional updates.
   * @abstract
   * @returns {consistencies}
   */
  getSerialConsistency(): consistencies {
    return undefined;
  }

  /**
   * Gets the provided timestamp for the execution in microseconds from the unix epoch (00:00:00, January 1st, 1970).
   * <p>When a timestamp generator is used, this method returns <code>undefined</code>.</p>
   * @abstract
   * @returns {Number|Long|undefined|null}
   */
  getTimestamp(): number | Long | undefined | null {
    return undefined;
  }

  //TODO: was exposed in .d.t.s. Are we removing it?
  /**
   * @param {Array} hints
   * @abstract
   * @ignore @internal
   */
  setHints(hints: string[]) {
    return undefined;
  }

  /**
   * Sets the keyspace for the execution.
   * @ignore @internal
   * @abstract
   * @param {String} keyspace
   */
  setKeyspace(keyspace: string) {
    return undefined;
  }

  /**
   * @abstract
   * @ignore @internal
   */
  setPageState(pageState: Buffer) {
    return undefined;
  }

  /**
   * Internal method that sets the preferred host.
   * @abstract
   * @ignore @internal
   */
  setPreferredHost(host: Host) {
    return undefined;
  }

  /**
   * Sets the index of the parameters that are part of the partition key to determine the routing.
   * @param {Array} routingIndexes
   * @abstract
   * @ignore @internal
   */
  setRoutingIndexes(routingIndexes: Array<any>) {
    return undefined;
  }

  /**
   * Sets the routing key.
   * @abstract
   * @ignore @internal
   */
  setRoutingKey(value) {
    return undefined;
  }
}

/**
 * Internal implementation of {@link ExecutionOptions} that uses the value from the client options and execution
 * profile into account.
 * @ignore @internal
 */
class DefaultExecutionOptions extends ExecutionOptions {
  protected _queryOptions: QueryOptions;
  protected _rowCallback: Function;
  protected _routingKey: any;
  protected _hints: any;
  protected _keyspace: any;
  protected _routingIndexes: any;
  protected _pageState: any;
  protected _preferredHost: Host;
  protected _client: Client;
  protected _defaultQueryOptions: QueryOptions;
  protected _profile: ExecutionProfile;
  protected _customPayload: object;
  /**
   * Creates a new instance of {@link ExecutionOptions}.
   * @param {QueryOptions} queryOptions
   * @param {Client} client
   * @param {Function|null} rowCallback
   */
  constructor(queryOptions: QueryOptions, client: Client, rowCallback: Function | null) {
    super();

    this._queryOptions = queryOptions;
    this._rowCallback = rowCallback;
    this._routingKey = this._queryOptions.routingKey;
    this._hints = this._queryOptions.hints;
    this._keyspace = this._queryOptions.keyspace;
    this._routingIndexes = this._queryOptions.routingIndexes;
    this._pageState = typeof this._queryOptions.pageState === 'string' ?
      utils.allocBufferFromString(this._queryOptions.pageState, 'hex') : this._queryOptions.pageState;
    this._preferredHost = null;

    this._client = client;
    this._defaultQueryOptions = client.options.queryOptions;
    this._profile = client.profileManager.getProfile(this._queryOptions.executionProfile);

    // Build a custom payload object designed for DSE-specific functionality
    this._customPayload = DefaultExecutionOptions.createCustomPayload(this._queryOptions, this._defaultQueryOptions);

    if (!this._profile) {
      throw new errors.ArgumentError(`Execution profile "${this._queryOptions.executionProfile}" not found`);
    }
  }

  /**
   * Creates a payload for given user.
   * @param {QueryOptions} userOptions
   * @param {QueryOptions} defaultQueryOptions
   * @private
   */
  static createCustomPayload(userOptions: QueryOptions, defaultQueryOptions: QueryOptions) {
    let customPayload = userOptions.customPayload || defaultQueryOptions.customPayload;
    const executeAs = userOptions.executeAs || defaultQueryOptions.executeAs;

    if (executeAs) {
      if (!customPayload) {
        customPayload = {};
        customPayload[proxyExecuteKey] = utils.allocBufferFromString(executeAs);
      } else if (!customPayload[proxyExecuteKey]) {
        // Avoid appending to the existing payload object
        customPayload = utils.extend({}, customPayload);
        customPayload[proxyExecuteKey] = utils.allocBufferFromString(executeAs);
      }
    }

    return customPayload;
  }

  /**
   * Creates a new instance {@link ExecutionOptions}, based on the query options.
   * @param {QueryOptions|null} queryOptions
   * @param {Client} client
   * @param {Function|null} [rowCallback]
   * @ignore @internal
   * @return {ExecutionOptions}
   */
  static create(queryOptions: QueryOptions | null, client: Client, rowCallback?: Function | null): ExecutionOptions {
    if (!queryOptions || typeof queryOptions === 'function') {
      // queryOptions can be null/undefined and could be of type function when is an optional parameter
      queryOptions = utils.emptyObject;
    }
    return new DefaultExecutionOptions(queryOptions, client, rowCallback);
  }

  getCaptureStackTrace() {
    return ifUndefined(this._queryOptions.captureStackTrace, this._defaultQueryOptions.captureStackTrace);
  }

  getConsistency() {
    return ifUndefined3(this._queryOptions.consistency, this._profile.consistency,
      this._defaultQueryOptions.consistency);
  }

  getCustomPayload() {
    return this._customPayload;
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

  isAutoPage() {
    return ifUndefined(this._queryOptions.autoPage, this._defaultQueryOptions.autoPage);
  }

  isBatchCounter() {
    return ifUndefined(this._queryOptions.counter, false);
  }

  isBatchLogged() {
    return ifUndefined3(this._queryOptions.logged, this._defaultQueryOptions.logged, true);
  }

  isIdempotent() {
    return ifUndefined(this._queryOptions.isIdempotent, this._defaultQueryOptions.isIdempotent);
  }

  /**
   * Determines if the query execution must be prepared beforehand.
   * @return {Boolean}
   */
  isPrepared(): boolean {
    return ifUndefined(this._queryOptions.prepare, this._defaultQueryOptions.prepare);
  }

  isQueryTracing() {
    return ifUndefined(this._queryOptions.traceQuery, this._defaultQueryOptions.traceQuery);
  }

  getKeyspace() {
    return this._keyspace;
  }

  getLoadBalancingPolicy() {
    return this._profile.loadBalancing;
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

  getPageState() {
    return this._pageState;
  }

  /**
   * Gets the profile defined by the user or the default profile
   * @internal
   * @ignore
   */
  getProfile() {
    return this._profile;
  }

  getRawQueryOptions() {
    return this._queryOptions;
  }

  getReadTimeout() {
    return ifUndefined3(this._queryOptions.readTimeout, this._profile.readTimeout,
      this._client.options.socketOptions.readTimeout);
  }

  getRetryPolicy() {
    return ifUndefined3(this._queryOptions.retry, this._profile.retry, this._client.options.policies.retry);
  }

  getRoutingIndexes() {
    return this._routingIndexes;
  }

  getRoutingKey() {
    return this._routingKey;
  }

  getRoutingNames() {
    return this._queryOptions.routingNames;
  }

  /**
   * Internal method to obtain the row callback, for "by row" results.
   * @ignore @internal
   */
  getRowCallback() {
    return this._rowCallback;
  }

  getSerialConsistency() {
    return ifUndefined3(
      this._queryOptions.serialConsistency, this._profile.serialConsistency, this._defaultQueryOptions.serialConsistency);
  }

  getTimestamp() {
    return this._queryOptions.timestamp;
  }

  /**
   * Internal property to set the custom payload.
   * @ignore
   * @internal
   * @param {Object} payload
   */
  setCustomPayload(payload: object) {
    this._customPayload = payload;
  }

  /**
   * @param {Array} hints
   */
  setHints(hints: Array<any>) {
    this._hints = hints;
  }

  /**
   * @param {String} keyspace
   */
  setKeyspace(keyspace: string) {
    this._keyspace = keyspace;
  }

  /**
   * @param {Buffer} pageState
   */
  setPageState(pageState: Buffer) {
    this._pageState = pageState;
  }

  /**
   * @param {Array} routingIndexes
   */
  setRoutingIndexes(routingIndexes: Array<any>) {
    this._routingIndexes = routingIndexes;
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

export { DefaultExecutionOptions, ExecutionOptions, proxyExecuteKey };
