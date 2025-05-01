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
  
import { auth, concurrent, errors, datastax, mapping, geometry, metadata, metrics, policies, tracker, types } from "../../../";
import * as root from "../../../";

import graph = datastax.graph;

export async function generatedFn() {
  let n:number;
  let s:string;
  let o:object;
  let f:Function;

  // root classes and interfaces
  let c1: root.Client;
  let c2: root.ExecutionProfile;
  let c3: root.ExecutionOptions;

  // root namespaces/objects
  o = root.types;
  o = root.errors;
  o = root.policies;
  o = root.auth;
  o = root.mapping;
  o = root.tracker;
  o = root.metrics;
  o = root.concurrent;
  o = root.metadata;
  o = root.geometry;
  o = root.datastax;

  // auth classes and interfaces
  let c4: auth.Authenticator;
  let c5: auth.AuthProvider;
  let c6: auth.DseGssapiAuthProvider;
  let c7: auth.DsePlainTextAuthProvider;
  let c8: auth.PlainTextAuthProvider;

  // errors classes and interfaces
  let c9: errors.ArgumentError;
  let c10: errors.AuthenticationError;
  let c11: errors.BusyConnectionError;
  let c12: errors.DriverError;
  let c13: errors.OperationTimedOutError;
  let c14: errors.DriverInternalError;
  let c15: errors.NoHostAvailableError;
  let c16: errors.NotSupportedError;
  let c17: errors.ResponseError;
  let c18: errors.VIntOutOfRangeException;

  // concurrent static functions
  f = concurrent.executeConcurrent;

  // concurrent classes and interfaces
  let c19: concurrent.ResultSetGroup;

  // metadata classes and interfaces
  let c20: metadata.Metadata;

  // metrics classes and interfaces
  let c21: metrics.ClientMetrics;
  let c22: metrics.DefaultMetrics;

  // tracker classes and interfaces
  let c23: tracker.RequestLogger;
  let c24: tracker.RequestTracker;

  // geometry classes and interfaces
  let c25: geometry.LineString;
  let c26: geometry.Point;
  let c27: geometry.Polygon;

  // graph classes and interfaces
  let c28: graph.Edge;
  let c29: graph.Element;
  let c30: graph.Path;
  let c31: graph.Property;
  let c32: graph.Vertex;
  let c33: graph.VertexProperty;
  let c34: graph.GraphResultSet;

  // types.dataTypes enum values
  n = types.dataTypes.custom;
  n = types.dataTypes.ascii;
  n = types.dataTypes.bigint;
  n = types.dataTypes.blob;
  n = types.dataTypes.boolean;
  n = types.dataTypes.counter;
  n = types.dataTypes.decimal;
  n = types.dataTypes.double;
  n = types.dataTypes.float;
  n = types.dataTypes.int;
  n = types.dataTypes.text;
  n = types.dataTypes.timestamp;
  n = types.dataTypes.uuid;
  n = types.dataTypes.varchar;
  n = types.dataTypes.varint;
  n = types.dataTypes.timeuuid;
  n = types.dataTypes.inet;
  n = types.dataTypes.date;
  n = types.dataTypes.time;
  n = types.dataTypes.smallint;
  n = types.dataTypes.tinyint;
  n = types.dataTypes.duration;
  n = types.dataTypes.list;
  n = types.dataTypes.map;
  n = types.dataTypes.set;
  n = types.dataTypes.udt;
  n = types.dataTypes.tuple;

  // types.consistencies enum values
  n = types.consistencies.any;
  n = types.consistencies.one;
  n = types.consistencies.two;
  n = types.consistencies.three;
  n = types.consistencies.quorum;
  n = types.consistencies.all;
  n = types.consistencies.localQuorum;
  n = types.consistencies.eachQuorum;
  n = types.consistencies.serial;
  n = types.consistencies.localSerial;
  n = types.consistencies.localOne;

  // types.protocolVersion enum values
  n = types.protocolVersion.v1;
  n = types.protocolVersion.v2;
  n = types.protocolVersion.v3;
  n = types.protocolVersion.v4;
  n = types.protocolVersion.v5;
  n = types.protocolVersion.v6;
  n = types.protocolVersion.dseV1;
  n = types.protocolVersion.dseV2;
  n = types.protocolVersion.maxSupported;
  n = types.protocolVersion.minSupported;

  // types.distance enum values
  n = types.distance.local;
  n = types.distance.remote;
  n = types.distance.ignored;

  // types.responseErrorCodes enum values
  n = types.responseErrorCodes.serverError;
  n = types.responseErrorCodes.protocolError;
  n = types.responseErrorCodes.badCredentials;
  n = types.responseErrorCodes.unavailableException;
  n = types.responseErrorCodes.overloaded;
  n = types.responseErrorCodes.isBootstrapping;
  n = types.responseErrorCodes.truncateError;
  n = types.responseErrorCodes.writeTimeout;
  n = types.responseErrorCodes.readTimeout;
  n = types.responseErrorCodes.readFailure;
  n = types.responseErrorCodes.functionFailure;
  n = types.responseErrorCodes.writeFailure;
  n = types.responseErrorCodes.syntaxError;
  n = types.responseErrorCodes.unauthorized;
  n = types.responseErrorCodes.invalid;
  n = types.responseErrorCodes.configError;
  n = types.responseErrorCodes.alreadyExists;
  n = types.responseErrorCodes.unprepared;
  n = types.responseErrorCodes.clientWriteFailure;

  o = types.unset;


  // types classes and interfaces
  let c35: types.BigDecimal;
  let c36: types.Duration;
  let c37: types.InetAddress;
  let c38: types.Integer;
  let c39: types.LocalDate;
  let c40: types.LocalTime;
  let c41: types.Long;
  let c42: types.ResultSet;
  let c43: types.ResultStream;
  let c44: types.Row;
  let c45: types.TimeUuid;
  let c46: types.Tuple;
  let c47: types.Vector;
  let c48: types.Uuid;

  // policies.addressResolution classes and interfaces
  let c49: policies.addressResolution.AddressTranslator;
  let c50: policies.addressResolution.EC2MultiRegionTranslator;

  // policies.loadBalancing classes and interfaces
  let c51: policies.loadBalancing.AllowListPolicy;
  let c52: policies.loadBalancing.DCAwareRoundRobinPolicy;
  let c53: policies.loadBalancing.DefaultLoadBalancingPolicy;
  let c54: policies.loadBalancing.LoadBalancingPolicy;
  let c55: policies.loadBalancing.RoundRobinPolicy;
  let c56: policies.loadBalancing.TokenAwarePolicy;
  let c57: policies.loadBalancing.WhiteListPolicy;

  // policies.reconnection classes and interfaces
  let c58: policies.reconnection.ReconnectionPolicy;
  let c59: policies.reconnection.ConstantReconnectionPolicy;
  let c60: policies.reconnection.ExponentialReconnectionPolicy;

  // policies.retry classes and interfaces
  let c61: policies.retry.IdempotenceAwareRetryPolicy;
  let c62: policies.retry.FallthroughRetryPolicy;
  let c63: policies.retry.RetryPolicy;

  // policies static functions
  f = policies.defaultAddressTranslator;
  f = policies.defaultLoadBalancingPolicy;
  f = policies.defaultRetryPolicy;
  f = policies.defaultReconnectionPolicy;
  f = policies.defaultSpeculativeExecutionPolicy;
  f = policies.defaultTimestampGenerator;

  // mapping classes and interfaces
  let c64: mapping.Mapper;
  let c65: mapping.ModelMapper;
  let c66: mapping.ModelBatchMapper;
  let c67: mapping.ModelBatchItem;
  let c68: mapping.Result;
  let c69: mapping.TableMappings;
  let c70: mapping.DefaultTableMappings;
  let c71: mapping.UnderscoreCqlToCamelCaseMappings;

  // mapping.q static functions
  f = mapping.q.in_;
  f = mapping.q.gt;
  f = mapping.q.gte;
  f = mapping.q.lt;
  f = mapping.q.lte;
  f = mapping.q.notEq;
  f = mapping.q.and;
  f = mapping.q.incr;
  f = mapping.q.decr;
  f = mapping.q.append;
  f = mapping.q.prepend;
  f = mapping.q.remove;


}

