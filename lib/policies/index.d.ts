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

import { Client, ExecutionOptions, Host, HostMap } from '../../';
import { types } from '../types';


export namespace policies {
  function defaultAddressTranslator(): addressResolution.AddressTranslator;

  function defaultLoadBalancingPolicy(): loadBalancing.LoadBalancingPolicy;

  function defaultReconnectionPolicy(): reconnection.ReconnectionPolicy;

  function defaultRetryPolicy(): retry.RetryPolicy;

  function defaultSpeculativeExecutionPolicy(): speculativeExecution.SpeculativeExecutionPolicy;

  function defaultTimestampGenerator(): timestampGeneration.TimestampGenerator;

  namespace addressResolution {
    interface AddressTranslator {
      translate(address: string, port: number, callback: Function): void;
    }

    class EC2MultiRegionTranslator implements AddressTranslator {
      translate(address: string, port: number, callback: Function): void;
    }
  }

  namespace loadBalancing {
    abstract class LoadBalancingPolicy {
      init(client: Client, hosts: HostMap, callback): void;

      getDistance(host: Host): types.distance;

      newQueryPlan(
        keyspace: string,
        executionOptions: ExecutionOptions,
        callback: (error: Error, iterator: Iterator<Host>) => void);

      getOptions(): Map<string, object>;
    }

    class DCAwareRoundRobinPolicy extends LoadBalancingPolicy {
      constructor(localDc: string);
    }

    class TokenAwarePolicy extends LoadBalancingPolicy {
      constructor(childPolicy: LoadBalancingPolicy);
    }

    class WhiteListPolicy extends LoadBalancingPolicy {
      constructor(childPolicy: LoadBalancingPolicy, whiteList: string[]);
    }

    class RoundRobinPolicy extends LoadBalancingPolicy {
      constructor();
    }
  }

  namespace reconnection {
    class ConstantReconnectionPolicy implements ReconnectionPolicy {
      constructor(delay: number);

      getOptions(): Map<string, object>;

      newSchedule(): Iterator<number>;

    }

    class ExponentialReconnectionPolicy implements ReconnectionPolicy {
      constructor(baseDelay: number, maxDelay: number, startWithNoDelay?: boolean);

      getOptions(): Map<string, object>;

      newSchedule(): Iterator<number>;
    }

    interface ReconnectionPolicy {
      getOptions(): Map<string, object>;

      newSchedule(): Iterator<number>;
    }
  }

  namespace retry {
    class DecisionInfo {
      decision: number;
      consistency: types.consistencies;
    }

    class OperationInfo {
      query: string;
      executionOptions: ExecutionOptions;
      nbRetry: number;
    }

    class IdempotenceAwareRetryPolicy extends RetryPolicy {
      constructor(childPolicy: RetryPolicy);
    }

    class RetryPolicy {
      onReadTimeout(
        info: OperationInfo,
        consistency: types.consistencies,
        received: number,
        blockFor: number,
        isDataPresent: boolean): DecisionInfo;

      onRequestError(info: OperationInfo, consistency: types.consistencies, err: Error): DecisionInfo;

      onUnavailable(
        info: OperationInfo, consistency: types.consistencies, required: number, alive: boolean): DecisionInfo;

      onWriteTimeout(
        info: OperationInfo,
        consistency: types.consistencies,
        received: number,
        blockFor: number,
        writeType: string): DecisionInfo;

      rethrowResult(): DecisionInfo;

      retryResult(consistency: types.consistencies, useCurrentHost?: boolean): DecisionInfo;
    }

    namespace RetryDecision {
      enum retryDecision {
        ignore,
        rethrow,
        retry
      }
    }
  }

  namespace speculativeExecution {
    class ConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
      constructor(delay: number, maxSpeculativeExecutions: number);

      getOptions(): Map<string, object>;

      init(client: Client): void;

      newPlan(keyspace: string, queryInfo: string | Array<object>): { nextExecution: Function };

      shutdown(): void;
    }

    class NoSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
      constructor();

      getOptions(): Map<string, object>;

      init(client: Client): void;

      newPlan(keyspace: string, queryInfo: string | Array<object>): { nextExecution: Function };

      shutdown(): void;
    }

    interface SpeculativeExecutionPolicy {
      getOptions(): Map<string, object>;

      init(client: Client): void;

      newPlan(keyspace: string, queryInfo: string|Array<object>): { nextExecution: Function };

      shutdown(): void;
    }
  }

  namespace timestampGeneration {
    class MonotonicTimestampGenerator implements TimestampGenerator {
      constructor(warningThreshold: number, minLogInterval: number);

      getDate(): number;

      next(client: Client): types.Long | number;
    }

    interface TimestampGenerator {
      next(client: Client): types.Long|number;
    }
  }
}