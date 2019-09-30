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

import { errors } from '../../';

export namespace metrics {
  interface ClientMetrics {
    onAuthenticationError(e: Error | errors.AuthenticationError): void;

    onClientTimeoutError(e: errors.OperationTimedOutError): void;

    onClientTimeoutRetry(e: Error): void;

    onConnectionError(e: Error): void;

    onIgnoreError(e: Error): void;

    onOtherError(e: Error): void;

    onOtherErrorRetry(e: Error): void;

    onReadTimeoutError(e: errors.ResponseError): void;

    onReadTimeoutRetry(e: Error): void;

    onResponse(latency: number[]): void;

    onSpeculativeExecution(): void;

    onSuccessfulResponse(latency: number[]): void;

    onUnavailableError(e: errors.ResponseError): void;

    onUnavailableRetry(e: Error): void;

    onWriteTimeoutError(e: errors.ResponseError): void;

    onWriteTimeoutRetry(e: Error): void;
  }

  class DefaultMetrics implements ClientMetrics {
    constructor();

    onAuthenticationError(e: Error | errors.AuthenticationError): void;

    onClientTimeoutError(e: errors.OperationTimedOutError): void;

    onClientTimeoutRetry(e: Error): void;

    onConnectionError(e: Error): void;

    onIgnoreError(e: Error): void;

    onOtherError(e: Error): void;

    onOtherErrorRetry(e: Error): void;

    onReadTimeoutError(e: errors.ResponseError): void;

    onReadTimeoutRetry(e: Error): void;

    onResponse(latency: number[]): void;

    onSpeculativeExecution(): void;

    onSuccessfulResponse(latency: number[]): void;

    onUnavailableError(e: errors.ResponseError): void;

    onUnavailableRetry(e: Error): void;

    onWriteTimeoutError(e: errors.ResponseError): void;

    onWriteTimeoutRetry(e: Error): void;
  }
}