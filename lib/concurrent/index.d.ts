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

import { Client } from '../../';
import { Readable } from 'stream';

export namespace concurrent {
  interface ResultSetGroup {
    errors: Error[];
    resultItems: any[];
    totalExecuted: number;
  }

  type Options = {
    collectResults?: boolean;
    concurrencyLevel?: number;
    executionProfile?: string;
    maxErrors?: number;
    raiseOnFirstError?: boolean;
  }

  function executeConcurrent(
    client: Client,
    query: string,
    parameters: any[][]|Readable,
    options?: Options): Promise<ResultSetGroup>;

  function executeConcurrent(
    client: Client,
    queries: Array<{query: string, params: any[]}>,
    options?: Options): Promise<ResultSetGroup>;
}