/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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