/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

import { auth, Client, types } from "../../../index";

/*
 * TypeScript definitions compilation tests for Client class.
 */

async function myTest(): Promise<any> {
  const client = new Client({
    contactPoints: ['h1', 'h2'],
    localDataCenter: 'dc1',
    keyspace: 'ks1',
    authProvider: new auth.PlainTextAuthProvider('a', 'b')
  });

  let promise: Promise<void>;
  let result: types.ResultSet;
  let error: Error;

  promise = client.connect();
  client.connect(err => error = err);

  const query = 'SELECT * FROM test';
  const params1 = [ 1 ];
  const params2 = { val: 1 };

  // Promise-based execution
  result = await client.execute(query);
  result = await client.execute(query, params1);
  result = await client.execute(query, params2);
  result = await client.execute(query, params1, { prepare: true });

  // Callback-based execution
  client.execute(query, useResult);
  client.execute(query, params1, useResult);
  client.execute(query, params2, useResult);
  client.execute(
    query,
    params2,
    { prepare: true, isIdempotent: false, consistency: types.consistencies.localOne },
    useResult);

  const queries1 = [
    'INSERT something',
    { query: 'UPDATE something', params: params1 },
    { query: 'INSERT something else', params: params2 }
  ];

  // Promise-based execution
  result = await client.batch(queries1);
  result = await client.batch(queries1, { prepare: true, isIdempotent: true });

  // Callback-based execution
  client.batch(queries1, useResult);
  client.batch(queries1, { prepare: true, logged: true, counter: false, executionProfile: 'ep1' }, useResult);

  promise = client.shutdown();
  client.shutdown(err => error = err);
}

function useResult(err: Error, rs: types.ResultSet): void {
  // Mock function that takes the parameters defined in the driver callback
}