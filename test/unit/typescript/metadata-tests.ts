/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

import { Client, Host, metadata, types } from "../../../index";
import TableMetadata = metadata.TableMetadata;
import QueryTrace = metadata.QueryTrace;


/*
 * TypeScript definitions compilation tests for metadata module.
 */

async function myTest(): Promise<any> {
  const client = new Client({
    contactPoints: ['h1', 'h2'],
    localDataCenter: 'dc1'
  });

  let promise: Promise<void>;
  let s: string;
  let n: number;
  let hosts: Host[];
  let emptyCb = (err: Error) => {};

  promise = client.connect();

  hosts = client.metadata.getReplicas('ks1', Buffer.from([ 0 ]));

  let table: TableMetadata = await client.metadata.getTable('ks1', 'table1');
  client.metadata.getTable('ks1', 'table1', (err, t) => useResult<TableMetadata>(err, t));

  client.metadata.refreshKeyspace('ks', emptyCb);
  promise = client.metadata.refreshKeyspace('ks');
  s = client.metadata.keyspaces['ks1'].strategy;

  let trace: QueryTrace = await client.metadata.getTrace(types.Uuid.random());
  client.metadata.getTrace(types.Uuid.random(), (err, t) => useResult<QueryTrace>(err, t));

  hosts = client.getState().getConnectedHosts();
  n = client.getState().getInFlightQueries(hosts[0]);
  n = client.getState().getOpenConnections(hosts[0]);
}

function useResult<T>(err: Error, rs: T): void {
  // Mock function that takes the parameters defined in the driver callback
}