// TypeScript Version: 2.2

/// <reference types="node" />

import * as events from 'events';

export let Client: ClientStatic;

export interface ClientStatic {
  new (options?: ClientOptions): Client;
}

export interface Client extends events.EventEmitter {
  connect(): Promise<void>;
  shutdown(): Promise<void>;
}

export interface ClientOptions {
  contactPoints: string[];
  localDataCenter?: string;
  keyspace?: string;
}