/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
var cassandra = require('cassandra-driver');

module.exports = {
  auth: require('./lib/auth'),
  Client: require('./lib/dse-client'),
  geometry: require('./lib/geometry'),
  graph: require('./lib/graph'),
  //export cassandra driver modules
  Encoder: cassandra.Encoder,
  errors: cassandra.errors,
  ExecutionProfile: require('./lib/execution-profile'),
  metadata: cassandra.metadata,
  policies: cassandra.policies,
  types: cassandra.types,
  version: require('./package.json').version
};