var cassandra = require('cassandra-driver');

module.exports = {
  auth: require('./lib/auth'),
  DseClient: require('./lib/dse-client'),
  geometry: require('./lib/geometry'),
  graph: {
    GraphResultSet: require('./lib/graph/result-set')
  },
  //export cassandra driver modules
  Encoder: cassandra.Encoder,
  errors: cassandra.errors,
  metadata: cassandra.metadata,
  policies: cassandra.policies,
  types: cassandra.types
};