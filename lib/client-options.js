var loadBalancing = require('./policies/load-balancing.js');
var reconnection = require('./policies/reconnection.js');
var retry = require('./policies/retry.js');
var options = {
  policies: {
    loadBalancing: new loadBalancing.RoundRobinPolicy(),
    reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
    retry: new retry.RetryPolicy()
  },
  pooling: {
    coreConnectionsPerHost: {
      '0': 2,
      '1': 1,
      '2': 0
    },
    maxConnectionsPerHost: {}
  }
};

module.exports = options;