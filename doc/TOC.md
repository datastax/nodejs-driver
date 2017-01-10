The DataStax Node.js Driver for Apache Cassandra is a modern feature-rich and highly tunable Node.js
client library for Apache Cassandra (1.2+) and DataStax Enterprise (3.1+) using exclusively Cassandra's binary
protocol and Cassandra Query Language v3.

## Getting started

A [`Client`](Client.html) instance maintains multiple connections to the cluster nodes and uses policies to determine
which node to use as coordinator for each query, how to handle retry and failover.

`Client` instances are designed to be long-lived and usually a single instance is enough per application.

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['host1'] });
client.execute('SELECT key FROM system.local')
  .then(function (result) {
      const row = result.first();
      console.log(row['key']);
  });
```

See [`Client class documentation`](Client.html).

## Submodules

- [auth](module-auth.html)
- [errors](module-errors.html)
- [metadata](module-metadata.html)
- [policies](module-policies.html)
- [policies/addressResolution](module-policies_addressResolution.html)
- [policies/loadBalancing](module-policies_loadBalancing.html)
- [policies/reconnection](module-policies_reconnection.html)
- [policies/retry](module-policies_retry.html)
- [types](module-types.html)
