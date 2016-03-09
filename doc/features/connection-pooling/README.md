# Connection pooling

The driver maintains one or more connections opened to each Cassandra node selected by the load-balancing policy.
The amount of connections per host is defined in the pooling configuration.

## Default pooling configuration 

The default number of connections per host depends on the version of the Cassandra cluster.

Cassandra versions 1.2 and 2.0 allow the clients to send up to `128` requests without waiting for a response per
connection. Higher versions of Cassandra ( that is 2.1 or greater) allow clients to send up to `32768` requests
without waiting for a response.

By default, the driver maintains two open connections to each host in the local datacenter and one to each host in a
remote datacenter for Cassandra 1.2 or 2.0 and one connection to each host (local or remote) for Cassandra 2.1 or
greater.

## Setting the number of connections per host 

You can set the number of connections per host in the pooling configuration:

```javascript
const cassandra = require('cassandra-driver');
const distance = cassandra.types.distance;

const options = {
   contactPoints: ['1.2.3.4'],
   pooling: {
      coreConnectionsPerHost: {
        [distance.local] = 2,
        [distance.remote] = 1
      } 
   }
};

const client = new Client(options);
```