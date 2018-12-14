# Connection pooling

The driver maintains one or more connections opened to each DataStax Enterprise node selected by the load-balancing
policy.
The amount of connections per host is defined in the pooling configuration.

## Default pooling configuration 

The default number of connections per host depends on the version of the DSE cluster.
When using the driver to connect to modern server versions (DSE 4.7 and above), the driver uses one
connection per host.

## Setting the number of connections per host 

If needed, you can set the number of connections per host depending on the distance, relative to the driver instance,
in the `pooling` configuration:

```javascript
const dse = require('dse-driver');
const distance = dse.types.distance;

const options = {
  contactPoints,
  localDataCenter,
  pooling: {
    coreConnectionsPerHost: {
      [distance.local]: 2,
      [distance.remote]: 1
    } 
  }
};

const client = new dse.Client(options);
```

## Simultaneous requests per connection

The driver limits the amount of concurrent requests per connection to `2048` with modern protocol versions and `128` 
with older versions of the protocol (v1 and v2).

You can throttle requests by setting the `maxRequestsPerConnection` value in the `poolingOptions`.

When the limit is reached for all connections to a host, the driver will move to the next host according to the query
plan. When the query plan is exhausted, the driver will yield a `NoHostAvailableError` containing 
`BusyConnectionError` instances per each host in the `innerErrors` property.  

## Get status of the connection pool

You can use `getState()` method to get a point-in-time information of the state of the connections pools to each host.

```javascript
const state = client.getState();
for (let host of state.getConnectedHosts()) {
  console.log('Host %s: open connections = %d; in flight queries = %d',
    host.address, state.getOpenConnections(host), state.getInFlightQueries(host));
}
```