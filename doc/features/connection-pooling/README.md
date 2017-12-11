# Connection pooling

The driver maintains one or more connections opened to each Apache Cassandra node selected by the load-balancing policy.
The amount of connections per host is defined in the pooling configuration.

## Default pooling configuration 

The default number of connections per host depends on the version of the Apache Cassandra cluster. When using the driver to connect to modern server versions (Apache Cassandra 2.1 and above), the driver uses one connection per host.

## Setting the number of connections per host 

If needed, you can set the number of connections per host depending on the distance, relative to the driver instance, in the `pooling` configuration:

```javascript
const cassandra = require('cassandra-driver');
const distance = cassandra.types.distance;

const options = {
   contactPoints: ['1.2.3.4'],
   pooling: {
      coreConnectionsPerHost: {
        [distance.local]: 2,
        [distance.remote]: 1
      } 
   }
};

const client = new Client(options);
```
