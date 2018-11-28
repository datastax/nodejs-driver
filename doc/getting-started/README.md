# Getting started

Getting started with the Node.js driver for DataStax Enterprise.

## Upgrading from the core driver

Upgrading from `cassandra-driver` to `dse-driver` can be as simple as changing the import statement to point to the
dse package:

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1'
});
```

Becomes:

```javascript
const dse = require('dse-driver');
const client = new dse.Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1'
});
```

All CQL features in the Cassandra driver (see the [core driver features][core-features]) are available in the
DSE driver.

## Connecting to a DSE cluster

To connect to a DSE cluster, you need to provide at least 1 node of the cluster, if there are more nodes than
the ones provided, the driver will automatically discover all the nodes in the cluster after it connects to the
first node.
 
Typically you create only 1 `Client` instance for a given Cassandra cluster and use it across your application.

```javascript
const dse = require('dse-driver');
const client = new dse.Client({ contactPoints: ['host1'] });
client.connect();
```

At this point, the driver will be connected to one of the contact points and discovered the rest of the nodes in your
cluster.  

See [Getting started with DataStax Node.js driver for more information][core-getting-started].

## Working with mixed workloads

The driver features [Execution Profiles](../features/execution-profiles/) that provide a mechanism to group together
a set of configuration options and reuse them across different query executions.

[Execution Profiles](../features/execution-profiles/) are specially useful when dealing with different workloads like
Graph and CQL workloads, allowing you to use a single `Client` instance for all workloads, for example:

```javascript
const client = new dse.Client({ 
  contactPoints: ['host1'],
  localDataCenter: 'oltp-us-west',
  profiles: [
    new ExecutionProfile('time-series', {
      consistency: consistency.localOne,
      readTimeout: 30000,
      serialConsistency: consistency.localSerial
    }),
    new ExecutionProfile('graph', {
      loadBalancing: new DseLoadBalancingPolicy('graph-us-west'),
      consistency: consistency.localQuorum,
      readTimeout: 10000,
      graphOptions: { name: 'myGraph' }
    })
  ]
});

// Use an execution profile for a CQL query
client.execute('SELECT * FROM system.local', null, { executionProfile: 'time-series' });

// Use an execution profile for a gremlin query
client.executeGraph('g.V().count()', null, { executionProfile: 'graph' });
```

[dse]: http://www.datastax.com/products/datastax-enterprise
[core-features]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/
[core-getting-started]: http://docs.datastax.com/en/developer/nodejs-driver/latest/getting-started/