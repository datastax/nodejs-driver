# Graph support

`Client` includes the `executeGraph()` method to execute graph queries:

```javascript
const client = new cassandra.Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'dc1',
  graphOptions: { name: 'demo' }
});

// executeGraph() method returns a Promise
client.executeGraph('g.V()')
  .then(function (result) {
    const vertex = result.first();
    console.log(vertex.label);
  });
```

Alternatively, you can use the callback-based execution:

```javascript
client.executeGraph('g.V()', function (err, result) {
  assert.ifError(err);
  const vertex = result.first();
  // ...
});
```

## Graph Options

You can set default graph options when initializing `Client` which will be used for all graph statements.  For
example, to avoid providing a `graphName` option in each `executeGraph()` call:

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'dc1',
  graphOptions: { name: 'demo' }
});
```

These options may be overridden by specifying the [execution profile](../execution-profiles) when calling `executeGraph()`:

```javascript
// Use a different graph name than the one provided when creating the client instance
const result = await client.executeGraph(query, params, { executionProfile: 'graph-oltp' });
const vertex = result.first();
console.log(vertex.label);
```

You can check out more info on [Execution Profiles](../execution-profiles).

## Handling Results

Graph queries return a `GraphResultSet`, which is an [iterable][iterable] of items. The format of the data returned is
dependent on the data requested.

Retrieving property values:

```javascript
const result = await client.executeGraph('g.V().hasLabel("person").values("name")');
for (const name of result) {
  console.log(name);
}
```

Retrieving vertices:

```javascript
const result = await client.executeGraph('g.V().hasLabel("person")');
for (const vertex of result) {
  console.log(vertex.label);
}
```

Retrieving edges:

```javascript
const result = await client.executeGraph('g.E()');
for (const edge of result) {
  console.log(edge.label);
}
```

### Parameters

Graph traversal execution supports named parameters. Parameters must be passed in as an object:

```javascript
const traversal = 'g.addV(vertexLabel).property("name", username)';
await client.executeGraph(traversal, { vertexLabel: 'person', username: 'marko' });
```

### Graph types

The DataStax Node.js driver supports a wide variety of TinkerPop types and [DSE types](../datatypes/). For graph
types that don't have a native JavaScript representation, the driver provides the [`types`
module](../../api/module.types/).

For example:

```javascript
const { types } = require('cassandra-driver');
const { Uuid, InetAddress } = types;

const traversal = 'g.addV("sample").property("uid", uid).property("ip_address", address)';
await client.execute(traversal, { uid: Uuid.random(), address: InetAddress.fromString('10.0.0.100') });
```

The same types are also supported for traversal execution results:

```javascript
const rs = await client.execute('g.V().hasLabel("sample").values("ip_address")');
for (const ip of rs) {
  console.log(ip instanceof InetAddress); // true
}
```

#### User-defined types

User-defined types (UDTs) are supported in the Node.js driver using JavaScript objects.

```javascript
const rs = await client.execute('g.V().hasLabel("sample").values("user_address")');
for (const address of rs) {
  console.log(`User address is ${address.street}, ${address.city} ${address.state}`);
}
```

In order to use a UDT as a parameter, you must wrap the object instance using `asUdt()` function to provide
additional information to properly represent the UDT on the server.  

```javascript
const { datastax } = require('cassandra-driver');
const { asUdt } = datastax.graph;

// Get the UDT metadata
const udtInfo = await client.metadata.getUdt(graphName, 'address');

// Build the UDT
const address = asUdt({ street: '123 Priam St.', city: 'My City', state: 'MY' }, udtInfo);

const traversal = 'g.addV("sample").property("uid", uid).property("user_address", address)';

// Use the UDT as parameter
await client.execute(traversal, { uid: Uuid.random(), address });
```

[iterable]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols#iterable
[modern-graph]: http://tinkerpop.apache.org/docs/3.4.5/reference/#graph-computing