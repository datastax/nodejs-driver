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

## Working code example

```javascript
/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const cassandra = require('cassandra-driver');
const async = require('async');

/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 *
 * Inserts some vertex and edges: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
 */

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  profiles: [
    // Set the graph name in the default execution profile
    new cassandra.ExecutionProfile('default', {
      graphOptions: { name: 'example_graph' }
    })
  ]
});

const modernSchema =
  'schema.config().option("graph.allow_scan").set("true");\n' +
  'schema.propertyKey("name").Text().ifNotExists().create();\n' +
  'schema.propertyKey("age").Int().ifNotExists().create();\n' +
  'schema.propertyKey("relationship_weight").Float().ifNotExists().create();\n' +
  'schema.vertexLabel("person").properties("name", "age").ifNotExists().create();\n' +
  'schema.edgeLabel("knows").properties("relationship_weight").connection("person", "person").ifNotExists().create();';

const modernGraph =
  'Vertex marko = graph.addVertex(label, "person", "name", "marko", "age", 29);\n' +
  'Vertex vadas = graph.addVertex(label, "person", "name", "vadas", "age", 27);\n' +
  'marko.addEdge("knows", vadas, "relationship_weight", 0.5f);\n';

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createGraph(next) {
    const query = 'system.graph("example_graph").ifNotExists().create();';
    // As the graph "example_graph" does not exist yet and
    // it is a system query, we need to set the graph name to `null`
    client.executeGraph(query, null, { graphName: null }, next);
  },
  function createSchema(next) {
    client.executeGraph(modernSchema, next);
  },
  function createVerticesAndEdges(next) {
    client.executeGraph(modernGraph, next);
  },
  function retrieveVertices(next) {
    client.executeGraph('g.V()', function (err, result) {
      if (err) {
        return next(err);
      }
      const vertex = result.first();
      console.log('First vertex: ', vertex);
      next();
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});
```

[iterable]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols#iterable
[modern-graph]: http://tinkerpop.apache.org/docs/3.4.5/reference/#graph-computing