# DataStax Enterprise Node.js Driver

This driver is built on top of [Node.js driver for Apache Cassandra][cassandra-driver] and provides the following
additions for [DataStax Enterprise][dse]:

* `Authenticator` implementations that use the authentication scheme negotiation in the server-side `DseAuthenticator`;
* encoders for geospatial types which integrate seamlessly with the driver;
* DSE graph integration.

The DataStax Enterprise Node.js Driver can be used solely with DataStax Enterprise. Please consult
[the license](#license).

## Installation

```bash
npm install dse-driver
```

[![Build Status](https://travis-ci.org/datastax/nodejs-dse-driver.svg?branch=master)](https://travis-ci.org/datastax/nodejs-dse-driver)

## Documentation

- [Documentation index][doc-index]
- [API docs][api-docs]
- [FAQ][faq]

## Getting Help

You can use the [project mailing list][mailing-list] or create a ticket on the [Jira issue tracker][jira]. 

## Getting Started

`Client` inherits from the CQL driver counterpart `Client`.

```javascript
const dse = require('dse-driver');
const client = new dse.Client({ contactPoints: ['host1', 'host2'] });

const query = 'SELECT name, email FROM users WHERE key = ?';
client.execute(query, [ 'someone' ])
  .then(result => console.log('User with email %s', result.rows[0].email));
```

Along with the rest of asynchronous execution methods in the driver, `execute()` returns a [`Promise`][promise] that
 can be chained using `then()` method. On modern JavaScript engines, promises can be awaited upon using the `await` 
 keyword within [async functions][async-fn].

Alternatively, you can use the callback-based execution for all asynchronous methods of the API by providing a 
callback as the last parameter.

```javascript
client.execute(query, [ 'someone' ], function(err, result) {
  assert.ifError(err);
  console.log('User with email %s', result.rows[0].email);
});
```

_In order to have concise code examples in this documentation, we will use the promise-based API of the driver 
along with the `await` keyword._

The same submodules structure in the Node.js driver for Apache Cassandra is available in the `dse-driver`, for example:

```javascript
const dse = require('dse-driver');
const Uuid = dse.types.Uuid;
```

## Authentication

For clients connecting to a DSE cluster secured with `DseAuthenticator`, two authentication providers are included:

* `DsePlainTextAuthProvider`: Plain-text authentication;
* `DseGSSAPIAuthProvider`: GSSAPI authentication;

To configure a provider, pass it when initializing a cluster:

```javascript
const dse = require('dse-driver');
const client = new dse.Client({
  contactPoints: ['h1', 'h2'], 
  keyspace: 'ks1',
  authProvider: new dse.auth.DseGssapiAuthProvider()
});
```

See the jsdoc of each implementation for more details.

## Graph

`Client` includes the `executeGraph()` method to execute graph queries:

```javascript
const client = new dse.Client({
  contactPoints: ['host1', 'host2'],
  profiles: [
    new ExecutionProfile('default', {
      graphOptions: { name: 'demo' }
    })
  ]
});
```

```javascript
// executeGraph() method returns a Promise
const result = await client.executeGraph('g.V()');
const vertex = result.first();
console.log(vertex.label);
```

### Graph Options

You can set graph options in execution profiles when initializing `Client`. Also, to avoid providing the graph name
option in each `executeGraph()` call, you can set the graph options in the default execution profile:

```javascript
const client = new dse.Client({
  contactPoints: ['host1', 'host2'],
  profiles: [
    new ExecutionProfile('default', {
      graphOptions: { name: 'demo' }
    }),
    new ExecutionProfile('demo2-profile', {
      graphOptions: { name: 'demo2' }
    })
  ]
});
```
```javascript
// Execute a traversal on the 'demo' graph
const result = await client.executeGraph(query, params);
```

If needed, you can specify an execution profile different from the default one: 

```javascript
// Execute a traversal on the 'demo2' graph
client.executeGraph(query, params, { executionProfile: 'demo2-profile'});
```

Additionally, you can also set the default graph options without using execution profiles (not recommended). 

```javascript
const client = new dse.Client({
  contactPoints: ['host1', 'host2'],
  graphOptions: { name: 'demo' }
});
```

### Handling Results

Graph queries return a `GraphResultSet`, which is an [iterable][iterable] of rows. The format of the data returned is
dependent on the data requested.  For example, the payload representing edges will be different than those that
represent vertices using the ['modern'][modern-graph] graph:

```javascript
// Creating the 'modern' graph
const query =
  'Vertex marko = graph.addVertex(label, "person", "name", "marko", "age", 29);\n' +
  'Vertex vadas = graph.addVertex(label, "person", "name", "vadas", "age", 27);\n' +
  'Vertex lop = graph.addVertex(label, "software", "name", "lop", "lang", "java");\n' +
  'Vertex josh = graph.addVertex(label, "person", "name", "josh", "age", 32);\n' +
  'Vertex ripple = graph.addVertex(label, "software", "name", "ripple", "lang", "java");\n' +
  'Vertex peter = graph.addVertex(label, "person", "name", "peter", "age", 35);\n' +
  'marko.addEdge("knows", vadas, "weight", 0.5f);\n' +
  'marko.addEdge("knows", josh, "weight", 1.0f);\n' +
  'marko.addEdge("created", lop, "weight", 0.4f);\n' +
  'josh.addEdge("created", ripple, "weight", 1.0f);\n' +
  'josh.addEdge("created", lop, "weight", 0.4f);\n' +
  'peter.addEdge("created", lop, "weight", 0.2f);';

await client.executeGraph(query);
```

```javascript
// Handling Edges
const result = await client.executeGraph('g.E()');
result.forEach(function (edge) {
  console.log(edge.id); // [an internal id representing the edge]
  console.log(edge.type); // edge
  console.log(edge.label); // created
  console.log(edge.properties.weight); // 0.4
  console.log(edge.outVLabel); // person
  console.log(edge.outV); // [an id representing the outgoing vertex]
  console.log(edge.inVLabel); // software
  console.log(edge.inV); // [an id representing the incoming vertex]
});
```

```javascript
// Using ES6 for...of
const result = await client.executeGraph('g.E()');
for (let edge of result) {
  console.log(edge.label); // created
  // ...
}
```

```javascript
// Handling Vertices
const result = await client.executeGraph('g.V().hasLabel("person")');
result.forEach(function(vertex) {
  console.log(vertex.id); // [an internal id representing the vertex]
  console.log(vertex.type); // vertex
  console.log(vertex.label); // person
  console.log(vertex.properties.name[0].value); // marko
  console.log(vertex.properties.age[0].value); // 29
});
```

### Parameters

Unlike CQL queries which support both positional and named parameters, graph queries only support named parameters.
As a result of this, parameters must be passed in as an object:

```javascript
const query = 'g.addV(label, vertexLabel, "name", username)';
const result = await client.executeGraph(query, { vertexLabel: 'person', username: 'marko' });
const vertex = result.first();
// ...
```

Parameters are encoded in json, thus will ultimately use their json representation (`toJSON` if present,
otherwise object representation).

You can use results from previous queries as parameters to subsequent queries.  For example, if you want to use the id
of a vertex returned in a previous query for making a subsequent query:

```javascript
let result = await client.executeGraph('g.V().hasLabel("person").has("name", "marko")');
const vertex = result.first();
result = await client.executeGraph('g.V(vertexId).out("knows").values("name")', { vertexId: vertex.id });
const names = result.toArray();
console.log(names); // [ 'vadas', 'josh' ]
```

### Prepared graph statements

Prepared graph statements are not supported by DSE Graph yet (they will be added in the near future).

## Geospatial types

DSE 5.0 comes with a set of additional CQL types to represent geospatial data: `PointType`, `LineStringType` and
`PolygonType`.

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes encoders and representations of these types in the `geometry` module that can be used directly
as parameters in queries:

```javascript
const dse = require('dse-driver');
const Point = dse.geometry.Point;
const insertQuery = 'INSERT INTO points_of_interest (name, coords) VALUES (?, ?)';
const selectQuery = 'SELECT coords FROM points_of_interest WHERE name = ?';

await client.execute(insertQuery, [ 'Eiffel Tower', new Point(48.8582, 2.2945) ], { prepare: true });
const result = await client.execute(selectQuery, ['Eiffel Tower'], { prepare: true });
const row = result.first();
const point = row['coords'];
console.log(point instanceof Point); // true
console.log('x: %d, y: %d', point.x, point.y); // x: 48.8582, y: 2.2945
```

## License

Copyright 2016-2017 DataStax

http://www.datastax.com/terms/datastax-dse-driver-license-terms


[dse]: http://www.datastax.com/products/datastax-enterprise
[cassandra-driver]: https://github.com/datastax/nodejs-driver
[iterable]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols#iterable
[modern-graph]: http://tinkerpop.apache.org/docs/3.2.4/reference/#_the_graph_structure
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[doc-index]: http://docs.datastax.com/en/developer/nodejs-driver-dse/latest/
[api-docs]: http://docs.datastax.com/en/developer/nodejs-driver-dse/latest/api/
[faq]: http://docs.datastax.com/en/developer/nodejs-driver-dse/latest/faq/
[promise]: https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Promise
[async-fn]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function