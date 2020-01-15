# Graph support

`Client` includes the `executeGraph()` method to execute graph queries:

```javascript
const client = new cassandra.Client({
  contactPoints: ['host1', 'host2'],
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
```

Parameters are encoded in json, thus will ultimately use their json representation (`toJSON` if present,
otherwise object representation).

You can use results from previous queries as parameters to subsequent queries.  For example, if you want to use the id
of a vertex returned in a previous query for making a subsequent query:

```javascript
let result = await client.executeGraph('g.V().hasLabel("person").has("name", "marko")');
const vertex = result.first();
result = await client.executeGraph('g.V(vertexId).out("knows").values("name")', {vertexId: vertex.id });
const names = result.toArray();
console.log(names); // [ 'vadas', 'josh' ]
```

### Prepared statements

Prepared graph statements are not supported by DSE Graph yet (they will be added in the near future).

[iterable]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols#iterable
[modern-graph]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure