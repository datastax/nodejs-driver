# Node.js Driver Extensions for DataStax Enterprise

This driver is built on top of [Node.js CQL driver for Apache Cassandra][cassandra-driver] and provides the following
extensions for DataStax Enterprise:

* `Authenticator` implementations that use the authentication scheme negotiation in the server-side `DseAuthenticator`;
* encoders for geospatial types which integrate seamlessly with the driver;
* DSE graph integration.

[cassandra-driver]: https://github.com/datastax/nodejs-driver


## Installation

The driver is distributed as a binary tarball, to make this module available to other projects run the following
command in the extracted directory:

```bash
npm link
```

The module may then be used in another project by using the following:

```bash
npm link cassandra-driver-dse
```


## Getting Started

`DseClient` wraps the CQL driver counterpart `Client`.  All CQL features available to  `Client` (see the
[CQL driver manual][core-manual]) can also be used with `DseClient`.

```javascript
var dse = require('cassandra-driver-dse');
var client = new dse.DseClient({ contactPoints: ['h1', 'h2'], keyspace: 'ks1'});
var query = 'SELECT email, last_name FROM user_profiles WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  assert.ifError(err);
  console.log('got user profile with email ' + result.rows[0].email);
});
```

[core-manual]: http://docs.datastax.com/en//developer/nodejs-driver/3.0/common/drivers/introduction/introArchOverview.html


## Authentication

For clients connecting to a DSE cluster secured with `DseAuthenticator`, two authentication providers are included:

* `DsePlainTextAuthProvider`: plain-text authentication;
* `DseGSSAPIAuthProvider`: GSSAPI authentication;

To configure a provider, pass it when initializing a cluster:

```javascript
var dse = require('cassandra-driver-dse');
var client = new dse.DseClient({ contactPoints: ['h1', 'h2'], keyspace: 'ks1',
                                 authProvider: new dse.auth.DseGssapiAuthProvider()});
```

See the jsdoc of each implementation for more details.


## Geospatial types

DSE 5.0 comes with a set of additional CQL types to represent geospatial data: `PointType`, `LineStringType`,
`PolygonType` and `CircleType`.

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes encoders and representations of these types in the `geometry` module that can be used directly
as parameters in queries:

```javascript
var Point = require('cassandra-driver-dse').geometry.Point;

client.execute("INSERT INTO points_of_interest (name, coords) VALUES (?, ?)", ['Eiffel Tower', new Point(48.8582, 2.2945)], function (err, result) {
    client.execute("SELECT coords FROM points_of_interest WHERE name = 'Eiffel Tower'", function (err, result) {
        var row = result.first();
        var point = row.coords;
        console.log('x: %d, y: %d', point.x, point.y); // x: 48.8582, y: 2.2945
    });
});
```


## Graph

`DseClient` includes a `executeGraph` to execute graph queries:

```javascript
client.executeGraph("system.createGraph('demo').ifNotExist().build()", function (err, result) {
    client.executeGraph("g.addV(label, 'test_vertex')", null, {graphName: 'demo'}, function (err, result) {
      client.executeGraph("g.V()", null, {graphName: 'demo'}, function (err, result) {
        var vertex = result.first();
        console.log(vertex.label); // test_vertex
      });
    });
});
```

### Graph Options

You can set default graph options when initializing `DseClient` which will be used for all graph statements.  For
example, to avoid needing to provide a `graphName` option in each `executeGraph` call:

```javascript
var dse = require('cassandra-driver-dse');
var client = new dse.DseClient({ contactPoints: ['h1', 'h2'], keyspace: 'ks1',
                                 graphOptions: {name: 'demo'}});
```

These options may be overriden at a statement level by providing them in the `options` parameter of `executeGraph` as
was shown in the first example.


### Handling Results

Graph queries return a `GraphResultSet`, which like the CQL core driver's [`ResultSet`][result-set] is an iterable of
rows.  The format of the data returned is dependent on the data requested.  For example, the payload representing edges
will be different than those that represent vertices using the ['modern'][modern] graph:

```javascript
// Creating the 'modern' graph
var query =
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

client.executeGraph(query, function (err, result) {
  assert.ifError(err);
});
```

```javascript
// Handling Edges
client.executeGraph('g.E()', function (err, result) {
  result.forEach(function(edge) {
    console.log(edge.id); // [an id representing the edge]
    console.log(edge.type); // edge
    console.log(edge.label); // created
    console.log(edge.properties.weight); // 0.4
    console.log(edge.outVLabel); // person
    console.log(edge.outV); // [an id representing the outgoing vertex]
    console.log(edge.inVLabel); // software
    console.log(edge.inV); // [an id representing the incoming vertex]
  });
});
```

```javascript
// Handling Vertices
client.executeGraph("g.V().hasLabel('person')", function (err, result) {
  console.log(err);
  result.forEach(function(vertex) {
    console.log(vertex.id); // [an id representing the vertex]
    console.log(vertex.type); // vertex
    console.log(vertex.label); // person
    console.log(vertex.properties.name[0].value); // marko
    console.log(vertex.properties.age[0].value); // 29
  });
});
```

[result-set]: http://docs.datastax.com/en/drivers/nodejs/3.0/module-types-ResultSet.html
[modern]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure


### Parameters

Unlike CQL queries which support both postional and named parameters, graph queries only support named parameters.
As a result of this, parameters must be passed in as an object:

```javascript
client.executeGraph("g.addV(label, vertexLabel, 'name', username)", {vertexLabel: 'person', username: 'marko'}, function (err, result) {
  var vertex = result.first();
  // ...
});
```

Parameters are encoded in json, thus will ultimately use their json representation (`toJSON` if present,
otherwise object representation).

You can use results from previous queries as parameters to subsequent queries.  For example, if you want to use the id
of a vertex returned in a previous query for making a subsequent query:

```javascript
client.executeGraph("g.V().hasLabel('person').has('name', 'marko')", function (err, result) {
  var vertex = result.first();
  client.executeGraph("g.V(vertexId).out('knows').values('name')", {vertexId: vertex.id}, function (err, result) {
    var names = result.toArray();
    console.log(names); // [ 'vadas', 'josh' ]
  });
});
```


### Prepared statements

Prepared graph statements are not supported by DSE yet (they will be added in the near future).