The DSE driver is built on top of [Node.js CQL driver for Apache Cassandra][cassandra-driver] and provides the
following extensions for DataStax Enterprise:

- [Authenticator implementations](module-auth.html) that use the authentication scheme negotiation in the server-side
DseAuthenticator
- [Geospatial types](module-geometry.html) support
- [DSE Graph](module-graph.html) integration


## Getting Started

[`Client`](Client.html) inherits from the CQL driver counterpart. All CQL features available to `Client` (see the 
[CQL driver manual][core-manual]) can also be used with the `Client` of the DSE module.

```javascript
const dse = require('dse-driver');
const client = new dse.Client({
  contactPoints: ['h1', 'h2']
});
```

```javascript
const query = 'SELECT name, email FROM users WHERE key = ?';
client.execute(query, [ 'someone' ])
  .then(result => console.log('User with email %s', result.rows[0].email));
```

Alternatively, you can use the callback-based execution for all asynchronous methods of the API.

```javascript
client.execute(query, [ 'someone' ], function(err, result) {
  assert.ifError(err);
  console.log('User with email %s', result.rows[0].email);
});
```

The `dse-driver` module also exports the submodules from the CQL driver, so you only need to import one module to access
all DSE and Cassandra types.

For example:
```javascript
const dse = require('dse-driver');
const Uuid = dse.types.Uuid;
```

### Graph

[`Client` includes a `executeGraph() method`](Client.html#executeGraph) to execute graph queries:

```javascript
// executeGraph() method returns a Promise when no callback has been provided
const result = await client.executeGraph('g.V()');
const vertex = result.first();
console.log(vertex.label);
```

[cassandra-driver]: https://github.com/datastax/nodejs-driver
[core-manual]: http://docs.datastax.com/en/developer/nodejs-driver/latest/