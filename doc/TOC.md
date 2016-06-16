The DSE driver is built on top of [Node.js CQL driver for Apache Cassandra][cassandra-driver] and provides the
following extensions for DataStax Enterprise:

- [Authenticator implementations](module-auth.html) that use the authentication scheme negotiation in the server-side
DseAuthenticator
- [Geospatial types](module-geometry.html) support
- [DSE Graph](module-graph.html) integration


## Getting Started

`Client` inherits from the CQL driver counterpart `Client`.  All CQL features available to `Client` (see the
[CQL driver manual][core-manual]) can also be used with the `Client` of the DSE module.

```javascript
const dse = require('dse-driver');
const client = new dse.Client({
  contactPoints: ['h1', 'h2'],
  keyspace: 'ks1',
  graphOptions: { name: 'graph1' }
});
const query = 'SELECT email, last_name FROM users WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  assert.ifError(err);
  console.log('User email ' + result.rows[0].email);
});
```

Additionally, the DSE module exports the submodules from the CQL driver, so you just need to import one module to access
all DSE and Cassandra types.

For example:
```javascript
const Uuid = dse.types.Uuid;
let id = Uuid.random();
```

### Graph

[`Client` includes a `executeGraph() method`](Client.html#executeGraph) to execute graph queries:

```javascript
client.executeGraph('g.V()', function (err, result) {
  assert.ifError(err);
  const vertex = result.first();
  console.log(vertex.label);
});
```

[cassandra-driver]: https://github.com/datastax/nodejs-driver
[core-manual]: http://docs.datastax.com/en/developer/nodejs-driver/3.0/common/drivers/introduction/introArchOverview.html