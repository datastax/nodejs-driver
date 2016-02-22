The DSE driver is built on top of [Node.js CQL driver for Apache Cassandra][cassandra-driver] and provides the
following extensions for DataStax Enterprise:

- [Authenticator implementations](module-auth.html) that use the authentication scheme negotiation in the server-side
DseAuthenticator
- [Geospatial types](module-geometry.html) support
- [DSE Graph](module-graph.html) integration


## Getting Started

`DseClient` inherits from the CQL driver counterpart `Client`.  All CQL features available to  `Client` (see the
[CQL driver manual][core-manual]) can also be used with `DseClient`.

```javascript
const dse = require('dse-driver');
const client = new dse.DseClient({ contactPoints: ['h1', 'h2'], keyspace: 'ks1'});
const query = 'SELECT email, last_name FROM user_profiles WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  assert.ifError(err);
  console.log('got user profile with email ' + result.rows[0].email);
});
```

Additionally, the dse module exports the submodules from the CQL driver, so you just need to import one module to access
all DSE and Cassandra types.

For example:
```javascript
const dse = require('dse-driver');
const Uuid = dse.types.Uuid;
```


[cassandra-driver]: https://github.com/datastax/nodejs-driver
[core-manual]: http://docs.datastax.com/en/developer/nodejs-driver/3.0/common/drivers/introduction/introArchOverview.html