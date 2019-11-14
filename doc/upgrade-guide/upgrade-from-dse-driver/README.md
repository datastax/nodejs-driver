# Upgrading from the DSE Driver

This guide is intended for users of the DSE driver that plan to migrate to the `cassandra-driver`.

The `cassandra-driver` now supports all DataStax products and features, such as Unified Authentication, 
Kerberos, geo types and graph traversal executions, allowing you to use a single driver for Apache Cassandra, DSE or 
other DataStax products.

Upgrading from `dse-driver` to `cassandra-driver ` can be as simple as changing the import statement to point to the 
dse package:

```javascript
const { Client } = require('dse-driver');

const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1' 
});
```

Becomes:

```javascript
const { Client } = require('cassandra-driver');

const client = new Client({
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1' 
});
```

## Submodules

Most of the child modules are in the same path. 

```javascript
const { auth, types, geometry, policies, mapping } = require('dse-driver');
```

Becomes: 

```javascript
const { auth, types, geometry, policies, mapping } = require('cassandra-driver');
```

The only notable module path distinctions are Graph and Search types that are under `datastax` module.

```javascript
const { graph, search } = require('dse-driver');
```

Becomes:

```javascript
const { datastax } = require('cassandra-driver');
const { graph, search } = datastax;
```