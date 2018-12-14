# Cluster and schema metadata

You can retrieve the cluster topology and the schema metadata information using the Node.js driver.

After establishing the first connection, the driver retrieves the cluster topology details and exposes these through
properties of the client object. This information is kept up to date using Cassandra event notifications.

The following example outputs hosts information about your cluster:

```javascript
client.hosts.forEach(function (host) {
   console.log(host.address, host.datacenter, host.rack);
});
```

Additionally, the keyspaces information is already loaded into the `Metadata` object, once the client is connected:

```javascript
console.log(Object.keys(client.metadata.keyspaces));
```

To retrieve the definition of a table, use the `Metadata#getTable()` method:

```javascript
client.metadata.getTable('ks1', 'table1')
  .then(function (tableInfo) {
    console.log('Table %s', table.name);
    table.columns.forEach(function (column) {
       console.log('Column %s with type %j', column.name, column.type);
    });
  });
```

When retrieving the same table definition concurrently, the driver queries once and invokes all callbacks with the
retrieved information.

## Schema agreement

Schema changes need to be propagated to all nodes in the cluster. Once they have settled on a common version, we say
that they are in agreement.

the driver waits for schema agreement after executing a schema-altering query. This is to ensure that subsequent
requests (which might get routed to different nodes) see an up-to-date version of the schema.

```ditaa
 Application             Driver           Server
------+--------------------+------------------+-----
      |                    |                  |
      |  CREATE TABLE...   |                  |
      |------------------->|                  |
      |                    |   send request   |
      |                    |----------------->|
      |                    |                  |
      |                    |     success      |
      |                    |<-----------------|
      |                    |                  |
      |          /--------------------\       |
      |          :Wait until all nodes+------>|
      |          :agree (or timeout)  :       |
      |          \--------------------/       |
      |                    |        ^         |
      |                    |        |         |
      |                    |        +---------|
      |                    |                  |
      |                    |  refresh schema  |
      |                    |----------------->|
      |                    |<-----------------|
      |   complete query   |                  |
      |<-------------------|                  |
      |                    |                  |
```

The schema agreement wait is performed serially, so the `execute()` call will only return after it has completed.

The check is implemented by repeatedly querying system tables for the schema version reported by each node, until they
all converge to the same value. If that doesn't happen within a given timeout, the driver will give up waiting.
The default timeout is `10` seconds, it can be customized when creating the `Client` instance:

```javascript
const client = new Client({
  contactPoints,
  localDataCenter,
  protocolOptions: { maxSchemaAgreementWaitSeconds: 20 }
});
```

After executing a statement, you can check whether schema agreement was successful or timed out:

```javascript
client.execute('CREATE TABLE table1 (id int PRIMARY KEY)')
  .then(rs => {
    console.log(`Is schema in agreement? ${rs.info.isSchemaInAgreement}`);
  });
```

Additionally, you can perform an on-demand check at any time:

```javascript
client.metadata.checkSchemaAgreement()
  .then(agreement => {
    console.log(`Is schema in agreement? ${agreement}`);
  });
```

Note that the on-demand check using `checkSchemaAgreement()` does not retry, it only queries system tables once.