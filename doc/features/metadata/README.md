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
client.metadata.getTable('ks1', 'table1', function (err, tableInfo) {
   if (!err) {
      console.log('Table %s', table.name);
      table.columns.forEach(function (column) {
         console.log('Column %s with type %j', column.name, column.type);
      });
   }
});
```

When retrieving the same table definition concurrently, the driver queries once and invokes all callbacks with the
retrieved information.