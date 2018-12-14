# Null and unset values

To complete a distributed DELETE operation, Cassandra replaces it with a special value called a tombstone which can be propagated to replicas. When inserting or updating a field, you can set a certain field to null as a way to clear the value of a field, and it is considered a DELETE operation. In some cases, you might insert rows using null for values that are not specified, and even though our intention is to leave the value empty, Cassandra represents it as a tombstone causing unnecessary overhead.

To avoid tombstones, in previous versions of Cassandra, you used different query combinations only containing the fields that had a value.

## Unset 

Cassandra 2.2 introduced the concept of unset for a parameter value. At the server level, this field value is not considered. This can be represented in the driver with the field unset.

```javascript
const query = 'INSERT INTO tbl1 (id, val1) VALUES (?, ?)';
client.execute(query, [ id, cassandra.types.unset ]);
```

You can even use the undefined primitive type to represent unset values on an INSERT or UPDATE operation.

```javascript
const query = 'INSERT INTO tbl1 (id, val1) VALUES (?, ?)';
client.execute(query, [ id, undefined ]);
```

The driver allows you to control the usage of undefined as unset with the flag useUndefinedAsUnset, which is set to true
in driver versions 3.0 and above:

```javascript
const client = new Client({
  contactPoints,
  localDataCenter,
  encoding: { useUndefinedAsUnset: false } 
});
```
