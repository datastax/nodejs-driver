# Frequently Asked Questions

### Which versions of Cassandra does the driver support?

The latest version of the driver supports any Cassandra version starting with 1.2.

### Which versions of CQL does the driver support?

It supports [CQL version 3](http://cassandra.apache.org/doc/latest/cql/index.html).

### How do I generate a random uuid or a time-based uuid?

Use the [Uuid and TimeUuid classes](/features/datatypes/uuids) inside the types module.

### Should I create one client instance per module in my application?

Normally you should use one `Client` instance per application. You should share that instance between modules within
your application.

### Should I shut down the pool after executing a query?

No, only call `client.shutdown()` once in your application's lifetime.

### How can I use a list of values with the IN operator in a WHERE clause?

To provide a dynamic list of values in a single parameter, use the `IN` operator followed by the question mark
placeholder without parenthesis in the query. The parameter containing the list of values should be of an instance of
Array.

For example:

```javascript
const query = 'SELECT * FROM table1 WHERE key1 = ? AND key2 IN ?';
const key1 = 'param1';
const allKeys2 = [ 'val1', 'val2', 'val3' ];
client.execute(query, [ key1, allKeys2 ], { prepare: true });
```
