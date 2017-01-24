# Frequently Asked Questions

### Which versions of DSE does the driver support?

The driver supports versions from 4.8 to 5 of [DataStax Enterprise][dse].

### How can I upgrade from the Cassandra driver to the DSE driver?

There is a section in the [Getting Started](../getting-started/) page.

### How do I generate a random uuid or a time-based uuid?

Use the [Uuid and TimeUuid classes](/features/datatypes/uuids) inside the types module.

### Can I use a single `Client` instance for graph and CQL?

Yes, you can. You should use [Execution Profiles](../features/execution-profiles/) to define your settings for CQL and
graph workloads, for example: define which datacenter should be used for graph or for CQL.

### Should I create one `Client` instance per module in my application?

Normally you should use one `Client` instance per application. You should share that instance between modules within
your application.

### Should I shut down the pool after executing a query?

No, only call `client.shutdown()` once in your application's lifetime, normally when you shutdown your application.

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

[dse]: http://www.datastax.com/products/datastax-enterprise