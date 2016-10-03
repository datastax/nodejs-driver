# User-defined functions and aggregates

Cassandra 2.2 introduced [user-defined functions](https://issues.apache.org/jira/browse/CASSANDRA-7395) (UDF) and
aggregates support. You access UDF and aggregate values in your queries like regular columns:

```javascript
const query = 'SELECT avg(salary) as salary FROM employees';
client.execute(query, function (err, result) {
    assert.ifError(err);
    const row = result.first();
    console.log('Average salary %d', row.salary); 
});
```

The driver also exposes [UDFs and aggregates metadata information][metadata-api], for example let's see how to retrieve the metadata
information of a UDF named iif, that takes a boolean and int parameter.

```javascript
client.metadata.getFunction('ks1', 'iif', ['boolean', 'int'], function (err, udf) {
    if (err) return console.error(err);
    console.log('Function metadata %j', udf);
});
```

[metadata-api]: /api/module.metadata/

