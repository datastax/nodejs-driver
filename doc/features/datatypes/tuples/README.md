# Tuples

Cassandra 2.1 introduced a `tuple` type for CQL.

A tuple is a fixed-length set of typed positional fields. With the driver, you retrieve and store tuples using the 
[`Tuple` class](/api/module.types/class.Tuple/).

For example, given the following table to represent the value of the exchange between two currencies.

```
CREATE TABLE forex (
   name text,
   time timeuuid,
   currencies frozen<tuple<text, text>>,
   value decimal,
   PRIMARY KEY (name, time)
);
```

To retrieve the `Tuple` value:

```
const query = 'SELECT name, time, currencies, value FROM forex where name = ?';
client.execute(query, [name], { prepare: true })
  .then(function (result) {
    result.rows.forEach(function (row) {
      console.log('%s to %s: %s', row.currencies.get(0), row.currencies.get(1), row.value);
    });
  });
```

You use the `get(index)` method to obtain the value at any position or the `values()` method to obtain an `Array`
representation of the `Tuple`.

To create a new `Tuple`, you use the constructor providing the values as parameters.

```javascript
const Tuple = require('dse-driver').types.Tuple;
// Create a new instance of a Tuple.
const currencies = new Tuple('USD', 'EUR');
const query = 'INSERT INTO forex (name, time, currencies, value)  VALUES (?, ?, ?, ?)';
const params = ['market1', TimeUuid.now(), currencies, new BigDecimal(1, 0)];
client.execute(query, params, { prepare: true });
```
