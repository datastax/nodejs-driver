# Collections

## List and Set 

When reading columns with CQL list or set data types, the driver exposes them as native Arrays. When writing values to a
list or set column, you can pass in a Array.

```javascript
client.execute('SELECT list_val, set_val, double_val FROM tbl')
  .then(function (result) {
    console.log(Array.isArray(result.rows[0]['list_val'])); // true
    console.log(Array.isArray(result.rows[0]['set_val']));  // true
  });
```

## Map 

JavaScript objects are used to represent the CQL map data type in the driver, because JavaScript objects are associative
arrays.

```javascript
client.execute('SELECT map_val FROM tbl')
  .then(function (result) {
    console.log(JSON.stringify(result.rows[0]['map_val'])); // {"key1":1,"key2":2}
  });
```

When using CQL maps, the driver needs a way to determine that the object instance passed as a parameter must be encoded
 as a map. Inserting a map as an object will fail:

```javascript
const query = 'INSERT INTO tbl (id, map_val) VALUES (?, ?)';
const params = [id, {key1: 1, key2: 2}];
client.execute(query, params)
  .catch(function (err) {
    console.log(err) // TypeError: The target data type could not be guessed
  });
```

To overcome this limitation, you should prepare your queries. Preparing and executing statements in the driver does not
require chaining two asynchronous calls, you can set the prepare flag in the query options and the driver will handle
the rest. The previous query, using the prepare flag, will succeed:

```javascript
client.execute(query, params, { prepare: true });
```

### ECMAScript Map and Set support 

The new built-in types in ECMAScript 6, Map and Set, can be used to represent CQL map and set values. To enable this option, you should specify the constructors in the client options.

```javascript
const options = {
  contactPoints,
  localDataCenter,
  encoding: { 
    map: Map,
    set: Set
  }
};

const client = new Client(options);
```

This way, when encoding or decoding map or set values, the driver uses those constructors:

```javascript
client.execute('SELECT map_val FROM tbl')
  .then(function (result) {
    console.log(result.rows[0]['map_val'] instanceof Map); // true
  });
```

### Vector

As of version 4.7.0 the driver also includes support for the vector type available in Cassandra 5.0.  Vectors are represented as instances of
the [Float32Array] class.  For example, to create and write to a vector with three dimensions you might do something like the following:

```javascript
await c.connect()
  .then(() => c.execute("drop keyspace if exists test"))
  .then(() => c.execute("create KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"))
  .then(() => c.execute("create table test.foo(i varint primary key, j vector<float,3>)"))
  .then(() => c.execute("create custom index ann_index on test.foo(j) using 'StorageAttachedIndex'"))

  // Base inserts using simple and prepared statements
  .then(() => c.execute(`insert into test.foo (i, j) values (?, ?)`, [cassandra.types.Integer.fromInt(1), new Float32Array([8, 2.3, 58])]))
  .then(() => c.execute(`insert into test.foo (i, j) values (?, ?)`, [cassandra.types.Integer.fromInt(5), new Float32Array([23, 18, 3.9])], {prepare: true}));
```

[Float32Array]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Float32Array
