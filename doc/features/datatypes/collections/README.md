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

const client = new cassandra.Client(options);
```

This way, when encoding or decoding map or set values, the driver uses those constructors:

```javascript
client.execute('SELECT map_val FROM tbl')
  .then(function (result) {
    console.log(result.rows[0]['map_val'] instanceof Map); // true
  });
```