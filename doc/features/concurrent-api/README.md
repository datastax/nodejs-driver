# Concurrent Execution API

The DataStax Node.js driver provides a set of [utilities for concurrent query execution](/api/module.concurrent/),
to facilitate executing multiple queries in parallel while controlling the concurrency level.

The concurrent execution API can useful when, for example, you want to insert a large group of rows from an `Array` or
 a `Stream` and evaluate failures, if any, at the end.
 
## Usage samples

### Using a fixed query and an Array of arrays as parameters

When an `Array` of arrays is provided, one query per each item in the `Array` will be executed, using each item as 
parameters.

```javascript
const query = 'INSERT INTO table1 (id, value) VALUES (?, ?)';
const parameters = [[1, 'a'], [2, 'b'], [3, 'c'], ]; // ...
const result = await executeConcurrent(client, query, parameters);
```

### Using a fixed query and a readable stream

When a `Stream` instance is provided the driver will read from the input stream and execute one query per item 
emitted. The driver will throttle reads of the input stream based on the concurrency level configured and the 
amount of current in-flight requests.
 
The `Stream` instance should be a readable, in object mode, and emit `Array` instances. Per each item emitted, one 
query will be executed. 

```javascript
const stream = csvStream.pipe(transformLineToArrayStream);
const result = await executeConcurrent(client, query, stream);
```
 
### Using a different queries

```javascript
const queryAndParameters = [
  { query: 'INSERT INTO videos (id, name, user_id) VALUES (?, ?, ?)',
    params: [ id, name, userId ] },
  { query: 'INSERT INTO user_videos (user_id, id, name) VALUES (?, ?, ?)',
    params: [ userId, id, name ] },
  { query: 'INSERT INTO latest_videos (id, name, user_id) VALUES (?, ?, ?)',
    params: [ id, name, userId ] },
];

const result = await executeConcurrent(client, queryAndParameters);
```

### Execute all queries and deal with execution errors at the end

When setting `raiseOnFirstError` to `false`, the driver will continue to execute the queries even when one or more 
errors are encountered. The returned `Promise` will be resolved and you can inspect the property `errors` to obtain 
each individual error information.

```javascript
const result = await executeConcurrent(client, query, parameters, { raiseOnFirstError: false });

for (let err of result.errors) {
  // ...
}
```

### Defining concurrency level

Use the `concurrencyLevel` option property to set the maximum amount of requests that can be executed simultaneously.
 It defaults to `100`.

```javascript
const result = await executeConcurrent(client, query, parameters, { concurrencyLevel: 200 });
```

### Collecting all the ResultSet instances of each individual execution

In the case you want the driver to collect each individual `ResultSet` instance, you can use the `collectResults` flag.

```javascript
const result = await executeConcurrent(client, query, parameters, { collectResults: true });

for (let rs of result.resultItems) {
  // ...
}
```