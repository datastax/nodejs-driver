# Parameterized queries

You can bind the values of parameters in a prepared statement either by _position_ or by using _named_ markers.

## Positional parameterized query 

When using positional parameters, the query parameters must be provided as an Array.

```javascript
const query = 'INSERT INTO artists (id, name) VALUES (?, ?)';
// Parameters by marker position
const params = ['krichards', 'Keith Richards'];
client.execute(query, params, { prepare: true }, callback);
```

##  Named parameterized query 

You declare the named markers in your queries and use a JavaScript object properties to define the parameters, with
the `Object` property names matching the parameters names.

```javascript
const query = 'INSERT INTO artists (id, name) VALUES (:id, :name)';
// Parameters by marker name
const params = { id: 'krichards', name: 'Keith Richards' };
client.execute(query, params, { prepare: true }, callback);
```

Defining named markers in your queries is supported in Cassandra 2.0 or greater for prepared statements and
Cassandra 2.1 or greater for non-prepared statements.

