# Connecting to your DataStax Apollo database using a secure connection bundle

## Quickstart

Use the `ClientOptions` property `cloud` to connect to your [DataStax Apollo database on Constellation] using 
your secure connection bundle (`secure-connect-DATABASE_NAME.zip`) and `credentials` property to provide your [CQL 
credentials].

Here is an example of the minimum configuration needed to connect to your DataStax Apollo database using the
secure connection bundle:

```javascript
const client = new Client({
  cloud: { secureConnectBundle: 'path/to/secure-connect-DATABASE_NAME.zip' },
  credentials: { username: 'user_name', password: 'p@ssword1' }
});
```

## Configurable settings when using a secure connection bundle

You can configure your `Client` instance using other `ClientOptions` properties, for example:

```javascript
const client = new Client({
  cloud: { secureConnectBundle: 'path/to/secure-connect-DATABASE_NAME.zip' },
  credentials: { username: 'user_name', password: 'p@ssword1' },
  keyspace: 'my_ks'
});
```

Note that `contactPoints` and `sslOptions` should not be set when using `secureConnectBundle`.

[DataStax Apollo database on Constellation]: https://constellation.datastax.com/
[CQL credentials]: https://cassandra.apache.org/doc/latest/cql/security.html#cql-roles