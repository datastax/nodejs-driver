# Authentication

The driver includes three authentication providers:

- `PlainTextAuthProvider`: Plain-text authentication for Apache Cassandra and DSE.
- `DsePlainTextAuthProvider`: Plain-text authentication for DSE unified auth.
- `DseGssapiAuthProvider`: GSSAPI authentication for DSE.

In case you are using plain-text authentication on the server, you can set the `credentials` when creating the 
`Client` instance.

```javascript
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
  contactPoints,
  localDataCenter,
  credentials: { username: 'my_username', password: 'my_p@ssword1!' }
});
```

## Setting the authentication provider

For other authentication methods, you can configure the provider in the `Client` options:

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints,
  localDataCenter,
  authProvider: new cassandra.auth.DseGssapiAuthProvider()
});
```

Note that to use the `DseGssapiAuthProvider` you need to add the dependency to `kerberos` version `~1.0.0` in your 
application.

## DSE Unified Authentication

DSE Unified Authentication allows you to:

- Proxy Login: Authenticate using a fixed set of authentication credentials but allow authorization of resources
based on another user id.
- Proxy Execute: Authenticate using a fixed set of authentication credentials but execute requests based on
another user id.

### Proxy Login

Proxy login allows you to authenticate with a user but act as another one. You need to ensure the authenticated
user has the permission to use the authorization of resources of the other user. 

In the following example, we allow user "ben" to authenticate but use the authorization of "alice".

We grant login permission to "ben" by using a `GRANT` CQL query:

```
GRANT PROXY.LOGIN ON ROLE 'alice' TO 'ben'
```

Once "ben" is granted proxy login as "alice":

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: [ 'host1', 'host2' ],
  authProvider: new cassandra.auth.DsePlainTextAuthProvider('ben', 'ben', 'alice')
});

// All requests will be executed using the authorizationId 'alice'
client.execute(query, params, { prepare: true });
```

### Proxy Execute

Proxy execute allows you to execute requests as another user than the authenticated one. You need to ensure the 
authenticated user has the permission to use the authorization of resources of the specified user.

In the following example will allow the user "ben" to execute requests as "alice":

We grant execute permission to "ben" by using a `GRANT` CQL query:

``` 
GRANT PROXY.EXECUTE on role user1 to server
```

Once "ben" is granted permission to execute queries as "alice":


```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: [ 'host1', 'host2' ],
  authProvider: new cassandra.auth.DsePlainTextAuthProvider('ben', 'ben')
});

// The following requests will be executed as 'alice'
client.execute(query, params, { prepare: true, executeAs: 'alice' });
```

Please see the [official documentation][auth-doc] for more details.

[auth-doc]: https://docs.datastax.com/en/latest-dse/datastax_enterprise/unifiedAuth/unifiedAuthTOC.html