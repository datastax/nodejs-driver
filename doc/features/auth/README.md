# Authentication

An authentication provider is included in the driver to connect to a cluster using plain-text authentication.

You can set the `credentials` to connect to an Apache Cassandra cluster secured using a `PasswordAuthenticator` or to 
a DSE cluster secured with `DseAuthenticator`, with plain-text authentication as default scheme.

```javascript
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
  contactPoints,
  localDataCenter,
  credentials: { username: 'my_username', password: 'my_p@ssword1!' }
});
```
