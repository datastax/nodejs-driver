# TLS/SSL

You can secure traffic between the driver and Apache Cassandra with TLS/SSL. There are two aspects to that:

- Client-to-node encryption, where the traffic is encrypted and the client verifies the identity of the 
Apache Cassandra nodes it connects to.
- Optional client certificate authentication, where Apache Cassandra nodes also verify the identity of the client.

This section describes the driver-side configuration, it assumes that you've already configured SSL encryption in 
Apache Cassandra, you can checkout the [server documentation that covers the basic procedures][client-to-node].

## Driver configuration

Use `sslOptions` property in the [`ClientOptions`](/api/type.ClientOptions/) to enable client TLS/SSL 
encryption:

```javascript
const client = new Client({ contactPoints, localDataCenter, sslOptions: { rejectUnauthorized: true }});

await client.connect();
```

You can define the same object properties as the options in the [standard Node.js `tls.connect()` 
method][tls-connect-options]. The main difference is that server certificate validation against the list of supplied
CAs is disabled by default. You should specify `rejectUnauthorized: true` in your settings to enable it.

### Enabling client certificate authentication

Much like in [Node.js standard tls module][nodejs-tls], you can use `cert` and `key` properties to provide the 
certificate chain and private key. Additionally, you can override the trusted CA certificates using `ca` property:

```javascript
const sslOptions = {
  // Necessary only if the server requires client certificate authentication.
  key: fs.readFileSync('client-key.pem'),
  cert: fs.readFileSync('client-cert.pem'),
  
  // Necessary only if the server uses a self-signed certificate.
  ca: [ fs.readFileSync('server-cert.pem') ],
  
  rejectUnauthorized: true
};

const client = new Client({ contactPoints, localDataCenter, sslOptions });
```

[client-to-node]: https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html
[tls-connect-options]: https://nodejs.org/api/tls.html#tls_tls_connect_options_callback
[nodejs-tls]: https://nodejs.org/api/tls.html 