# Native protocol

The native protocol defines the format of the binary messages exchanged between the driver and Cassandra over TCP. As a
driver user what you need to be aware of is that some Cassandra features are only available with a specific protocol
version, but if you are interested in the technical details you can check [the specification in the Cassandra
codebase](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=tree;f=doc;hb=HEAD).

## Controlling the protocol version 

By default, the driver uses the highest protocol version supported by the driver and the Cassandra cluster. If you want
to limit the protocol version to use, you do so in the protocol options.

```javascript
const cassandra = require('cassandra-driver');
const protocolVersion = cassandra.types.protocolVersion;
const client = new cassandra.Client({
   contactPoints: [ 'host1', 'host2' ],
   protocolOptions: { maxVersion: protocolVersion.v3 }
});
```

## Mixed cluster versions and rolling upgrades 

The protocol version used between the client and the Cassandra cluster is negotiated upon establishing the first
connection. For clusters with nodes running mixed versions of Cassandra and during rolling upgrades this could represent
an issue that could lead to limited availability.

To exemplify the above, consider a mixed cluster having nodes running either Cassandra 2.1 or 2.0.

- The first contact point is a 2.1 host, so the driver negotiates native protocol version 3
- While connecting to the rest of the cluster, the driver contacts a 2.0 host using native protocol version 3, which
fails; an error is logged and this host will be permanently ignored.

For these scenarios, mixed version clusters and rolling upgrades, it is strongly recommended to set the maximum protocol
version when initializing the client:

```javascript
const client = new Client({
   contactPoints: [ 'host1', 'host2' ],
   protocolOptions: { maxVersion: protocolVersion.v2 }
});
```

And switching it to the highest protocol version once the upgrade is completed, by leaving the maximum protocol version
unspecified or by using `protocolVersion.maxSupported`:

```javascript
const client = new Client({
   contactPoints: [ 'host1', 'host2' ],
   protocolOptions: { maxVersion: protocolVersion.maxSupported }
});
```