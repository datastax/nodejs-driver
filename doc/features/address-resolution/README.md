# Address resolution

The driver auto-detects new Cassandra nodes when they are added to the cluster by means of server-side push
notifications and checking the system tables.

For each node, the address the driver receives the address set as [`rpc_address` in the node's cassandra.yaml
file](https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__rpc_address)
(or [`broadcast_rpc_address` when 
defined](https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__rpc_address)).
In most cases, this is the correct value, however, sometimes the addresses received in this manner are either not
reachable directly by the driver or are not the preferred address to use. A common such scenario is a multi-datacenter
deployment with a client connecting using the private IP address to the local datacenter (to reduce network costs) and
the public IP address for the remote datacenter nodes.

## The AddressTranslator interface 

The `AddressTranslator` interface allows you to deal with such cases, by transforming the address sent by a Cassandra
node to another address to be used by the driver for connection.

```javascript
function MyAddressTranslator() {
}

util.inherits(MyAddressTranslator, AddressTranslator);

MyAddressTranslator.prototype.translate = function (address, port, callback) {
   // Your custom translation logic.
};
```

You then configure the driver to use your AddressTranslator implementation in the client options.

```javascript
const client = new Client({
   contactPoints: ['1.2.3.4'], 
   policies: { 
      addressResolution: new MyAddressTranslator() 
   }
});
```

Note: The contact points provided while creating the Client are not translated, only addresses retrieved from or sent by
Cassandra nodes are.

## EC2 multi-region 

The `EC2MultiRegionTranslator` class is provided out of the box. It helps optimize network costs when your
infrastructure (both Cassandra nodes and clients) is distributed across multiple Amazon EC2 regions:

- a client communicating with a Cassandra node in the same EC2 region should use the node’s private IP address (which is
less expensive);
- a client communicating with a node in a different region should use the public IP address.

To use this implementation, provide an instance when initializing the `Client` object.

```javascript
const dse = require('dse-driver');
const addressResolution = dse.policies.addressResolution;
const client = new dse.Client({
   contactPoints: ['1.2.3.4'], 
   policies: { 
      addressResolution: new addressResolution.EC2MultiRegionTranslator() 
   }
});
```

The `Client` class performs a reverse DNS lookup of the origin address to find the domain name of the target instance.
Then it performs a forward DNS lookup of the domain name; the EC2 DNS does the private to public switch automatically
based on location.
