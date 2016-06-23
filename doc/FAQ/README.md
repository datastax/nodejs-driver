# Frequently Asked Questions

### Which versions of DSE does the driver support?

The driver supports versions from 4.8 to 5 of [DataStax Enterprise][dse].

### How can I upgrade from the Cassandra driver to the DSE driver?

There is a section in the [Getting Started](../getting-started/) page.

### Where can I find more tutorials or documentation?

All the functionality present in the Cassandra driver is available on the DSE driver, [any tutorial or documentation
that references the DataStax Node.js driver for Apache Cassandra also applies to this driver][core-features].

### Can I use a single `Client` instance for graph and CQL?

Yes, you can. You should use [Execution Profiles](../features/execution-profiles/) to define your settings for CQL and
graph workloads, for example: define which datacenter should be used for graph or for CQL.

### Should I create one `Client` instance per module in my application?

Normally you should use one `Client` instance per application. You should share that instance between modules within
your application.

### Should I shut down the pool after executing a query?

No, only call `client.shutdown()` once in your application's lifetime, normally when you shutdown your application.

[dse]: http://www.datastax.com/products/datastax-enterprise
[core-features]: http://datastax.github.io/nodejs-driver/features/