# DataStax Enterprise Node.js Driver usage samples

These folder contains examples on how to use some features of the [DataStax Enterprise][dse] with the Node.js Driver.

You should also visit the Documentation and FAQ sections.

## Code samples
- Basic
  - [Connect](basic/basic-connect.js)
  - [Execute with nested callbacks](basic/basic-execute.js)
  - [Execute using async series](basic/basic-execute-flow.js)
- Geospatial types
  - [Working with geospatial types](geotypes/intro.js)
- Graph types
  - [Working with DSE Graph](graph/intro.js)

Each example is generally structured in a way were the `Client` is connected at the beginning and shutdown at the end.
While this is suitable for example single script purposes, you should reuse a single `Client` instance and
only call `client.shutdown()` once in your application's lifetime.

[dse]: http://www.datastax.com/products/datastax-enterprise
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user