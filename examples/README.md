# Node.js Driver usage samples

These folder contains examples on how to use some features of the Node.js Driver for [Apache Cassandra][cassandra].

You should also visit the [Documentation][doc-index] and [FAQ][faq].

## Code samples
- Basic
  - [Connect](basic/basic-connect.js)
  - [Execute with promise-based API](basic/basic-execute.js)
  - [Execute using callbacks](basic/basic-execute-flow.js)
- Metadata
  - [Get hosts information](metadata/metadata-hosts.js)
  - [Get keyspaces information](metadata/metadata-keyspaces.js)
  - [Get table information](metadata/metadata-table.js)
- Graph
  - [Working with DSE Graph](graph/intro.js)
- Data types
  - [Working with geospatial types](geotypes/intro.js)
  - [Working with user-defined types (UDT)](udt/udt-insert-select.js)
  - [Working with tuples](tuple/tuple-insert-select.js)
- Query tracing
  - [Retrieving the trace of a query request](tracing/retrieve-query-trace.js)

Each example is generally structured in a way were the `Client` is connected at the beginning and shutdown at the end.
While this is suitable for example single script purposes, you should reuse a single `Client` instance and
only call client.shutdown() once in your application's lifetime.

If you have any doubt regarding these examples, feel free to post your question in the [mailing list][mailing-list].

[cassandra]: http://cassandra.apache.org/
[doc-index]: http://docs.datastax.com/en/developer/nodejs-driver/latest/
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[faq]: http://docs.datastax.com/en/developer/nodejs-driver/latest/faq/