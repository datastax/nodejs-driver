# DataStax Node.js Driver usage samples

This folder contains examples on how to use some features of the DataStax Node.js Driver.

You should also visit the [Documentation][doc-index] and [FAQ][faq].

## Code samples
- Basic
  - [Connect](basic/basic-connect.js)
  - [Execute with promise-based API](basic/basic-execute.js)
  - [Execute using callbacks](basic/basic-execute-flow.js)
- Mapper
  - [Insert and retrieve using the Mapper](mapper/mapper-insert-retrieve.js)
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
- Concurrent execution
  - [Insert multiple rows in a table from an Array using built-in
  method](concurrent-executions/execute-concurrent-array.js)
  - [Execute multiple queries in a loop with a defined concurrency level](concurrent-executions/execute-in-loop.js)

Each example is generally structured in a way where the `Client` is connected at the beginning and shutdown at the end.
While this is suitable for example single script purposes, you should reuse a single `Client` instance and
only call `client.shutdown()` when exiting your application.

If you have any questions regarding these examples, feel free to post your questions in the [mailing list][mailing-list].

[dse]: https://www.datastax.com/products/datastax-enterprise
[doc-index]: https://docs.datastax.com/en/developer/nodejs-driver/latest/
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[faq]: https://docs.datastax.com/en/developer/nodejs-driver/latest/faq/