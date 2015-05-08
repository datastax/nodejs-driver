# Node.js Driver usage samples

These folder contains examples on how to use some features of the Node.js Driver for [Apache Cassandra][cassandra].

You should also visit the [Documentation][doc-index] and [FAQ][faq].

Each example is generally structured in a way were the `Client` is connected at the beginning and shutdown at the end.
While this is suitable for example single script purposes, you should reuse a single `Client` instance and
only call client.shutdown() once in your application's lifetime.

If you have any doubt regarding these examples, feel free to post your question in the [mailing list][mailing-list].

[cassandra]: http://cassandra.apache.org/
[doc-index]: http://www.datastax.com/documentation/developer/nodejs-driver/2.0/
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user