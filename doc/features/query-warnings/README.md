# Query warnings

When a query is considered to be harmful for the overall cluster, Cassandra issues a warning that is written to the
Cassandra logs. From Cassandra 2.2, [these warnings are also returned to the client drivers][protocol-warnings].

In the driver, these warnings are [returned in the ResultSet property information](/api/module.types/class.ResultSet/). The warning is still
written to the [driver logs](/#logging).

[protocol-warnings]: https://issues.apache.org/jira/browse/CASSANDRA-8930
