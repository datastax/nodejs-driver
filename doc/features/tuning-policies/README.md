# Tuning policies

## Load balancing policy

The load balancing policy interface consists of three methods:

- `#distance(Host host)`: determines the distance to the specified host. The values are `distance.ignored`, 
`distance.local`, and `distance.remote`.
- `#init(client, hosts, callback)`: initializes the policy. The driver calls this method only once and before any other
method calls are made.
- `#newQueryPlan(keyspace, queryOptions, callback)`: executes a callback with the iterator of hosts to use for a query.
Each new query calls this method.

The policies are responsible for yielding a group of nodes in an specific order for the driver to use (if the first
node fails, it uses the next one). There are four load-balancing policies implemented in the driver: 

- `DCAwareRoundRobinPolicy`: a datacenter-aware, round-robin, load-balancing policy. This policy provides round-robin
queries over the node of the local datacenter. It also includes in the query plans returned a configurable number of
hosts in the remote data centers, but those are always tried after the local nodes.
- `RoundRobinPolicy`: a policy that yields nodes in a round-robin fashion.
- `TokenAwarePolicy`: a policy that yields replica nodes for a given partition key and keyspace. The token-aware policy
uses a child policy to retrieve the next nodes in case the replicas for a partition key are not available.
- `WhiteListPolicy`: a policy that wraps the provided child policy but only "allow" hosts from the provided
whilelist. Keep in mind however that this policy defeats somewhat the host auto-detection of the driver. As such, this 
policy is only useful in a few special cases or for testing, but is not optimal in general.

### Default load-balancing policy

The default load-balancing policy is `DefaultLoadBalancingPolicy`. The policy yields local replicas for a given 
key and, if not available, it yields nodes of the local datacenter in a round-robin manner.

## Reconnection policy

The reconnection policy consists of one method:

- `#newSchedule()`: creates a new schedule to use in reconnection attempts.

By default, the driver uses an exponential reconnection policy. The driver includes these two policy classes:

- `ConstantReconnectionPolicy`
- `ExponentialReconnectionPolicy`

## Retry policy

A client may send requests to any node in a cluster whether or not it is a replica of the data being queried.
This node is placed into the coordinator role temporarily. Which node is the coordinator is determined by the load
balancing policy for the cluster. The coordinator is responsible for routing the request to the appropriate replicas.
If a coordinator fails during a request, the driver connects to a different node and retries the request.
If the coordinator knows before a request that a replica is down, it can throw an `UnavailableException`, but if the
replica fails after the request is made, it throws a `TimeoutException`. Of course, this all depends on the consistency
level set for the query before executing it.

A retry policy centralizes the handling of query retries, minimizing the need for catching and handling of exceptions in
 your business code.

The retry policy interface consists of four methods:

- `#onReadTimeout(info, consistency, received, blockFor, isDataPresent)`: determines what to do when the driver
gets a `ReadTimeoutException` response from a Cassandra node.
- `#onUnavailable(info, consistency, required, alive)`: determines what to do when the driver gets an 
`UnavailableException` response from a Cassandra node.
- `#onWriteTimeout(info, consistency, received, blockFor, writeType)`: determines what to do when the driver gets
a `WriteTimeoutException` response from a Cassandra node
- `#onRequestError(info, consistency, err)`: defines whether to retry and at which consistency level on an 
unexpected error, invoked in the following situations:
    - On a client timeout, while waiting for the server response , being the error an instance of 
    `OperationTimedOutError`.
    - On a connection error (socket closed, etc.).
    - When the contacted host replies with an error, such as `overloaded`, `isBootstrapping`, `serverError`, etc. In 
    this case, the error is instance of `ResponseError`

The [operation info][OperationInfo], passed as a parameter to the retry policy methods, exposes the `query` and query 
`options` as properties.

A default and base retry policy are included.

### Query idempotence

Note that as of version 2.0, the configured `RetryPolicy` is not engaged when a query errors with a
`WriteTimeoutException` or request error and the query was not [idempotent][idempotent].

[generators]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator
[OperationInfo]: /api/module.policies/module.retry/type.OperationInfo/
[idempotent]: ../speculative-executions/#query-idempotence
