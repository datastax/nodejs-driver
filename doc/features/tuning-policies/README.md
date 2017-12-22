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

The default load-balancing policy is the `TokenAwarePolicy` with `DCAwareRoundRobinPolicy` as a child policy. It may
seem complex but it actually isn't: The policy yields local replicas for a given key and, if not available, it yields
nodes of the local datacenter in a round-robin manner.

### Setting the load-balancing policy

To use a load-balancing policy, you pass it in as a clientOptions object to the `Client` constructor.

```javascript
// You can specify the local dc relatively to the node.js app
const localDatacenter = 'us-east';
const loadBalancingPolicy = new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy(localDatacenter); 
const clientOptions = {
   policies : {
      loadBalancing : loadBalancingPolicy
   }
}; 
const client = new cassandra.Client(clientOptions);
```

### Implementing a custom load-balancing policy

The built-in policies in the Node.js driver cover most common use cases. In the rare case that you need to implement
your own policy you can do it by inheriting from one of the existent policies or the abstract `LoadBalancingPolicy`
class.

You have to take into account that the same policy is used for all queries in order to yield the hosts in correct order.

The load-balancing policies are implemented using the [Iterator
Protocol](https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Iteration_protocols#iterator), a convention for
lazy iteration allowing to produce only the next value in the series without producing a full Array of values. Under
ECMAScript 2015, it enables you to use the new [generators][generators].

**Example**: A policy that selects every node except an specific one.

Note that this policy is a sample and it is not intended for production use. Use datacenter-based policies instead.

```javascript
function BlackListPolicy(blackListedHost, childPolicy) {
  this.blackListedHost = blackListedHost;
  this.childPolicy = childPolicy;
}

util.inherits(BlackListPolicy, LoadBalancingPolicy);

BlackListPolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  //initialize the child policy
  this.childPolicy.init(client, hosts, callback);
};

BlackListPolicy.prototype.getDistance = function (host) {
  return this.childPolicy.getDistance(host);
};

BlackListPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  const self = this;
  this.childPolicy.newQueryPlan(keyspace, queryOptions, function (iterator) {
    callback(self.filter(iterator));
  });
};

BlackListPolicy.prototype.filter = function (childIterator) {
  const self = this;
  return {
    next: function () {
      var item = childIterator.next();
      if (!item.done && item.value.address === self.blackListedHost) {
        // skip
        return this.next();
      }
      return item;
    }
  };
};
```

Or you can use [ES2015 Generators][generators]:

```javascript
BlackListPolicy.prototype.filterES6 = function* (childIterator) {
  for (let host of childIterator) {
    if (host.address === this.blackListedHost) {
      continue;
    }
    yield host;
  }
};
```

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

A default and base retry policy is included, along with `IdempotenceAwareRetryPolicy` that considers query idempotence.

### Query idempotence

Note that the current behaviour of the driver allows the `RetryPolicy` to retrieve the query idempotence as part of the
information and take a decision whether to retry the execution or not. In future versions, the driver will rethrow the
error back to the consumer for non-idempotent queries, without using the `RetryPolicy` for this case.

[generators]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator
[OperationInfo]: /api/module.policies/module.retry/type.OperationInfo/