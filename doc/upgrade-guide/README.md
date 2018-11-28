# Upgrade guide

The purpose of this guide is to detail the changes made by the successive versions of the DataStax Node.js Driver that 
are relevant to for an upgrade from prior versions.

If you have any questions or comments, you can [post them on the mailing list][mailing-list].

## 2.0

The following is a list of changes made in version 2.0 of the driver that are relevant when upgrading from version 1.x.

### localDataCenter is now a required Client option

When using `DseLoadBalancingPolicy`, which is used by default, or `DCAwareRoundRobinPolicy` a local data center must 
now be provided to the `Client` options parameter as `localDataCenter`.  This is necessary to prevent routing 
requests to nodes in remote data centers unintentionally.

### Removed `createAsAWrapper()` method from `DseLoadBalancingPolicy` class

Previously, we supported the use of the `DseLoadBalancingPolicy` as a wrapper of a child policy. The new 
implementation of the `DseLoadBalancingPolicy` provides replica ordering using "Power of Two Random Choices" algorithm 
and removes the possibility of chaining policies.

### Changes to the retry and load-balancing policies

`ExecutionOptions` is introduced as a wrapper around the `QueryOptions`.
The `ExecutionOptions` contains getter methods to obtain the values of each option, defaulting to the execution profile
options or the ones defined in the `ClientOptions`. Previously, a shallow copy of the provided `QueryOptions` was 
used, resulting in unnecessary allocations and evaluations.

The `LoadBalancingPolicy` and `RetryPolicy` base classes changed method signatures to take `ExecutionOptions` instances 
as argument instead of `QueryOptions`.

Note that no breaking change was introduced for execution methods such as `Client#execute()`, `Client#batch()`, 
`Client#eachRow()` and `Client#stream()`. This change only affects custom implementations of the policies.

### Query idempotency and retries

The configured `RetryPolicy` is not engaged when a query errors with a `WriteTimeoutException` or request error and 
the query was not idempotent.

In order to control the possibility of retrying when an timeout/error is encountered, you must mark the query as 
idempotent. You can define it at `QueryOptions` level when calling the execution methods.

```javascript
client.execute(query, params, { prepare: true, isIdempotent: true })
```

Additionally, you can define the default idempotence for all executions when creating the `Client` instance:

```javascript
const client = new Client({ contactPoints, queryOptions: { isIdempotent: true }})
```

Previously, a similar behaviour was available using `IdempotenceAwareRetryPolicy`, that is now marked as deprecated.

### Removed `retryOnTimeout` property of `QueryOptions`

`retryOnTimeout`, the property that controlled whether a request should be tried when a response wasn't obtained 
after a period of time is no longer available. 

The behaviour should be now controlled using `onRequestError()` method on the `RetryPolicy`  for idempotent 
queries.

### Changes on `OperationInfo` of the retry module 

The retry policy methods takes [`OperationInfo`][op-info] as a parameter. Some `OperationInfo` properties changes or 
were removed.

- Deprecated properties `handler`, `request` and `retryOnTimeout` were removed.
- `options` property was replaced by `executionOptions` which is an instance of `ExecutionOptions`.

### Removed `meta` property from `ResultSet`

On earlier versions of the driver, the `ResultSet` exposed the property `meta` which contained the raw result metadata.
This property was removed in the latest version.

[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[op-info]: https://docs.datastax.com/en/developer/nodejs-driver/latest/api/module.policies/module.retry/type.OperationInfo/