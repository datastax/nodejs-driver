# Upgrade guide

The purpose of this guide is to detail the changes made by the successive versions of the DataStax Node.js Driver that 
are relevant to for an upgrade from prior versions.

If you have any questions or comments, you can [post them on the mailing list][mailing-list].

## 4.0

The following is a list of changes made in version 4.0 of the driver that are relevant when upgrading from version 3.x.

### Changes to the retry and load-balancing policies

`ExecutionInfo` is introduced as a wrapper around the `QueryOptions` provided to the execution methods.
The `ExecutionInfo` contains getter methods to obtain the values of each option, defaulting to the execution profile
options or the ones defined in the `ClientOptions`. Previously, a shallow copy of the provided `QueryOptions` was 
used, resulting in unnecessary allocations and evaluations.

The `LoadBalancingPolicy` and `RetryPolicy` base classes changed method signatures to take `ExecutionInfo` instances 
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
- `options` property was replaced by `executionInfo` which is an instance of `ExecutionInfo`.

### Removed `meta` property from `ResultSet`

On earlier versions of the driver, the `ResultSet` exposed the property `meta` which contained the raw result metadata.
This property was removed in the latest version.

---

## 2.0

The following is a list of changes made in version 2.0 of the driver that are relevant when upgrading from version 1.x.

### API Changes

1. `uuid` and `timeuuid` values are decoded as [`Uuid`](../features/datatypes/uuids) and
[`TimeUuid`](../features/datatypes/uuids) instances.

1. `decimal` values are decoded as [`BigDecimal`](../features/datatypes/numerical) instances.

1. `varint` values are decoded as [`Integer`](../features/datatypes/numerical) instances.

1. `inet` values are decoded as `InetAddress` instances.


[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[op-info]: https://docs.datastax.com/en/developer/nodejs-driver/latest/api/module.policies/module.retry/type.OperationInfo/