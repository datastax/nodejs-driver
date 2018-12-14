# Query timestamps

In Cassandra, each mutation has a microsecond-precision timestamp, which is used to order operations relative to
each other.

The timestamp can be provided by the client or assigned server-side based on the time the server processes the request.

Letting the server assign the timestamp can be a problem when the order of the writes matter: with unlucky
timing (different coordinators, network latency, etc.), two successive requests from the same client might be
processed in a different order server-side, and end up with out-of-order timestamps.

## Client-side generation

### Using a timestamp generator

When using Apache Cassandra 2.1+ or DataStax Enterprise 4.7+, it's possible to send the operation timestamp in the
request. Starting from version 3.2 of the Node.js driver, the driver uses [`MonotonicTimestampGenerator`][mtg] 
by default to generate the request timestamps.

You can provide a different generator when creating the `Client` instance:

```javascript
const client = new Client({
  contactPoints,
  localDataCenter,
  policies: {
    timestampGeneration: new MyCustomTimestampGenerator()
  }
});
```

To implement a custom timestamp generator, you must implement `TimestampGenerator` base class.

In addition, you can also set the default timestamp on a per-execution basis in the query options:

```javascript
session.execute(query, params, { timestamp: timestamp });
```

[mtg]: ../../api/module.policies/module.timestampGeneration/class.MonotonicTimestampGenerator/


#### Accuracy

As defined by ECMAScript, the `Date` object has millisecond resolution. The [`MononoticTimestampGenerator`][mtg]
uses a incremental counter to generate the sub-millisecond part of the timestamp until the next clock tick.

#### Monotonicity

The [`MononoticTimestampGenerator`][mtg] implementation also guarantees that the returned timestamps will always be
monotonically increasing, even if multiple updates happen under the same millisecond.

Note that to guarantee such monotonicity, if more than one thousand timestamps are generated within the same
millisecond, or in the event of a system clock skew, _the implementation might return timestamps that drift out into
the future_. When this happens, the built-in generator logs a periodic warning message. See their non-default
constructors for ways to control the warning interval.


### Provide the timestamp in the query

Alternatively, if you are using a lower server version, you can explicitly provide the timestamp in your CQL query:

```javascript
client.execute('INSERT INTO my_table(c1, c2) VALUES (1, 1) USING TIMESTAMP 1482156745633040');
```