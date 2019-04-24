# Logging

The DataStax Node.js driver uses [events][events] to expose logging information decoupled from any specific logging 
framework.

The driver's `Client` inherits from [`EventEmitter`][event-emitter] and it triggers `'log'` events.

```javascript
client.on('log', (level, className, message, furtherInfo) => {
  console.log('log event: %s -- %s', level, message);
});
```

The level being passed to the listener can be `'verbose'`, `'info'`, `'warning'` or `'error'`.
 
`verbose` level is only suitable for debugging and it's usually too noisy. We recommend that you gather logging 
events from `info` and above on production environments.

## Tracking query latency and size

The `RequestLogger` logs queries executed by the driver and it allows tracking requests considered slow and/or large.

A request is considered "slow" when it takes longer to complete than a configured threshold in milliseconds. A request
is considered to be large when the request size is greater than a configured threshold in bytes.

To turn on this feature, you first need to create an instance of `RequestLogger` and use it when creating the `Client`
instance:

```javascript
const cassandra = require('cassandra-driver');

const requestTracker = new cassandra.tracker.RequestLogger({ slowThreshold: 1000 });
const client = new Client({ contactPoints, localDataCenter, requestTracker });
```

You can subscribe to `'slow'`, `'large'`, `'normal'` and `'failure'` events using the emitter object instance:

```javascript
requestTracker.emitter.on('slow', message => console.log(message));
```

An example message would be:

```
[10.1.1.1:9042] Slow request, took 305 ms (request size 35 bytes / response size 1 KB): SELECT col1, col2 FROM table1 WHERE id = ? [1]
```

Note that events will be emitted only when certain options are defined:
- `'slow'` events will only be emitted if `slowThreshold` is set.
- `'large'` events will only be emitted if `requestSizeThreshold` is set.
- `'normal'` events will only be emitted if `logNormalRequests` is set to `true`. This setting can be changed at 
runtime using the `RequestLogger` property of the same name. 
- `'failure'` events will only be emitted if `logErroredRequests` is set to `true`. This setting can be changed at
runtime using the property of the same name.

You can provide your own tracker implementing `RequestTracker` interface. 

[events]: https://nodejs.org/api/events.html
[event-emitter]: https://nodejs.org/api/events.html#events_class_eventemitter