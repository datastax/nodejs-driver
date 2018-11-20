# UUID and time-based UUID data types

The driver provides ways to generate and decode UUIDs and time-based UUID.

## Uuid 

The [`Uuid` class](/api/module.types/class.Uuid/) provides support for representing Cassandra uuid data type. To generate a version 4 unique identifier,
use the `Uuid` static method `random()`:

```javascript
const Uuid = require('dse-driver').types.Uuid;
const id = Uuid.random();
```

The driver decodes Cassandra uuid data type values as an instances of `Uuid`.

```javascript
client.execute('SELECT id FROM users')
  .then(function (result) {
    console.log(result.rows[0].id instanceof Uuid); // true
    console.log(result.rows[0].id.toString());      // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  });
```

You can also parse a string representation of a uuid into a `Uuid instance:

```javascript
const id = Uuid.fromString(stringValue);
console.log(id instanceof Uuid);            // true
console.log(id.toString() === stringValue); // true
```

## TimeUuid 

The [`TimeUuid` class](/api/module.types/class.TimeUuid/) provides support for representing Cassandra `timeuuid` data type.
To generate a time-based identifier, you can use the `now()` and `fromDate()` static methods:

```javascript
const TimeUuid = require('dse-driver').types.TimeUuid;
const id1 = TimeUuid.now();
const id2 = TimeUuid.fromDate(new Date());
```

The driver decodes CQL timeuuid data type values as instances of `TimeUuid`.

```javascript
client.execute('SELECT id, timeid FROM sensor')
  .then(function (result) {
    console.log(result.rows[0].timeid instanceof TimeUuid); // true
    console.log(result.rows[0].timeid instanceof Uuid); // true, it inherits from Uuid
    console.log(result.rows[0].timeid.toString());      // <- xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    console.log(result.rows[0].timeid.getDate());       // <- Date stored in the identifier
  });
```

You can specify the other parts of the identifier, such as the node and the clock sequence, or the 100-nanosecond
precision value of the date using the optional parameters in `fromDate()` method.

```javascript
const ticks = 9123; // A number from 0 to 9999
const id = TimeUuid.fromDate(new Date(), ticks, node, clock);
```
