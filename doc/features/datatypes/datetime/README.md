# Date and time representation

## Timestamp 

The CQL timestamp data type represents a single moment in time with millisecond precision, and is represented by the
driver as an [ECMAScript Date][date].

## Date 

Introduced in Cassandra 2.2, a date portion without a time-zone, is represented as a [LocalDate](/api/module.types/class.LocalDate/).

## Time 

Introduced in Cassandra 2.2, a time portion without a time-zone, is represented as a [LocalTime](/api/module.types/class.LocalTime/).

[date]: https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Date
