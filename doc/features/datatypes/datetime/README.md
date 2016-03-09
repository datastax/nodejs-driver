# Date and time representation

## Timestamp 

The CQL timestamp data type represents a single moment in time with millisecond precision, and is represented by the
driver as an [ECMAScript Date][date].

## Date 

Introduced in Cassandra 2.2, a date portion without a time-zone, is represented as a [LocalDate][localdate-api].

## Time 

Introduced in Cassandra 2.2, a time portion without a time-zone, is represented as a [LocalTime][localtime-api].

[date]: https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Date
[localdate-api]: http://docs.datastax.com/en/drivers/nodejs/3.0/module-types-LocalDate.html
[localtime-api]: http://docs.datastax.com/en/drivers/nodejs/3.0/module-types-LocalTime.html