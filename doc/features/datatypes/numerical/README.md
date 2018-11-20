# Numerical values

The driver provides support for all the CQL numerical datatypes, such as int, float, double, bigint, varint, and
decimal. There is only one numerical datatype in ECMAScript standard, `Number` which represent a
double-precision 64-bit value. It is used by the driver to handle double, float, int, smallint and tinyint values.

## int, float, and double 

JavaScript provides methods to operate with Numbers (that is, IEEE 754 double-precision floats) and built-in operators
(sum, subtraction, division, bitwise, etc), making it a good fit for CQL datatypes int, float. and double.

When decoding any of these datatype values, it is returned as a `Number`.

```javascript
client.execute('SELECT int_val, float_val, double_val FROM tbl')
  .then(function (result) {
    console.log(typeof result.rows[0]['int_val']);    // Number
    console.log(typeof result.rows[0]['float_val']);  // Number
    console.log(typeof result.rows[0]['double_val']); // Number
  });
```

When encoding the data, the driver tries to encode a `Number` as double because it can not automatically determine if is
dealing with an int, a float, or a double.

Inserting a Number value as a double succeeds:

```javascript
const query = 'INSERT INTO tbl (id, double_val) VALUES (?, ?)';
client.execute(query, [ id, 1.2 ]);
```

But doing the same with a float fails:

```javascript
const query = 'INSERT INTO tbl (id, float_val) VALUES (?, ?)';
client.execute(query, [ id, 1.2 ])
  .catch(function (err) {
    console.log(err) // ResponseError: Expected 4 or 0 byte value for a float (8)
  });
```

Trying to do the same with an int, also fails because Cassandra expects a float or an int, and the driver sent a 64-bit
double.

```javascript
const query = 'INSERT INTO tbl (id, int_val) VALUES (?, ?)';
client.execute(query, [ id, 1 ])
  .catch(function (err) {
    console.log(err) // ResponseError: Expected 4 or 0 byte int (8)
  });
```

To overcome this limitation, you should prepare your queries. Because preparing and executing statements in the driver
does not require chaining two asynchronous calls, you can set the prepare flag in the query options and the driver
handles the rest.

The previous query, using the prepare flag, succeeds no matter if it is an int, float, or double:

```javascript
const query = 'INSERT INTO tbl (id, int_val) VALUES (?, ?)';
client.execute(query, [ id, 1 ], { prepare: true });
```


## decimal 

The [`BigDecimal` class](/api/module.types/class.BigDecimal/) provides support for representing the CQL decimal datatype, because JavaScript has no built-in
arbitrary precision decimal representation.

```javascript
const BigDecimal = require('dse-driver').types.BigDecimal;
const value1 = new BigDecimal(153, 2);
const value2 = BigDecimal.fromString('1.53');
console.log(value1.toString());     // 1.53
console.log(value2.toString());     // 1.53
console.log(value1.equals(value2)); // true
```

The driver decodes CQL decimal datatype values as instances of `BigDecimal`.

```javascript
client.execute('SELECT decimal_val FROM users')
  .then(function (result) {
    console.log(result.rows[0]['decimal_val'] instanceof BigDecimal); // true
  });
```

## bigint 

The [`Long` class](/api/module.types/class.Long/) provides support for representing the CQL bigint datatype, because JavaScript has no built-in 64-bit
integer representation.

```javascript
const Long = require('dse-driver').types.Long;
const value1 = Long.fromNumber(101);
const value2 = Long.fromString('101');
console.log(value1.toString());             // 101
console.log(value2.toString());             // 101
console.log(value1.equals(value2));         // true
console.log(value1.add(value2).toString()); // 202
```

The driver decodes CQL bigint datatype values as instances of `Long`.

```javascript
client.execute('SELECT bigint_val FROM users')
  .then(function (result) {
    console.log(result.rows[0]['bigint_val'] instanceof Long); // true
  });
```

## varint 

The [`Integer` class](/api/module.types/class.Integer/), originally part of the Google Closure math library, provides support for representing CQL varint
datatype values, because JavaScript has no arbitrarily-large signed integer representation.

```javascript
const Integer = require('dse-driver').types.Integer;
const value1 = Integer.fromNumber(404);
const value2 = Integer.fromString(404);
console.log(value1.toString());             // 404
console.log(value2.toString());             // 404
console.log(value1.equals(value2));         // true
console.log(value1.add(value2).toString()); // 808
```

The driver decodes CQL varint datatype values as instances of `Integer`.

```javascript
client.execute('SELECT varint_val FROM users')
  .then(function (result) {
    console.log(result.rows[0]['varint_val'] instanceof Integer); // true
  });
```

## smallint and tinyint 

Cassandra 2.2 introduced `smallint` for 2-byte numerical representation and `tinyint` for 1-byte numerical
representation.

The driver represents these types as `Number` to take advantage of the ECMAScript built-in operations for `Number` (sum,
subtraction, division, bitwise, etc).

For `tinyint`, only Numbers between -128 and 127 are valid, and for `smallint` only Numbers between -32768 and 32767.
Numbers outside valid ranges will callback with `TypeError` when executing.

## ECMAScript BigInt support

On modern JavaScript engines with [`BigInt`][bigint] support (e.g., Node.js 10+), you can use ECMAScript
`BigInt` to represent `varint` and/or `bigint` CQL data types with the Node.js driver.

To enable this option, you must specify it in the client options:

```javascript
const client = new Client({
  contactPoints,
  encoding: { 
      useBigIntAsLong: true,
      useBigIntAsVarint: true
  }
});
```

You can use `BigInt` to represent `varint` or `bigint` CQL data types or both.

```javascript
client.execute('SELECT varint_value FROM table')
  .then(rs => console.log(typeof rs.rows[0]['varint_value'])); // "bigint"
```

[bigint]: https://github.com/tc39/proposal-bigint
