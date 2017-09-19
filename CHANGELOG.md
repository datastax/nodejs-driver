# ChangeLog - DataStax Node.js Driver

## 3.3.0

2017-09-19

### Features

- [NODEJS-82] - Speculative query retries
- [NODEJS-287] - Provide metrics on the state of connections to Cassandra
- [NODEJS-308] - Add CDC to TableOptionsMetadata and TableOptions for Cassandra 3.8+
- [NODEJS-309] - Allow prepared statements to be prepared on all nodes
- [NODEJS-339] - Avoid using deprecated Buffer constructors
- [NODEJS-343] - Improve performance of Murmur 3 partitioner
- [NODEJS-359] - Add 'applied' to ResultSet, similar to java-drivers ResultSet.wasApplied()
- [NODEJS-375] - Expose optional callbacks Uuid.random() as async overload
- [NODEJS-376] - Stringify tokens once to simplify computations when building token map

### Bug Fixes

- [NODEJS-365] - Routing key component length is encoded as int16 instead of uint16
- [NODEJS-370] - Consistency of trace queries is not configurable
- [NODEJS-373] - Empty string in a map field returned as null on query

## 3.2.2

2017-06-05

### Bug Fixes

- [NODEJS-346] - Shutdown doesn't work if error occurs after control connection initialization
- [NODEJS-347] - Metadata: Schema parser fails to handle index_options null values
- [NODEJS-355] - Domain without dots will not connect
- [NODEJS-358] - TokenAwarePolicy does not take statement keyspace into account
- [NODEJS-360] - ControlConnection: when any of the queries to refresh topology fail it will not attempt to reconnect
- [NODEJS-362] - Driver fails to encode Duration's with large values

## 3.2.1

2017-04-24

### Features

- [NODEJS-332] - Support Duration Type
- [NODEJS-338] - Make protocol negotiation more resilient

## 3.2.0

2017-01-17

### Notable Changes

- Promise support ([#194](https://github.com/datastax/nodejs-driver/pull/194)).
- Timestamp generation: client-side timestamps are generated and sent in the request by default when the 
server supports it ([#195](https://github.com/datastax/nodejs-driver/pull/195)).
- Added `isIdempotent` query option which is set to `false` by default: future versions of the driver will use this
 value to consider whether an execution should be retried or directly rethrown to the consumer without using the retry
 policy ([#197](https://github.com/datastax/nodejs-driver/pull/197)).

### Features

- [NODEJS-322] - Timestamp Generator Support for providing Client Timestamps Improvement
- [NODEJS-189] - Support promises
- [NODEJS-230] - Expose ResultSet `@@iterator`
- [NODEJS-325] - Add explicit idempotency setting in the query options

## 3.1.6

2016-11-14

### Bug Fixes

- [NODEJS-294] - TokenAwarePolicy: Avoid yielding the primary replica first

## 3.1.5

2016-10-07

### Bug Fixes

- [NODEJS-313] - Client-to-node encryption: mark request as written before invoking socket.write()

## 3.1.4

2016-09-21

### Bug Fixes

- [NODEJS-310] - Reading streaming frames with flags can result in uncaught error

## 3.1.3

2016-08-31

### Bug Fixes

- [NODEJS-303] - Protocol version downgrade fails on OSX and Windows.

## 3.1.2

2016-08-30

### Bug Fixes

- [NODEJS-283] - Possible connection leak if pool is shutting down while core connections are being created.
- [NODEJS-288] - Callback never executed in error on subsequent Client.execute with Client configured with keyspace
that doesn't exist.
- [NODEJS-293] - When client.connect() return error - client.shutdown() not work properly.
- [NODEJS-296] - Cannot read property 'consistency' of null, TypeError: Cannot read property 'consistency' of null
- [NODEJS-297] - DCAwareRoundRobinPolicy should make a local reference to host arrays
- [NODEJS-301] - 'Trying to access beyond buffer length' error if Warnings, Custom Payload, or Trace Id present in
non-RESULT response Improvement
- [NODEJS-265] - Remove connection from host pool when closed by server side

## 3.1.1

2016-06-30

### Bug Fixes

- [NODEJS-284] - Driver fails to resolve host names in the local hosts file

## 3.1.0

2016-06-28

### Notable Changes

- Introduced experimental Execution Profiles API ([#156](https://github.com/datastax/nodejs-driver/pull/156))
- Removed dependency to [async](https://github.com/caolan/async) package (
[#138](https://github.com/datastax/nodejs-driver/pull/138)).
- Enhanced retry policies: handle client timeouts, connection closed and other errors. New retry decision: try next
host ([#143](https://github.com/datastax/nodejs-driver/pull/143)).

### Features

- [NODEJS-261] - Execution profiles
- [NODEJS-105] - New Retry Policy Decision - try next host
- [NODEJS-106] - Don't mark host down while one connection is active
- [NODEJS-107] - Prevent duplicate metadata fetches from control connection and allow disabling schema metadata fetching
- [NODEJS-247] - Schedule idleTimeout before descheduling the previous
- [NODEJS-177] - Use A-record with multiple IPs for contact points
- [NODEJS-201] - Avoid dynamically copying query options properties into users query options
- [NODEJS-236] - Handle empty map values gracefully
- [NODEJS-240] - Replace async dependency
- [NODEJS-242] - Expose default policies and default options
- [NODEJS-248] - Optimize query plan hosts iteration
- [NODEJS-249] - Avoid using Object.defineProperty() in ResultSet constructor
- [NODEJS-251] - Expose onRequestError() method in the RetryPolicy prototype

### Bug Fixes

- [NODEJS-246] - InetAddress validation improperly flags IPv4-mapped IPv6
- [NODEJS-250] - Timeout duration reported in OperationTimedOutError does not consider statement-level options.
- [NODEJS-252] - Prepared statement metadata does not use logged keyspace
- [NODEJS-255] - InetAddress.toString() improperly truncates last group if preceding bytes are 0 for ipv6 addresses
- [NODEJS-257] - Connection wrongly parses IPv6 from Host address
- [NODEJS-273] - readTimeout set to 0 in queryOptions is not used.

## 3.0.2

2016-04-05

### Features

- [NODEJS-228] - Allow setting read timeout at statement level

### Bug Fixes

- [NODEJS-159] - Metadata.getTokenToReplicaNetworkMapper does not account for multiple racks in a DC
- [NODEJS-235] - Decoding error can result in callback not executed
- [NODEJS-237] - Timeuuid generation sub-millisecond portion is not guaranteed to be increasing
- [NODEJS-238] - eachRow() retry attempts after read timeout don't execute rowCallback

## 3.0.1

2016-02-08

### Features

- [NODEJS-211] - Pass the authenticator name from the server to the auth provider

### Bug Fixes

- [NODEJS-216] - Inet with alpha character is converting the character to 0

## 3.0.0

2015-12-14

### Notable Changes

- Default consistency changed back to `LOCAL_ONE`.

### Features

- [NODEJS-155] - Schedule reconnections using Timers
- [NODEJS-195] - Expose encode()/decode() functions
- [NODEJS-204] - Change default consistency level to LOCAL_ONE
- [NODEJS-198] - Avoid using Function.prototype.bind() for common execution path
- [NODEJS-200] - Use Error.captureStackTrace() only when setting enabled

### Bug Fixes

- [NODEJS-193] - BigDecimal.fromString() should throw a TypeError if there is a conversion error
- [NODEJS-197] - Can't parse column type if it contains UDT that is a quoted identifier
- [NODEJS-202] - Support for "custom" types after CASSANDRA-10365
- [NODEJS-203] - RoundRobinPolicies: Missing return statement when calling callback

## 3.0.0-rc1

2015-11-11

### Notable Changes

- Added support for Cassandra 3.0.0
- _Breaking_ Changed default consistency to `LOCAL QUORUM` [#103](https://github.com/datastax/nodejs-driver/pull/103)
- _Breaking_ `Aggregate#initCondition` now returns the string representation of the value
[#102](https://github.com/datastax/nodejs-driver/pull/102)
- Manual paging via `ResultSet#nextPage()` and `Client#stream()` throttling
[#111](https://github.com/datastax/nodejs-driver/pull/111)

### Features

- [NODEJS-186] - Update schema type representation to CQL
- [NODEJS-68] - Manual paging support via nextPage() and client.stream() throttling
- [NODEJS-130] - Add buffer for non-streaming rows messages
- [NODEJS-142] - Frame coalescing on connection
- [NODEJS-169] - Update async dependency
- [NODEJS-178] - Change default consistency level to LOCAL_QUORUM
- [NODEJS-181] - Update default behavior unbound values in prepared statements

### Bug Fixes

- [NODEJS-164] - Defunct connection is not properly removed from pool
- [NODEJS-190] - useUndefinedAsUnset should not apply to udt, tuple, set, list and map members.

## 3.0.0-beta1

2015-10-19

### Notable Changes

- Added support for Cassandra 3.0-rc1
- New index metadata API [#98](https://github.com/datastax/nodejs-driver/pull/98)

### Features

- [NODEJS-163] - Process Modernized Schema Tables for C* 3.0
- [NODEJS-166] - Process Materialized View Metadata
- [NODEJS-170] - Process materialized view events
- [NODEJS-171] - Process changes in 'columns' table in C* 3.0-rc1+
- [NODEJS-172] - Process crc_check_chance column from 'tables' and 'views' metadata tables
- [NODEJS-182] - Add missing options to table / view metadata
- [NODEJS-183] - Add support for parsing Index metadata

### Bug Fixes

- [NODEJS-185] - Metadata fetch of table with ColumnToCollectionType fails

## 2.2.2

2015-10-14

### Features

- [NODEJS-187] - Expose Metadata prototype to be available for _promisification_

### Bug Fixes

- [NODEJS-160] - Error setting routing keys before query execution
- [NODEJS-175] - Select from table after a new field is added to a UDT can result in callback never fired
- [NODEJS-185] - Metadata fetch of table with ColumnToCollectionType fails

## 2.2.1

2015-09-14

### Features

- [NODEJS-162] - Add coordinator of query to error object

### Bug Fixes

- [NODEJS-154] - Local datacenter could not be determined
- [NODEJS-165] - Driver 2.2 fails to connect under windows server for cassandra 2.1

## 2.2.0

2015-08-10

### Notable Changes

- **Client**: All requests use `readTimeout` that can be configured in the `socketOptions`, enabled by default to
12secs
- **Client**: Now exposes topology and node status change events: `hostAdd`, `hostRemove`, `hostUp` and `hostDown`

### Features

- [NODEJS-140] - WhiteListPolicy
- [NODEJS-114] - Client-Configurable High Level Request Timeout
- [NODEJS-138] - Provide option to open all connections at startup
- [NODEJS-149] - Expose node status and topology changes
- [NODEJS-152] - Enable client read timeout by default

### Bug Fixes

- [NODEJS-111] - Connect should callback in error after shutdown
- [NODEJS-151] - 'All host(s) tried for query failed' error immediately after Cassandra node failure
- [NODEJS-156] - RequestHandler retry should not use a new query plan
- [NODEJS-157] - Control connection can fail and not be re-established if it errors on initOnConnection

## 2.2.0-rc1

2015-06-18

### Notable Changes

- Added support for Cassandra 2.2 and native protocol v4

### Features

- [NODEJS-117] - Small int and byte types for C* 2.2
- [NODEJS-118] - Support new date and time types
- [NODEJS-121] - Distinguish between `NULL` and `UNSET` values in Prepared Statements
- [NODEJS-122] - Add support for client warnings
- [NODEJS-123] - Support Key-value payloads in native protocol v4
- [NODEJS-124] - Use PK columns from v4 prepared responses
- [NODEJS-125] - Support UDF and Aggregate Function Schema Meta
- [NODEJS-126] - Add client address to query trace
- [NODEJS-129] - Support server error in Startup response for C* 2.1
- [NODEJS-131] - Handle new C* 2.2 errors

### Bug Fixes

- [NODEJS-119] - Rare 'write after end' error encountered while reconnecting with lower protocol version on nodejs 0.10.x
- [NODEJS-120] - Connection 'object is not a function' at Connection.handleResult
- [NODEJS-127] - Integer.toBuffer() gives wrong representation for positive numbers with the msb on
- [NODEJS-128] - getPeersSchemaVersions uses system.local instead of system.peers
- [NODEJS-136] - LocalDate fails to parse dates less than -271821-04-20 and greater than 275760-09-13
- [NODEJS-137] - DriverInternalError - No active connection found
- [NODEJS-139] - Use retry policy defined in the query options
- [NODEJS-141] - Node schema change - keyspace metadata does not exist
- [NODEJS-146] - Unhandled 'error' event caused by RST on Socket on Connection Initialization causes app to terminate
