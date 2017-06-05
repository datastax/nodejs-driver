# ChangeLog - DataStax Enterprise Node.js Driver

## 1.3.1

2017-06-05

### Bug Fixes

- [NODEJS-346] - Shutdown doesn't work if error occurs after control connection initialization
- [NODEJS-347] - Metadata: Schema parser fails to handle index_options null values
- [NODEJS-355] - Domain without dots will not connect
- [NODEJS-358] - TokenAwarePolicy does not take statement keyspace into account
- [NODEJS-360] - ControlConnection: when any of the queries to refresh topology fail it will not attempt to reconnect
- [NODEJS-362] - Driver fails to encode Duration's with large values

## 1.3.0

2017-04-18

### Bug Fixes

- [NODEJS-350] - Handle new WRITE_FAILURE and READ_FAILURE format in DSE_V1 protocol

## 1.3.0-beta1

2017-02-13

### Notable Changes

- Removed `cassandra-driver` external dependency. The DSE driver is now self-contained.
- Support for DSE 5.1 EAP3.

### Improvements

- [NODEJS-324] - DSE Auth 5.1: Support Proxy Authentication in 5.1
- [NODEJS-328] - Support DSE 5.1 DateRangeField
- [NODEJS-329] - Read optional workload set from node metadata
- [NODEJS-332] - Support Duration Type
- [NODEJS-338] - Make protocol negotiation more resilient


## 1.2.0

2017-01-17

### Notable Changes

- Promise support ([#194](https://github.com/datastax/nodejs-driver/pull/194)).
- Timestamp generation: client-side timestamps are generated and sent in the request by default when the 
server supports it ([#195](https://github.com/datastax/nodejs-driver/pull/195)).
- Added `isIdempotent` query option which is set to `false` by default: future versions of the driver will use this
 value to consider whether an execution should be retried or directly rethrown to the consumer without using the retry
 policy ([#197](https://github.com/datastax/nodejs-driver/pull/197)).

### Improvements

- [NODEJS-334] - Support promises in `executeGraph()` method
- [NODEJS-336] - Update core driver dependency to v3.2.0

## 1.1.0

2016-11-18

### Improvements

- [NODEJS-316] - Implement fromString() method for geotypes
- [NODEJS-317] - Support bytecode-json decoding in the DSE Driver
- [NODEJS-318] - Include graph language in the Execution Profiles

## 1.0.4

2016-10-07

### Improvements

- [NODEJS-315] - Update core driver dependency to v3.1.5.

## 1.0.3

2016-09-21

### Improvements

- [NODEJS-311] - Update core driver dependency to v3.1.4.

## 1.0.2

2016-08-31

### Improvements

- [NODEJS-305] - Update core driver dependency to v3.1.3.

## 1.0.1

2016-08-30

### Improvements

- [NODEJS-302] - Update core driver dependency to v3.1.2.

## 1.0.0

2016-08-30

General availability version