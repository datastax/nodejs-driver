# Upgrade Guide to 2.0

The purpose of this guide is to detail the changes made by the version 2.0 of the DataStax Node.js Driver that are relevant to an upgrade from version 1.0.

We used the opportunity of a major version bump to incorporate your feedback and improve the API. Unfortunately this means there are some breaking changes, but the new API should be both simpler and more complete.

## API Changes

1. `uuid` and `timeuuid` values are decoded as [Uuid][uuid] and [TimeUuid][timeuuid] instances.

1. `decimal` values are decoded as [BigDecimal][decimal] instances.

1. `varint` values are decoded as [Integer][integer] instances.

1. `inet` values are decoded as [InetAddress][inet] instances.

_If you have any question or comment, please [post it on the mailing list][mailing-list]._

  [mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
  [uuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Uuid.html
  [timeuuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-TimeUuid.html
  [inet]: http://www.datastax.com/drivers/nodejs/2.0/module-types-InetAddress.html
  [decimal]: http://www.datastax.com/drivers/nodejs/2.0/module-types-BigDecimal.html
  [integer]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Integer.html