# Promise and callback-based API

The driver supports both [promises][promise] and callbacks for the asynchronous methods exposed in the `Client` and
`Metadata` prototypes, you can choose the approach that suits your needs.

## Promise-based API

```javascript
client.execute('SELECT name, email FROM users')
  .then(result => console.log('User with email %s', result.rows[0].email));
```

When a `callback` is not provided as the last argument, the driver will return a `Promise`, without the need to 
_promisify_ the driver module. Returned promises are instances of [`Promise` global object][promise] and are created
using the default constructor: `new Promise(executor)`.

In case you want the driver to use a third party `Promise` module (ie: [bluebird][bluebird]) to create the `Promise`
instances, you can optionally provide your own factory method when creating the `Client` instance, for example:

```javascript
const BbPromise = require('bluebird');
const client = new Client({
  contactPoints,
  localDataCenter,
  promiseFactory: BbPromise.fromCallback
});
```

## Callback-based API

All asynchronous methods of the driver supports an optional `callback` as the last argument.

```javascript
client.execute('SELECT name, email FROM users', function(err, result) {
  assert.ifError(err);
  console.log('User with email %s', result.rows[0].email);
});
```

[promise]: https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Promise
[bluebird]: http://bluebirdjs.com/