# User-defined types

A User-Defined type (UDT) simplifies handling a group of related properties.

An example is a user account table that contains address details described through a set of columns: street, city, zip
code. With the addition of UDTs, you can define this group of properties as a type and access them as a single entity
or separately.

User-defined types are declared at the keyspace level.

With the Node.js driver, you can retrieve and store UDTs using JavaScript objects.

For example, given the following UDT and table:

``
CREATE TYPE address (
   street text,
   city text,
   state text,
   zip int,
   phones set<text>
);

CREATE TABLE users (
   name text PRIMARY KEY,
   email text,
   address frozen<address>
);
```

You retrieve the user address details as a regular JavaScript object.

```javascript
const query = 'SELECT name, email, address FROM users WHERE id = ?';
client.execute(query, [name], { prepare: true }, function (err, result) {
   var row = result.first();
   var address = row.address;
   console.log('User lives in %s - %s', address.city, address.state); 
});
```

You modify the address using JavaScript objects as well:

```javascript
const address = {
   city: 'Santa Clara',
   state: 'CA',
   street: '3975 Freedom Circle',
   zip: 95054,
   phones: ['650-389-6000']
};
var query = 'UPDATE users SET address = ? WHERE id = ?';
client.execute(query, [address, name], { prepare: true}, callback);
```

**Setting the prepare flag is recommended** because it helps the driver to accurately map the UDT fields from and to
object properties.

You can provide JavaScript objects as parameters without setting the prepare flag, but you must provide the parameter
hint (`udt<address>`) and initially the driver makes an extra roundtrip to the cluster to retrieve the UDT metadata.

## Nesting user-defined types in CQL 

User defined types can nested arbitrarily. Here is an example based on the schema used in the previous example, but
with the phones column changed from `set<text>` to a `set<frozen<phone>>`. The phone UDT contains an alias, a 
phone_number and a country_code.

```
CREATE TYPE phone ( 
   alias text,
   phone_number text, 
   country_code int
);

CREATE TYPE address (
   street text,
   city text,
   state text,
   zip int,
   phones set<frozen<phone>>
);

CREATE TABLE users (
   name text PRIMARY KEY,
   email text,
   address frozen<address>
);
```

You access the UDT fields in the same way, but as nested JavaScript objects.

```javascript
const query = 'SELECT name, email, address FROM users WHERE id = ?';
client.execute(query, [name], { prepare: true }, function (err, result) {
   const row = result.first();
   const address = row.address;
   // phones is an Array of Objects
   address.phones.forEach(function (phone) {
      console.log(Phone %s: %s', phone.alias, phone.phone_number);
   });
});
```