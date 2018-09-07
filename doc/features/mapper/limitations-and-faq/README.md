# Limitations and Frequently Asked Questions

## Limitations

There are some limitations in the Mapper design:

- Secondary indexes and SOLR queries are not supported.
- Case sensitive CQL identifiers (enclosed with double quotation marks) are not supported.
- When mapping a model to multiple tables/views
    - Columns with the same name must be of the same type.
    - Updating primary keys of any of the tables is not supported and will result in additional rows being created on
     the server side.
- The following CQL features are not supported
    - Deleting an individual map key/value pair is not supported, as in `DELETE favs['author'] FROM ...`
    - Updating single UDT fields map key/values is not supported, e.g: `id.field = 3`

Note that some limitations can be overcome by using [Custom Queries](#custom-queries).

## FAQ

### Should I specify the keyspace per mapping?

If you are using a single keyspace containing all your data, its recommended that you set the keyspace name when 
creating the `Client` instance. For example:

```javascript
const client = new Client({ contactPoints, keyspace: 'my_keyspace' });
const mapper = new Mapper(client);
```

When dealing with multiple keyspaces, you can specify the keyspace of the tables for each model using the 
`MappingOptions`:

```javascript
const client = new Client({ contactPoints});
const mapper = new Mapper(client, {
  models: {
    'Video': { 
      tables: [ 'videos', 'user_videos', 'latest_videos' ],
      keyspace: 'killrvideo'
    }
  }
});
```
