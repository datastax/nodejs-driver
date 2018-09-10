# Defining mappings

You can define how your application model is represented on your database by setting the `MappingOptions`.

In general, you should specify the table name(s) and the naming convention you are using on the CQL objects and your
 application models.

```javascript
const UnderscoreCqlToCamelCaseMappings = cassandra.mappings.UnderscoreCqlToCamelCaseMappings;

const mappingOptions = {
  models: {
    'User': {
      tables: ['users'],
      mappings: new UnderscoreCqlToCamelCaseMappings()
    }
  }
};

// Create the Mapper using the mapping options
const mapper = new Mapper(client, mappingOptions);
```

When a certain column or property doesn't match the naming convention, you can specify each column name and property 
name key-value pair, for example:

```javascript
const mappingOptions = {
  models: {
    'User': {
      tables: ['users'],
      mappings: new UnderscoreCqlToCamelCaseMappings(),
      columns: {
        'userid': 'userId',
        'firstname': 'firstName'
      }
    }
  }
};
```

## Mapping to Multiple Tables

In order to get more efficient reads, you often need to denormalize your schema. Denormalization and duplication 
of data is a common data modeling pattern with Apache Cassandra and DataStax Enterprise.

The Mapper supports mapping a single model to multiple tables or views. These tables will be used for mutations when 
using `insert()`, `update()` and `remove()` methods, and the most suitable table or view will be used according to 
the keys specified.

To use multiple tables/views with the same model, specify the names in the `MappingOptions`.

```javascript
const mappingOptions = {
  models: {
    'User': {
      tables: [ 'videos', 'user_videos', 'latest_videos' ],
      mappings: new UnderscoreCqlToCamelCaseMappings(),
      columns: {
        'videoid': 'videoId',
        'userid': 'userId'
      }
    }
  }
};
```

Then, when invoking `ModelMapper` methods multiple tables will be affected for mutations.

```javascript
// The following invocation will create a batch inserting a row on each of the tables
await videoMapper.insert({ videoId, userId, addedDate, yyyymmdd, name });
```

When selecting rows, the most suitable table will be used according to the table or view primary keys.

```javascript
// The following call will use table `user_videos` to get videos by user id
const result = await videoMapper.find({ userId });
```


## Mapping to a Materialized View

Similar to mapping to a table, you can map to a [materialized view][view]. The main difference is that views are 
not used for mutations.

```javascript
const mappingOptions = {
  models: {
    'User': {
      tables: [ 'videos', 'user_videos', 'latest_videos', { name: 'videos_by_location', isView: true } ],
      mappings: new UnderscoreCqlToCamelCaseMappings(),
      columns: {
        'videoid': 'videoId',
        'userid': 'userId'
      }
    }
  }
};
```

---

You can look at the [Queries documentation](../queries/) for examples on retrieving and saving objects.

*Note that throughout the Mapper documentation the [killrvideo schema][killrvideo] is used.* 

[killrvideo]: https://github.com/pmcfadin/killrvideo-sample-schema
[view]: https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateMV.html