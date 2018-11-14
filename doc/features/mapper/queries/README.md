# Queries

The Mapper generates the CQL queries and adapts the results into objects according to the [defined
mappings](../defining-mappings/).

*Note that throughout the Mapper documentation the [killrvideo schema][killrvideo] is used.* 

## Retrieval

The are three methods in the `ModelMapper` that are used to retrieve objects from the database:

- `find()`: filters by one or more primary keys and returns the `Result` that is an iterable of objects.
- `get()`: Gets one document matching the provided filter or `null` when not found. Note that all partition and 
clustering keys must be defined in order to use this method.
- `findAll()`: selects all the objects returns the `Result` that is an iterable of objects. This is only recommended
to be used for tables with a limited amount of results. Otherwise, breaking up the token ranges on the client side 
would be better.

When a model is mapped to multiple tables or views, the mapper will select the table that matches the primary keys 
and the fields provided.

Additionally, the retrieval methods support using relational operators, setting multiple conditions on the same 
field, setting the order and defining the specific fields. This operator and clauses are translated and applied 
on the server side, no client-side filtering is performed by the Mapper.

### Usage examples

#### Get a single object

```javascript
const video = await videoMapper.get({ videoId });
```

#### Get an iterable of objects

Get all videos posted by a user

```javascript
const result = await videoMapper.find({ userId });
```

#### Find objects matching a relational operator applied on a field

Get videos from a user since a specific date 

```javascript
const result = await videoMapper.find({ userId, addedDate: q.gt(myDate) });
```

`q.gt()` represents the "greater than" operator (`>`), you can access all operators in `q` under the `mapping` module
 
```javascript
const q = dse.mapping.q;
```

#### Get objects using multiple conditions on the same field

Get videos from a user between two dates.

```javascript
const result = await videoMapper.find({ userId, addedDate: q.and(q.gte(beginDate), q.lt(endDate)) });
```

#### Get few selected fields of the objects

Get only name and description of the videos

```javascript
const result = await videoMapper.find({ userId }, { fields: ['name', 'description' ]});
```

#### Get objects with a specific order

Get all videos posted by a user sorted by added date in descending order.

```javascript
const result = await videoMapper.find({ userId }, { orderBy: { 'addedDate': 'desc' }});
```

## Insert

Use the `insert()` method on a `ModelMapper` instance to *upsert* a new object.

When a model is mapped to multiple tables, it will insert a row in each table when all the primary keys are specified
 grouped in a logged batch (either all or none of the insert operations will succeed).

Additionally, `insert()` supports conditional clause for [lightweight transactions (CAS)][lwt] that allows to 
insert only if the row doesn't exist. Please note that using IF conditions will incur a non-negligible performance 
cost on the server-side so this should be used sparingly.

### Usage examples

#### Insert a single object into one or more tables

Insert a video

```javascript
await videoMapper.insert({ videoId, name, addedDate, userId, description });
```

#### Insert few selected fields of an object

Insert only the id, the name and the added date, regardless of the other properties specified in the object.

```javascript
await videoMapper.insert(video, { fields: ['videoId', 'name', 'description'] });
```

#### Insert an object if it doesn't exist

Insert a video when there isn't a video with the same id.

```javascript
await videoMapper.insert({ videoId, name, description }, { ifNotExists: true });
```

## Update

Use the `update()` method on a `ModelMapper` instance to *upsert* a new object.

When a model is mapped to multiple tables, it will update a row in each table when all the primary keys are specified
 grouped in a logged batch (either all or none of the update operations will succeed).

Additionally, `update()` supports conditional clause for [lightweight transactions (CAS)][lwt] that allows to 
specify the condition that has to be met for the update to occur. Please note that using IF conditions will incur a 
non-negligible performance cost on the server-side so this should be used sparingly.

### Usage examples

#### Update a single object into one or more tables

Update a video

```javascript
await videoMapper.update({ videoId, name, addedDate, userId, description });
```

#### Update few selected fields of an object

Update only the name and the added date, regardless of the other properties specified in the object.

```javascript
await videoMapper.update(video, { fields: ['videoId', 'name', 'description'] });
```

#### Update an object using a [conditional statement][lwt]

Update a video when the existing name contains a certain value.

```javascript
await videoMapper.update({ videoId, name, description }, { when: { name: 'original name' } });
```

## Delete

Use the `remove()` method on a `ModelMapper` instance to delete an object.

When a model is mapped to multiple tables, it will delete the row on each table when all the primary keys are specified
 grouped in a logged batch (either all or none of the delete operations will succeed).

Additionally, `remove()` supports conditional clause for [lightweight transactions (CAS)][lwt] that allows to 
specify the condition that has to be met for the delete to occur. Please note that using IF conditions will incur a 
non-negligible performance cost on the server-side so this should be used sparingly.

### Usage examples

#### Delete a single object into one or more tables

Delete a video

```javascript
await videoMapper.delete({ videoId });
```

#### Delete an object using a [conditional statement][lwt]

Delete a video when the existing name contains a certain value.

```javascript
await videoMapper.delete({ videoId }, { when: { name: 'original name' } });
```

## Group mutations in a batch

You can batch multiple operations for both single partition and multiple partitions when [atomicity][atomicity] and
[isolation][isolation] is a requirement for a group of changes.

You can use the field `batching` of a `ModelMapper` to create each item of a batch and the `batch()` method of the 
`Mapper` to submit the request.

### Usage example

#### Update a group of objects

Update two videos from a user in a batch.

```javascript
const changes = [
  videoMapper.batching.update({ userId, videoId1, name1, addedDate1 }),
  videoMapper.batching.update({ userId, videoId2, name2, addedDate2 })
];

// Execute the batch
await mapper.batch(changes);
```

## Custom queries

The Mapper supports bypassing query generation, allowing you to specify the CQL query. It will execute the query and 
map the results according to the [mapping configuration](../defining-mappings/).

Use `mapWithQuery()` method to create your own `ModelMapper` execution method.

### Usage example

```javascript
// Write your own query using query markers for parameters
const query = 'SELECT COUNT(videoid) as video_count FROM user_videos WHERE userid = ? GROUP BY userid';

// Create a new ModelMapper method with your own query
// and a function to extract the parameters from an object 
videoMapper.getCount = videoMapper.mapWithQuery(query, video => [ video.userId ]);
```

Once you created a new `ModelMapper` method, you can use it in your application.

```javascript
const result = await videoMapper.getCount({ userId });
console.log(result.first().videoCount);
```

The result will be an instance of `Result` with the columns mapped to the property name according to the 
configuration, similar to other `ModelMapper` methods.

Note that you must use query markers to represent parameters in the query, you should avoid hard-coding the parameter
values in the query. 

### Execution options

The last parameter of the `ModelMapper` execution methods is a string representing the [Execution
Profile](../../execution-profiles). Execution profiles allows you to define the execution options once and reuse them
 across different execution invocations.

As stated in the [Execution Profiles documentation](../../execution-profiles), you should define the profiles when 
creating the `Client` instance.

```javascript
const client = new Client({ 
  contactPoints, 
  profiles: [ 
    new ExecutionProfile('default', {
      consistency: consistency.one,
      readTimeout: 10000
    }),
    new ExecutionProfile('oltp-sample', {
      consistency: consistency.localQuorum
    })
  ]
});
```

Then, you can use those execution profile names when executing a query with the Mapper, you can look at the methods 
signature for more info.

### Examples

```javascript
videoMapper.get({ videoid }, 'oltp-sample');

videoMapper.find({ userId }, 'oltp-sample');

// After the document info
videoMapper.find({ userId }, { fields: ['name', 'description'] }, 'oltp-sample');

videoMapper.update(video, 'oltp-sample');

videoMapper.insert(video, { ifNotExists: true }, 'oltp-sample');

// Use default execution profile
userMapper.get({ videoId });

// Use another execution profile
userMapper.get({ videoId }, 'another-execution-profile');
```

---

You can look at the [documentation on defining mappings](../defining-mappings/) to understand how tables and columns 
are mapped into object and properties.

[killrvideo]: https://github.com/pmcfadin/killrvideo-sample-schema
[lwt]: https://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertLWT.html
[atomicity]: https://en.wikipedia.org/wiki/Atomicity_(database_systems)
[isolation]: https://en.wikipedia.org/wiki/Isolation_(database_systems)