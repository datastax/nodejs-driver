# Getting Started

The Mapper is provided as part of the driver package.

```javascript
const dse = require('dse-driver');
const Client = dse.Client;
const Mapper = dse.mapping.Mapper;

const client = new Client({ contactPoints, keyspace });
```

Create a `Mapper` instance and reuse it across your application. You can specify you model properties and how those
are mapped to table columns can be defined in the [MappingOptions](../defining-mappings/). 

```javascript
const mapper = new Mapper(client, { 
  models: { 'Video': { tables: ['videos'] } }
});
```

A `ModelMapper` contains all the logic to retrieve and save objects from and to the database.

```javascript
const videoMapper = mapper.forModel('Video');
```

Internally, the `Mapper` contains a single `ModelMapper` instance per model in your application, you can call 
`mapper.forModel(name)` each time you need a model mapper with no additional cost.

To retrieve a single object, use `get()` method of the `ModelMapper`.

```javascript
const video = await videoMapper.get({ videoId: myVideoId });
```

Use `find()` method to filter by one or more primary keys.

```javascript
const userVideos = await videoMapper.find({ userId: myUserId });
```

Insert an object using `insert()` method.

```javascript
await videoMapper.insert({ videoId, userId, addedDate, name });
```

Update an object using `update()` method.

```javascript
await videoMapper.update({ videoId, userId, addedDate, name: newName });
```

Delete an object using `remove()` method.

```javascript
await videoMapper.remove({ videoId });
```

Keep in mind that both `Mapper` and `Client` instances are designed to be long lived. If you don't want to maintain 
both instances on separate fields, you can access the `Client` instance using the `Mapper` property `client`. For 
example, you can shutdown your `Client` before exiting your application by calling:

```javascript
mapper.client.shutdown();
```

You can look at the [Queries documentation](../queries/) for more examples of retrieving and saving 
objects and you read the [Mappings documentation](../defining-mappings/) to understand how 
tables and columns are mapped into properties.

*Note that throughout the Mapper documentation the [killrvideo schema][killrvideo] is used.* 

[killrvideo]: https://github.com/pmcfadin/killrvideo-sample-schema