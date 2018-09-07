# Mapper

The driver provides an object mapper that lets you interact with your data like you would interact with a set
 of documents.

## Mapper Features

- No / minimal configuration required: no need to specify the schema manually, it uses the driver schema metadata
- Support denormalized schemas and materialized views: one model can be mapped to multiple tables
- Convention-based mapping
- Support bypassing query generation / bring your own queries and map results
- Minimal performance impact compared to the core driver

## Basic Usage

Retrieving objects from the database:

```javascript
const videos = await videoMapper.find({ userId });
for (let video of videos) {
  console.log(video.name);
}
```

Updating an object from the database:

```javascript
await videoMapper.update({ id, userId, name, addedDate, description });
```

Note that execution methods return a `Promise`, to simplify the code examples in the documentation [async 
functions][async-function] are used.

You can continue by reading the [Getting Started Guide](getting-started/) or other topics in the Mapper documentation:

- [Getting Started Guide](getting-started/)
- [Queries](queries/)
- [Defining Mappings](defining-mappings/)
- [Limitations and FAQ](limitations-and-faq/)

[async-function]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function