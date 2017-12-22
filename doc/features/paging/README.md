# Fetching large result sets

When dealing with a large number of rows, the single-threaded nature of Node.js should be taken into consideration
because processing large results can take significant CPU time and can lead to higher levels of memory consumption.
The driver addresses this by exposing the `eachRow()` and `stream()` methods, that parse the rows and yield them to the
user as they come through the network.

The driver only requests a limited number of rows each time (`5000` being the default `fetchSize`). To retrieve the
rows beyond this default size, use one of the several paging mechanisms.

## Automatic paging

The `stream()` method automatically fetches the following pages, yielding the rows as they come through the network and
retrieving the following page after the previous rows were read (throttling).

```javascript
client.stream(query, parameters, options)
  .on('readable', function () {
    // readable is emitted as soon a row is received and parsed
    let row;
    while (row = this.read()) {
      // process row
    }
  })
  .on('end', function () {
    // emitted when all rows have been retrieved and read
  });
```

With the `eachRow()` method, you can retrieve the following pages automatically by setting the `autoPage` flag to
`true` in the queryOptions to request the following pages automatically. Because `eachRow()` does not handle back
pressure, it is only suitable when there is minimum computation per row required and no additional I/O, otherwise it
ends up buffering an unbounded amount of rows.


```javascript
client.eachRow(query, parameters, { prepare: true, autoPage : true }, function(n, row) {
   // Invoked per each row in all the pages
}, callback);
```

## Manual paging 

If you want to retrieve the next page of results only when you ask for it (for example, in a web page or after a
certain computation or job finished), you can use the `eachRow()` method.

There are two ways that `eachRow()` method allows you to fetch the next page of results.

```javascript
const options = { prepare : true , fetchSize : 1000 };
client.eachRow(query, parameters, options, function (n, row) { 
     // Invoked per each row in all the pages
  }, function (err, result) {
     // Called once the page has been retrieved.
     if (result.nextPage) {
       // Retrieve the following pages:
       // the same row handler from above will be used
       result.nextPage();
     }
  }
);
```

You can use `pageState` property, a string token made available in the result if there are additional result pages.

```javascript
const options = { prepare : true, fetchSize : 200 };
client.eachRow(query, parameters, options, function (n, row) { 
     // Row callback.
     }, function (err, result) {
        // End callback.
        // Store the paging state.
        pageState = result.pageState;
     }
);
```

In the next request, use the `pageState` to fetch the following rows.

```javascript
// Use the pageState in the queryOptions to continue where you left it.
const options = { pageState : pageState, prepare : true, fetchSize :  200 };
client.eachRow(query, parameters, options, function (n, row) {
   // Row callback.
   }, function (err, result) {
      // End callback.
      // Store the next paging state.
      pageState = result.pageState;
   }
);
```

Saving the paging state works well when you only let the user move from one page to the next. But it doesn't allow
arbitrary jumps (like "go directly to page 10"), because you can't fetch a page unless you have the paging state of the
previous one. Such a feature would require offset queries, which are not natively supported by Cassandra.

**Note**: The page state token can be manipulated to retrieve other results within the same column family, so it is not
safe to expose it to the users in plain text.