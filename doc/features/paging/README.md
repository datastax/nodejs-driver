# Fetching large result sets

When dealing with a large number of rows, the driver breaks the result into _pages_, only requesting a limited number of
rows each time (`5000` being the default `fetchSize`). To retrieve the rows beyond this default size, use one of the
following paging mechanisms.

## Automatic paging

The driver supports asynchronous iteration of the `ResultSet` using the built-in [Async Iterator][async-it], fetching
the following result pages after the previous one has been yielded.

Large result sets can be iterated using the [`for await ... of`][for-of-await] statement:

```javascript
const result = await client.execute(query, params, { prepare: true });

for await (const row of result) {
  console.log(row[columnName]);
}
```

Under the hood, the driver will get all the rows of the query result using multiple requests. Initially,
when calling `execute()` it will retrieve the first page of results according to the fetch size (defaults to `5000`).
If there are additional rows, those will be retrieved once the async iterator yielded the rows from the previous page.

If needed, you can use `isPaged()` method of `ResultSet` instance to determine whether there are more pages of results
than initially fetched.

Note that using the async iterator will not affect the internal state of the <code>ResultSet</code> instance.
You should avoid using both <code>rows</code> property that contains the row instances of the first page of
results, and the async iterator, that will yield all the rows in the result regardless on the number of pages.

## Manual paging

Sometimes it is convenient to save the paging state in order to restore it later. For example, consider a stateless
web service that displays a list of results with a link to the next page. When the user clicks that link, we want to
run the exact same query, except that the iteration should start where we stopped on the previous page.

To do so, the driver exposes a `pagingState` object that represents where we were in the result set when the last page
was fetched:

```javascript
const options = { prepare: true , fetchSize: 1000 };
const result = await client.execute(query, parameters, options);

// Property 'rows' will contain only the amount of items of the first page (max 1000 in this case)
const rows = result.rows;

// Store the page state
let pageState = result.pageState;
```

In the next request, use the `pageState` to fetch the following rows.

```javascript
// Use the pageState in the queryOptions to continue where you left it.
const options = { pageState, prepare: true, fetchSize: 1000 };
const result = await client.execute(query, parameters, options);

// Following rows up to fetch size (1000)
const rows = result.rows;

// Store the next paging state.
pageState = result.pageState;
```

Saving the paging state works well when you only let the user move from one page to the next. But it doesn't allow
arbitrary jumps (like "go directly to page 10"), because you can't fetch a page unless you have the paging state of the
previous one. Such a feature would require offset queries, which are not natively supported by Apache Cassandra.

**Note**: The page state token can be manipulated to retrieve other results within the same column family, so it is not
safe to expose it to the users in plain text.

## Row streams

If you want to handle a large result set as a [`Stream`][stream] of rows, you can use `stream()` method of the
`Client` instance. The `stream()` method automatically fetches the following pages, yielding the rows as they come
through the network and retrieving the following page only after the previous rows were read (throttling).

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

[query-options]: /api/type.QueryOptions/
[client-stream]: /api/class.Client/#stream
[stream]: https://nodejs.org/api/stream.html
[async-it]: https://github.com/tc39/proposal-async-iteration
[for-of-await]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/for-await...of