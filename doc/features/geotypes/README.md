## Geospatial types

[DataStax Enterprise][dse] 5.0 comes with a set of additional CQL types to represent geospatial data:

- `PointType`
- `LineStringType`
- `PolygonType`.

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes encoders and representations of these types in the `geometry` module that can be used directly
as parameters in queries. All Javascript geospatial types implement `toString()`, that returns the string representation
in [Well-known text][wkt] format, and `toJSON()`, that returns the JSON representation in [GeoJSON][geojson] format.

## Usage

```javascript
const dse = require('dse-driver');
const Point = dse.geometry.Point;
const insertQuery = 'INSERT INTO points_of_interest (name, coords) VALUES (?, ?)';
const selectQuery = 'SELECT coords FROM points_of_interest WHERE name = ?';

client.execute(insertQuery, ['Eiffel Tower', new Point(48.8582, 2.2945)], function (err, result) {
  assert.ifError(err);
  client.execute(selectQuery, ['Eiffel Tower'], function (err, result) {
    assert.ifError(err);
    const row = result.first();
    const point = row['coords'];
    console.log(point instanceof Point); // true
    console.log('x: %d, y: %d', point.x, point.y); // x: 48.8582, y: 2.2945
    console.log(point.toString()); // 'POINT (48.8582 2.2945)'
  });
});
```

[dse]: http://www.datastax.com/products/datastax-enterprise
[wkt]: https://en.wikipedia.org/wiki/Well-known_text
[geojson]: https://en.wikipedia.org/wiki/GeoJSON