"use strict";
const cassandra = require('cassandra-driver');
const Uuid = cassandra.types.Uuid;
const UnderscoreCqlToCamelCaseMappings = cassandra.mapping.UnderscoreCqlToCamelCaseMappings;

const client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

const mapper = new cassandra.mapping.Mapper(client, { models: {
  'Video': {
    tables: ['videos', 'user_videos', 'latest_videos'],
    keyspace: 'examples',
    columns: {
      'videoid': 'videoId',
      'userid': 'userId'
    },
    mappings: new UnderscoreCqlToCamelCaseMappings()
  }
}});

const videoId = Uuid.random();
const userId = Uuid.random();

/**
 * Inserts an object and retrieves it using the Mapper.
 * Note that the tables are from killrvideo schema, more info:
 * https://github.com/pmcfadin/killrvideo-sample-schema/blob/master/killrvideo-schema.cql
 */
client.connect()
  .then(function () {
    const queries = [
      `CREATE KEYSPACE IF NOT EXISTS examples
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }`,
      'USE examples',
      `CREATE TABLE IF NOT EXISTS videos (videoid uuid, userid uuid, name varchar, description varchar, location text,
      location_type int, preview_thumbnails map<text,text>, tags set<varchar>, added_date timestamp,
      PRIMARY KEY (videoid))`,
      `CREATE TABLE IF NOT EXISTS user_videos (userid uuid, added_date timestamp, videoid uuid, name text,
      preview_image_location text, PRIMARY KEY (userid, added_date, videoid))
      WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,
      `CREATE TABLE IF NOT EXISTS latest_videos (yyyymmdd text, added_date timestamp, videoid uuid, name text,
      preview_image_location text, PRIMARY KEY (yyyymmdd, added_date, videoid))
      WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,
    ];
    let p = Promise.resolve();
    // Create the schema executing the queries serially
    queries.forEach(query => p = p.then(() => client.execute(query)));
    return p;
  })
  .then(() => {
    const videoMapper = mapper.forModel('Video');
    // Insert on tables "videos" and "user_videos"
    return videoMapper.insert({ videoId, addedDate: new Date(), userId, name: 'My video', description: 'My desc'});
  })
  .then(() => {
    const videoMapper = mapper.forModel('Video');
    // SELECT using table "videos"
    return videoMapper.find({ videoId });
  })
  .then(results => console.log('--Obtained video by id\n', results.first()))
  .then(() => {
    const videoMapper = mapper.forModel('Video');
    // SELECT using table "user_videos"
    return videoMapper.find({ userId });
  })
  .then(results => console.log('--Obtained video by user id\n', results.first()))
  .catch(function (err) {
    console.error('There was an error', err);
  })
  .then(() => client.shutdown());