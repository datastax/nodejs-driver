# Execution Profiles

Execution profiles provide a mechanism to group together a set of configuration options and reuse them across different 
query executions. This feature is specially useful when dealing with different workloads like DSE Graph, Cql OLTP
workloads, DSE search, ...

These options include:

- Load balancing policy
- Retry policy
- Consistency levels
- Per-host request timeout
- Graph Options
    - Graph name
    - Graph traversal source
    - Graph read consistency
    - Graph write consistency

## Using Execution Profiles

### Initializing cluster with profiles

Execution profiles should be created when creating the `Client` instance with a name that identifies it and the settings
that apply to the profile.

```javascript
const aggregationProfile = new ExecutionProfile('aggregation', {
  consistency: consistency.localQuorum,
  loadBalancing: new DCAwareRoundRobinPolicy('us-west'),
  retry: myRetryPolicy,
  readTimeout: 30000,
  serialConsistency: consistency.localSerial
});
const client = new Client({ 
  contactPoints: ['host1'], 
  profiles: [ aggregationProfile ]
});
```

Note that while the above options are all the supported settings on the execution profiles, you can specify only the
ones that are required for the executions, using the `'default'` profile to fill the rest of the options.

#### Default execution profile

You can define a default profile, using the name `'default'`:

```javascript
const client = new Client({ 
  contactPoints: ['host1'], 
  profiles: [ 
    new ExecutionProfile('default', {
      consistency: consistency.one,
      readTimeout: 10000
    }),
    new ExecutionProfile('graph-oltp', {
      consistency: consistency.localQuorum,
      graphOptions:  { name: 'myGraph' }
    })
  ]
});
```

The default profile will be used to fill the unspecified options in the rest of the profiles. In the above example, the
read timeout for the profile named `'graph-oltp'` will be the one defined in the default profile (10,000 ms).

For the settings that are not specified in the default profile, the driver will use the default `Client` options.

### Using an execution profile by name

Use the name to specify which profile you want to use for the execution.

```javascript
client.execute(query, params, { executionProfile: 'aggregation' });
```
### Using an execution profile by instance

You can also use the `ExecutionProfile` instance.

```javascript
client.execute(query, params, { executionProfile: aggregationProfile });
```

### Using default execution profile

When the execution profile is not provided in the options, the default execution profile is used.

```javascript
client.execute(query, params, null);
```