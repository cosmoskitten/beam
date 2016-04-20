# Google Cloud Platform IOs

## Google Cloud Bigtable IO

This IO library provides Beam sources and sinks to make it possible to read and
write Google Cloud Bigtable data from Beam pipelines.

For more information, see the online documentation at [Google Cloud Bigtable](https://cloud.google.com/bigtable/).

### Reading from Google Cloud Bigtable

The Bigtable source returns a set of rows from a single table, returning a `PCollection<Row>`.

To configure a Google Cloud Bigtable source, you must supply a table ID and a `BigtableOptions`
or builder configured with the project and other information necessary to identify the
Bigtable cluster. A `RowFilter` may also optionally be specified using `BigtableIO.Read.withRowFilter()`.
For example:

```java
BigtableOptions.Builder optionsBuilder =
  new BigtableOptions.Builder()
    .setProjectId("project")
    .setClusterId("cluster")
    .setZoneId("zone");

Pipeline p = ...;

// Scan the entire table.
p.apply("read",
  BigtableIO.read()
    .withBigtableOptions(optionsBuilder)
    .withTableId("table"));

// Scan a subset of rows that match the specified row filter.
p.apply("filtered read",
  BigtableIO.read()
    .withBigtableOptions(optionsBuilder)
    .withTableId("table")
    .withRowFilter(filter));
```

### Writing to Google Cloud Bigtable

The Bigtable sink executes a set of row mutations on a single table. It takes as input a
`PCollection<KV<ByteString, Iterable<Mutation>>>`, where the `ByteString` is the key of the
row being mutated, and each `Mutation` represents an idempotent transformation for that row.

To configure a Google Cloud Bigtable sink, you must supply a table ID and a `BigtableOptions`
or builder configured with the project and other information necessary to identify the
Bigtable cluster, for example:

```java
BigtableOptions.Builder optionsBuilder =
  new BigtableOptions.Builder()
    .setProjectId("project")
    .setClusterId("cluster")
    .setZoneId("zone");

PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;

data.apply("write",
  BigtableIO.write()
    .withBigtableOptions(optionsBuilder)
    .withTableId("table"));
```

### Permissions

Permission requirements depend on the `PipelineRunner` that is used to execute the Beam job.
Please refer to the documentation of corresponding `PipelineRunner` for more details.
