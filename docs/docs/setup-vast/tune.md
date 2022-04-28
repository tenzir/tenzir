---
sidebar_position: 5
---
# Tune

This section describes tuning knobs that have a notable effect on system
performance.

## Batching: Table Slices

VAST processes events in batches. Because the structured data has the shape of a
table, we call these batches *table slices*. The following options control their
shape and behavior.

:::note Implementation Note
Table slices are implemented as *Record Batches* in Apache Arrow.
:::

### Size

Most components in VAST operate on table slices, which makes the table slice
size a fundamental tuning knob on the spectrum of throughput and latency. Small
table slices allow for shorter processing times, resulting in more scheduler
context switches and a more balanced workload. But the increased pressure on the
scheduler comes at the cost of throughput. Conversely, a large table slice size
creates more work for each actor invocation and makes them yield less frequently
to the scheduler. As a result, other actors scheduled on the same thread may
have to wait a little longer.

The option `vast.import.batch-size` sets an upper bound for the number of events
per table slice.

The option merely controls number of events per table slice, but not
necessarily the number of events until a component forwards a batch to the next
stage in a stream. The CAF streaming framework uses a credit-based flow-control
mechanism to determine buffering of tables slices.

:::tip
Setting `vast.import.batch-size` to 0 causes the table slice size to be
unbounded and leaves it to other parameters to determine the actual table slice
size.
:::

### Import Timeout

The `vast.import.batch-timeout` option sets a timeout for forwarding buffered
table slices to the importer. If the timeout fires before a table slice reaches
`vast.import.batch-size`, then the table slice will contain fewer events and
ship immediately.

## Memory usage and caching

The amount of memory that a VAST server process is allowed to use can currently
not be configured directly as a configuration file option. Instead of such a
direct tuning knob, the memory usage can be influenced through the configuration
of the caching, catalog and disk monitor features.

### Caching

VAST groups table slices with the same schema in a *partition*. When building a
partition, the parameter `max-partition-size` sets an upper bound on the number
of records in a partition, across all table slices.

A LRU cache of partitions accelerates queries to recently used partitions. The
parameter `max-resident-partitions` controls the number of partitions in the LRU
cache.

### Catalog

The catalog manages partition meta data and is responsible for deciding whether
a partition qualifies for a certain query. It does so by maintaining *sketch*
data structures (e.g., Bloom filters, summary statistics) for each partition.
Sketches are highly space-efficient at the cost of being probabilistic and
yielding false positives.

Due to this characteristic sketches can grow sub-linear, doubling the number of
events in a sketch does not lead to a doubling of the memory requirement.
Because the catalog must be traversed in full for a given query it needs to be
maintained in active memory to provide high responsiveness.

As a consequence, the overall amount of data in a VAST instance and the
`max-partition-size` determine the memory requirements of the catalog. The
option `max-partition-size` is inversely linked to the number of sketches in the
catalog. That means increasing the `max-partition-size` is an effective method
to reduce the memory requirements for the catalog.

### Tune sketch parameters

A false positive can have substantial impact on the query latency by
materializing irrelevant partitions, which involves unnecessary I/O. Based on
the cost of I/O, this penalty may be substantial. Conversely, reducing the false
positive rate increases the memory consumption, leading to a higher resident set
size and larger RAM requirements.

You can control this space-time trade-off in the configuration section
`vast.index` by specifying index *rules*. Each rule corresponds to one sketch
and consists of the following components:

- `targets`: a list of extractors to describe the set of fields whose values to
  add to the sketch.
- `fp-rate`: an optional value to control the false-positive rate of the sketch.

VAST does not create field-level sketches unless a dedicated rule with a
matching target configuration exists.

#### Example

```yaml
vast:
  index:
    rules:
      - targets:
          # field synopses: need to specify fully qualified field name
          - suricata.http.http.url
        fp-rate: 0.005
      - targets:
          - :addr
        fp-rate: 0.1
```

This configuration includes two rules (= two sketches), where the first rule
includes a field extractor and the second a type extractor. The first rule
applies to a single field, `suricata.http.http.url`, and has false-positive rate
of 0.5%. The second rule creates one sketch for all fields of type `addr` that
has a false-positive rate of 10%.

### Approximate memory requirements

A rough formula for estimating the memory requirements takes the configuration,
the input data rate, and the amount of unique IP address and string values in
the input data stream into account.

```
memory_est = (segments + 1) * max-segment-size + avg-partition-mem-size *
(max-resident-partitions + 1) + sketch-factor * max-num-partitions
```

where:

```
max-num-partitions = disk-budget-high / avg-partition-disk-size
```

The `sketch-factor` depends on the rate of unique string or address values in
the input data stream and the `catalog-fp-rate` option. This has been measured
to 2.0% at a false positive rate of `0.001`.

The `avg-partition-disk-size` is in turn a function of the input data
distribution and `max-partition-size` config option. It has been measured at
about 163 MiB for a typical suricata eve.log.

Finally, the `avg-partition-mem-size` can be conservatively calculated as twice
the size of the `avg-partition-disk-size`. Multiplied to the value of
`max-resident-partitions + 1` from the configuration file we get the maximum
occupancy of the partitions.

#### Example

Putting in the measured example values and a `disk-budget-high` setting of
2000 GiB we can calculate the estimated memory consumption to

```
11 * 1 Gib + 11 * 0.326 Gib + 0.02 * 2000 Gib
11 Gib + 3.6 Gib + 40 Gib
54.6 Gib
```

As you can see, depending on the disk budget and the entropy in the data the
sketches that make up the catalog can quickly become the largest contributor to
the memory requirement. Increasing the `max-partition-size` from the default 1
Mib to 8 Mib reduces the sketch factor to `0.016` for our sample data, if we
reduce the `max-resident-partitions` to `2` for a fair comparison, we get the
the following new composition:

```
11 * 1 Gib + 3 * 2.608 Gib + 0.016 * 2000 Gib
11 Gib + 7.2 Gib + 32 Gib
50.2 Gib
```

## Shutdown

The `stop` command gracefully brings down a VAST server that has been started
with the `start` command.

It is also possible to send a signal `SIGINT(2)` to the `vast` process instead
of using `vast stop`, but in only works on the same machine that runs the
server process. We recommend using `vast stop`, as it also works over the wire.

The `stop` command blocks until the server process has terminated, and returns
a zero exit code upon success, making it suitable for use in launch system
scripts.

The configuration option `vast.shutdown-grace-period` sets the time to wait
until component shutdown finishes cleanly before inducing a hard kill.

:::note
The server waits for ongoing import processes to terminate before shutting down
itself. In case an import process is hanging, you can always terminate the
hanging process manually to shutdown the server.
:::

## Logging

The VAST server writes log files into a file named `server.log` in the database
directory by default. Set the option `vast.log-file` to change the location of
the log file.

VAST client processes do not write logs by default. Set the option
`vast.client-log-file` to enable logging. Note that relative paths are
interpreted relative to the current working directory of the client process.

Server log files rotate automatically after 10 MiB. The option
`vast.disable-log-rotation` allows for disabling log rotation entirely, and the
option `vast.log-rotation-threshold` sets the size limit when a log file should
be rotated.

VAST processes log messages in a dedicated thread, which buffers up to 1M
messages by default. The option `vast.log-queue-size` controls this setting.
