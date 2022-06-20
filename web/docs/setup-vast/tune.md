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
table slices to the remote VAST node. If the timeout fires before a table slice
reaches `vast.import.batch-size`, then the table slice will contain fewer events
and ship immediately.

The `vast.import.read-timeout` option determines how long a call to read data
from the input will block. After the read timeout elapses, VAST tries again at a
later.

## Memory usage and caching

The amount of memory that a VAST server process is allowed to use can currently
not be configured directly as a configuration file option. Instead of such a
direct tuning knob, the memory usage can be influenced through the configuration
of the caching, catalog and disk monitor features.

### Caching

VAST groups table slices with the same schema in a *partition*. When building a
partition, the parameter `max-partition-size` sets an upper bound on the number
of records in a partition, across all table slices. The parameter
`active-partition-timeout` provides a time-based upper bound: once reached, VAST
considers the partition as complete, regardless of the number of records.

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
    # Set the default false-positive rate for type-level synopses
    default-fp-rate: 0.001
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

VAST processes log messages in a dedicated thread, which by default buffers up
to 1M messages for servers, and 100 for clients. The option
`vast.log-queue-size` controls this setting.

## Rebuild Partitions

The `rebuild` command re-ingests events from existing partitions and replaces
them with new partitions. This makes it possible to upgrade persistent state to
a newer version, or recreate persistent state after changing configuration
parameters, e.g., switching from the Feather to the Parquet store backend.
Rebuilding partitions also recreates their sketches. The process takes place
asynchronously in the background.

:::info Upgrade from VAST v1.x partitions
You can use the `rebuild` command to upgrade your VAST v1.x partitions to v2.x,
which yields better compression and have a streamlined representation. We
recommend this to be able to use newer features that do not work with v1.x
partitions.
:::

This is how you run it:

```bash
vast rebuild [--all] [--undersized] [--parallel=<number>] [<expression>]
```

The on-disk format of VAST's partitions is versioned. By default, the `rebuild`
command only considers partitions whose version number is not the newest
version. The `--all` flag makes the command instead consider _all_ partitions
rather than only outdated ones. This is useful when you change configuration
options and want to regenerate all partitions. The `--undersized` flag causes
VAST to only rebuild partitions that are under the configured partition size
limit.

The `--parallel` options is a performance tuning knob. The parallelism level
controls how many sets of partitions to rebuild in parallel. This value defaults
to 1 to limit the CPU and memory requirements of the rebuilding process, which
grow linearly with the selected parallelism level.

An optional expression allows for restricting the set of partitions to rebuild.
VAST performs a catalog lookup with the expression to identify the set of
candidate partitions. This process may yield false positives, as with regular
queries, which in this case causes potentially unaffected partitions to undergo
a rebuild process. For example, to rebuild outdated partitions containing
`suricata.flow` events older than 2 weeks, run the following command:

```bash
vast rebuild '#type == "suricata.flow" && #import_time < 2 weeks ago'
```
