The `import` command ingests data. An optional filter expression allows for
restricing the input to matching events. The format of the imported data must
be explicitly specified:

```bash
vast import [options] <format> [options] [expr]
```

The `import` command is the dual to the `export` command.

This is easiest explained on an example:

```bash
vast import suricata < path/to/eve.json
```

The above command signals the running node to ingest (i.e., to archive and index
for later export) all Suricata events from the Eve JSON file passed via standard
input.

### Filter Expressions

An optional filter expression allows for importing the relevant subset of
information only. For example, a user might want to import Suricata Eve JSON,
but skip over all events of type `suricata.stats`.

```bash
vast import suricata '#type != "suricata.stats"' < path/to/eve.json
```

For more information on the optional filter expression, see the [query language
documentation](https://docs.tenzir.com/vast/query-language/overview).

### Format-Specific Options

Some import formats have format-specific options. For example, the `pcap` import
format has an `interface` option that can be used to ingest PCAPs from a network
interface directly. To retrieve a list of format-specific options, run `vast
import <format> help`, and similarly to retrieve format-specific documentation,
run `vast import <format> documentation`.

### Type Filtering

The `--type` option filters known event types based on a prefix.  E.g., `vast
import json --type=zeek` matches all event types that begin with `zeek`, and
restricts the event types known to the import command accordingly.

VAST permanently tracks imported event types. They do not need to be specified
again for consecutive imports.

### Batching

The import command parses events into table slices (batches). The following
options control the batching:

#### `vast.import.batch-size`

Sets an upper bound for the number of events per table slice.

Most components in VAST operate on table slices, which makes the table slice
size a fundamental tuning knob on the spectrum of throughput and latency.  Small
table slices allow for shorter processing times, resulting in more scheduler
context switches and a more balanced workload. However, the increased pressure
on the scheduler comes at the cost of throughput. A large table slice size
allows actors to spend more time processing a block of memory, but makes them
yield less frequently to the scheduler. As a result, other actors scheduled on
the same thread may have to wait a little longer.

The `vast.import.batch-size` option merely controls number of events per table
slice, but not necessarily the number of events until a component forwards a
batch to the next stage in a stream. The [CAF streaming
framework](https://actor-framework.readthedocs.io/en/latest/Streaming.html) uses
a credit-based flow-control mechanism to determine buffering of tables slices.
Setting `vast.import.batch-size` to 0 causes the table slice size to be
unbounded and leaves it to other parameters to determine the actual table slice
size.

#### `vast.import.batch-timeout`

Sets a timeout for forwarding buffered table slices to the importer.

The `vast.import.batch-timeout` option controls the maximum buffering period
until table slices are forwarded to the node. The default batch timeout is one
second.

#### `vast.import.read-timeout`

Sets a timeout for reading from input sources.

The `vast.import.read-timeout` option determines how long a call to read data
from the input will block. The process yields and tries again at a later time if
no data is received for the set value. The default read timeout is 20
milliseconds.
