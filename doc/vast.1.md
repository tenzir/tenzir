# VAST 1 "@MAN_DATE@" @VERSION_MAJ_MIN@ "Visibility Across Space and Time"

NAME
----

`vast` -- manage a VAST topology

SYNOPSIS
--------

`vast` [*options*] *command* [*arguments*]

OVERVIEW
--------

VAST is a platform for explorative data analysis. It ingests various types of
data formats (e.g., logs, network packet traces) and provides type-safe search
in a rich data model.

DESCRIPTION
-----------

The `vast` executable enables management of a VAST topology by interacting with
a **node**, which acts as a container for the system components. Typically,
each physical machine in a VAST deployment corresponds to one node. For
single-machine deployments all components run inside a single process, whereas
cluster deployments consist of multiple nodes with components spread across
them.

Nodes can enter a peering relationship and build a topology. All peers have
the same authority: if one fails, others can take over. By default, each
node includes all core components: **archive**, **index**, and **importer**. For
more fine-grained control about the components running on a node, one can spawn
the node in "bare" mode to get an empty container. This allows for more
flexible arrangement of components to best match the available system hardware.

The following key components exist:

**source**
  Generates events from a data source, such as packets from a network interface
  or log files.

**sink**
  Receives events as the result of a query and displays them in specific output
  format, such as JSON, PCAP (for packets), or Zeek logs.

**archive**
  Compressed bulk storage of the all events.

**index**
  Accelerates queries by constructing bitmap indexes over a subset of the event
  data.

**importer**
  Accepts events from **source**s, assigns them unique 64-bit IDs, and relays
  them to **archive** and **index**.

**exporter**
  Accepts query expressions from users, asks **index** for hits, takes them to
  **archive** to extract candidates, and relays matching events to **sink**s.

### Schematic
```
                +--------------------------------------------+
                | Node                                       |
                |                                            |
  +--------+    |             +--------+                     |    +-------+
  | source |    |         +--->archive <------+           +-------> sink  |
  +----zeek+-------+      |   +--------<---+  v-----------++ |    +---json+
                |  |      |                |  | exporter   | |
                | +v------++           +------>------------+ |
     ...        | |importer|           |   |     ...         |      ...
                | +^------++           |   |                 |
                |  |      |            |   +-->------------+ |
  +--------+-------+      |            |      | exporter   | |
  | source |    |         |   +--------v      ^-----------++ |    +-------+
  +----pcap+    |         +---> index  <------+           +-------> sink  |
                |             +--------+                     |    +--ascii+
                |                                            |
                |                                            |
                +--------------------------------------------+
```
The above diagram illustrates the default configuration of a single node and
the flow of messages between the components. The **importer**, **index**, and
**archive** are singleton instances within the **node**. **Source**s are spawned
on demand for each data import. **Sink**s and **exporter**s form pairs that are
spawned on demand for each query. **Source**s and **sink**s exist in their own
processes, and are primarily responsible for parsing the input and formatting
the search results.

OPTIONS
-------

The *options* in front of *command* control how to to connect to a node.

The following *options* are available:

`-d` *dir*, `--dir`=*dir* [*.*]
  The VAST directory for logs and state.

`-e` *endpoint*, `--endpoint`=*endpoint* [*127.0.0.1:42000*]
  The endpoint of the node to connect to or launch. (See below)

`-i` *id*, `--id`=*id* [*hostname*]
  Overrides the node *id*, which defaults to the system hostname.
  Each node in a topology must have a unique ID, otherwise peering fails.

`-h`, `-?`, `--help`
  Display a help message and exit.

When specifying an endpoint via `-e`, `vast` connects to that endpoint to
obtain a **node** handle. An exception is the command `vast start`,
which uses the endpoint specification to spawn a **node**.

### endpoint

An endpoint has the format *host:port* where *host* is a hostname or IP address
and *port* the transport-layer port of the listening daemon. Either can be
omitted: *host* or *:port* are also valid endpoints. IPv6 addresses must be
enclosed in brackets in conjunction with a *port*, e.g., *[::1]:42001*.

COMMANDS
--------

This section describes each *command* and its *arguments*. The following
commands exist:
    *help*          displays a help message
    *version*       displays the software version
    *start*         starts a node
    *stop*          stops a node
    *peer*          peers with another node
    *status*        shows various properties of a topology
    *spawn*         creates a new component
    *kill*          terminates a component
    *import*        imports data from STDIN or file
    *export*        exports query results to STDOUT or file

### help

Synopsis:

  *help*

Displays a help message and exits.

### version

Synopsis:

  *version*

Displays the software version and exits.

### start

Synopsis:

  *start* [*arguments*]

Start a node.

Available *arguments*:

`-a` *query*
   A query for aging out historical data periodically. Do not set this before
   careful consideration! Hits for this query are deleted without backup.

`f` *interval*
   Frequency for running the aging query and deleting all hits.

### stop

Synopsis:

  *stop*

Stops the node and terminates all contained components.

### peer

Synopsis:

  *peer* *endpoint*

Joins a topology through a node identified by *endpoint*.
See **OPTIONS** for a description of the *endpoint* syntax.

### status

Synopsis:

  *status*

Displays various properties of a topology.

### spawn

Synopsis:

  *spawn* [*arguments*] *component* [*parameters*]

Creates a new component of kind *component*. Some components can have at most
one instance while others can have multiple instances.

Available *arguments*:

`-l` *label*
   A unique identifier for *component* within a node. The default label
   has the form *component* where *N* is a running counter increased for each
   spawned instance of *component*.

Available *component* values with corresponding *parameters*:

*consensus* [*parameters*]
`-i` *id* [*random*]
  Choose an explicit server ID for the consensus module. The default value is
  chosen uniformly at random from the set of valid IDs.

*archive* [*parameters*]
  `-s` *segments* [*10*]
    Number of cached segments
  `-m` *size* [*128*]
    Maximum segment size in MB

*index* [*parameters*]
  `-p` *partitions* [*10*]
    Number of passive partitions.
  `-e` *events* [*1,048,576*]
    Maximum events per partition. When an active partition reaches its
    maximum, the index evicts it from memory and replaces it with an empty
    partition.

*importer*

*exporter* [*parameters*] *expression*
  `-c`
    Marks this exporter as *continuous*.
  `-h`
    Marks this exporter as *historical*.
  `-u`
    Marks this exporter as *unified*, which is equivalent to both
    `-c` and `-h`.
  `-e` *n* [*0*]
    Limit the number of events to extract; *n = 0* means unlimited.

*source* **X** [*parameters*] [*expression*]
  **X** specifies the format of *source*. If *expression* is present, it will
  act as a whitelist that will skip all events that do not match. Each source
  format has its own set of parameters, but the following parameters apply to
  all formats:
  `-r` *input*
    Filesystem path or type-specific name that identifies event *input*.
  `-s` *schema*
    Path to an alterative *schema* file that overrides the default schema.
  `-d`
    Treats `-r` as a listening UNIX domain socket instead of a regular file.

*source* *zeek*

*source* *bgpdump*

*source* *mrt*

*source* *test* [*parameters*]
  `-e` *events*
    The maximum number of *events* to generate.

*source* *pcap* [*parameters*]
  `-c` *cutoff*
    The *cutoff* values specifies the maximum number of bytes to record per
    flow in each direction. That is, the maximum number of recorded bytes flow
    bytes can at most be twice as much as *cutoff*. the flow will be ignored
  `-f` *max-flows* [*1,000,000*]
    The maximum number of flows to track concurrently. When there exist more
    flows than *max-flows*, a new flow will cause eviction of a element from
    the flow table chosen uniformly at random.
  `-a` *max-age* [*60*]
    The maximum lifetime of a flow before it gets evicted from the flow table.
  `-p` *c*
    Enable pseudo-realtime mode by a factor of *1/c* to artificially delay
    packet processing when reading from trace files. This means that the PCAP
    source in that it sleeps for the amount of time observed in the packet
    timestamp differences. If the PCAP source encounters a packet *p1* after a
    previous packet *p0* with timestamps *t1* and *t0*, then it will sleep for
    time *(t1-t0)/c* before processing *p1*.

*sink* **X** [*parameters*]
  **X** specifies the format of *sink*. Each source format has its own set of
  parameters, but the following parameters apply to all formats:
  `-w` *path*
    Name of the filesystem *path* (file or directory) to write events to.
  `-d`
    Treats `-w` as a listening UNIX domain socket instead of a regular file.

*sink* *ascii*

*sink* *zeek*

*sink* *csv*

*sink* *json*

*sink* *pcap* [*parameters*]
  `-f` *flush* [*1000*]
    Flush the output PCAP trace after having processed *flush* packets.

*profiler* [*parameters*]
  If compiled with gperftools, enables the gperftools CPU or heap profiler to
  collect samples at a given resolution.
  `-c`
    Launch the CPU profiler.
  `-h`
    Launch the heap profiler.
  `-r` *seconds* [*1*]
    The profiling resolution in seconds.

### kill

Synopsis:

  *kill* *label*

Terminates a component. The argument *label* refers to a component label.

### import

Synopsis:

  *import* [*parameters*] *format* [*format-parameters*]
  `-t` *type*
    Produce table slices of given *type* instead of producing the default
    row-oriented table slices.
  `-r` *file*
    Read from *file* instead of STDIN.
  `-d`
    Treat `-r` as listening UNIX domain socket.

Imports data in a specific *format* on standard input and send it to a node.
This command is a shorthand for spawning a source locally and connecting it to
the given node's importer.
All *format-parameters* get passed to *format*.

### export

Synopsis:

  *export* [*parameters*] *format* [*format-parameters*] *expression*
  `-w` *file*
    Write to *file* instead of STDOUT.
  `-d`
    Treat `-w` as UNIX domain socket to connect to.

Issues a query and exports results to standard output. This command is a
shorthand for spawning a exporter and local sink, linking the two, and relaying
the resulting event stream arriving at the sink to standard output.
All *format-parameters* get passed to *format*.

EXAMPLES
--------

Start a node at 10.0.0.1 on port 42000 in the foreground:

    vast -e 10.0.0.1:42000 start

Send [Zeek](http://www.zeek.org) logs to the remote node:

    zcat *.log.gz | vast import zeek

Import a PCAP trace into a local VAST node in one shot:

    vast import pcap < trace.pcap

Run a historical query, printed in ASCII, limited to at most 10 results:

    vast export -e 10 ascii :addr in 10.0.0.0/8

Query a local node and get the result back as PCAP trace:

    vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
      | ipsumdump --collate -w - \
      | tcpdump -r - -nl

Make the node at 10.0.0.1 peer with 10.0.0.2:

    vast -e 10.0.0.1 peer 10.0.0.2

Connect to a node running at 1.2.3.4 on port 31337 and display topology details:

    vast -e 1.2.3.4:31337 status

FORMATS
-------

VAST can import and export various data formats. Some formats only work for
import, some only for export, and some for both.

### ASCII

- **Type**: writer
- **Representation**: ASCII
- **Dependencies**: none

The ASCII format is VAST's built-in way of representing events. It features an
unambiguous grammar for all data types. For example, an instance of `count`
is rendered as `42`, a timespan as `42ns`, a `string` as `"foo"`, or a
`set<bool>` as `{F, F, T}`.

### BGPdump

- **Type**: reader
- **Representation**: ASCII
- **Dependencies**: none

The BGPdump format is the textual output of the MRT format (see below).

### Zeek

- **Type**: reader, writer
- **Representation**: ASCII
- **Dependencies**: none

The Zeek format reads and writes ASCII output from the
[Zeek](https://www.zeek.org) network security monitor. A log consists of a
sequence of header rows, followed by log entries.

### CSV

- **Type**: writer
- **Representation**: ASCII
- **Dependencies**: none

The Comma-Separated Values (CSV) format writes one events as rows, prepended by
a header representing the event type. Whenever a new event type occurs, VAST
generates a new header.

### JSON

- **Type**: writer
- **Representation**: ASCII
- **Dependencies**: none

The JSON format writes events as in
[JSON Streaming](https://en.wikipedia.org/wiki/JSON_streaming) style. In
particular, VAST uses line-delimited JSON (LDJSON) to render one event per
line.

### MRT

- **Type**: reader
- **Representation**: binary
- **Dependencies**: none

The **Multi-Threaded Routing Toolkit (MRT)** format describes routing protocol
messages, state changes, and routing information base contents. See
[RFC 6396](https://tools.ietf.org/html/rfc6396) for a complete reference. The
implementation relies on BGP attributes, which
[RFC 4271](https://tools.ietf.org/html/rfc4271) defines in detail.

### PCAP

- **Type**: reader, writer
- **Representation**: binary
- **Dependencies**: libpcap

The PCAP format reads and writes raw network packets with *libpcap*. Events of
this type consit of the connection 4-tuple plus the binary packet data as given
by libpcap.

### Test

- **Type**: generator
- **Representation**: binary
- **Dependencies**: none

The test format acts as a "traffic generator" to allow users to generate
arbitrary events according to VAST's data model. It takes a schema as input and
then looks for specific type attributes describing distribution functions.
Supported distributions include `uniform(a, b)`, `normal(mu, sigma)`, and
`pareto(xm, alpha)`.

For example, to generate an event consisting of singular, normally-distributed
data with mean 42 and variance 10, you would provide the following schema:

  type foo = real &uniform(42, 10)

DATA MODEL
----------

VAST relies on a rich and strong type interface to support various
type-specific query operations and optimizations.

### Terminology

The phyiscal representation of information in VAST is *data*. A *type*
describes how to interpret data semantically. A type optionally carries a name
and a list of *attributes* in the form of key-value pairs. Together, a data
instance and a type form a *value*. A value with a named type is an *event*.
In addition to a value, an event has a timestamp and unique ID.

### Types

A type can be a *basic type*, a *container type* or a *compound type*.

#### Basic Types

- `bool`: a boolean value
- `int`: a 64-bit signed integer
- `count`: a 64-bit unsigned integer
- `real`: a 64-bit double (IEEE 754)
- `duration`: a time duration (nanoseconds granularity)
- `time`: a time point (nanoseconds granularity)
- `string`: a fixed-length string optimized for short strings
- `pattern`: a regular expression
- `address`: an IPv4 or IPv6 address
- `subnet`: an IPv4 or IPv6 subnet
- `port`: a transport-layer port

#### Container Types

- `vector<T>`: a sequence of instances of type T
- `set<T>`: an unordered mathematical set of instances of type T
- `map<T, U>`: an associative array that maps instances of type T to type U

#### Compound types

- `record { ... }`: a structure that contains a fixed number of typed and named
  *fields*.

### Schemas

A *schema* consists of a sequence of type statements having the form

    type T = x

where `T` is the name of a new type and `x` the name of an existing or built-in
type. Example:

    type foo = count

    type bar = record {
      x: foo,
      y: string,
      z: set<addr>
    }

This schema defines two types, a simple alias `foo` and a record `bar`.

### Specifying Schemas

During data import, VAST attempts to infer the *schema*, i.e., the pyiscal
representation of data along with plausible types. Users can also control
explicitly how to handle data by manually providing a path to schema file via
the command line option `-s <schema>`.

The only restriction is that the manually provided schema must be *congruent*
to the existing schema, that is, the types must be representationall equal.
Record field names do not affect congruence. Neither do type attributes.

For example, let's look at the builtin schema for PCAP data:

    type pcap::packet = record {
      meta: record {
        src: addr,
        dst: addr,
        sport: port,
        dport: port
      },
      data: string &skip
    }

A packet consists of meta data and a payload. The above schema skips the
payload (note the `&skip` attribute) because there exists no one-size-fits-all
strategy to indexing it. A congruent schema that further skips the
transport-layer ports may look as follows:

    type originator = addr

    type responder = addr

    type pcap::packet = record {
      header: record {
        orig: originator,
        resp: responder,
        sport: port &skip,
        dport: port &skip
      },
      payload: string &skip
    }

ISSUES
------

If you encounter a bug or have suggestions for improvement, please file an
issue at <http://vast.fail>.

SEE ALSO
--------

Visit <http://vast.io> for more information about VAST.
