# VAST 1 "@MAN_DATE@" @VERSION_MAJ_MIN@ "Visibility Across Space and Time"

NAME
----

VAST -- a unified platform for network forensics

SYNOPSIS
--------

`vast` [*options*...] [*command*] [*arguments*...]

DESCRIPTION
-----------

VAST is a distributed platform for large-scale network forensics. The
system is exclusively implemented in terms of the actor model. In this
model, concurrent entities (**actors**) execute in parallel and communicate
asynchronously solely via message passing. VAST users connect actors together
to create a specific system topology. Each physical node in a VAST deployment
typically runs a single `vast` daemon process which accommodates numerous
actors. For single-node deployments all actors can run inside this process,
but for multi-node deployments users can spawn actors on any node to configure
flexible topologies that suits best their data processing pipelines.

With the `vast` executable users can manage the system topology, configure
event sources and sinks to import and export data, and issue queries to
retrieve results.

The following key actors exist:

**TRACKER**
  Keeps track of the global system topology, registers new actors and wires
  them together.

**SOURCE**
  Generates events from a data source, such as packets from a network interface
  or log files.

**SINK**
  Receives events as the result of a query and displays them in specific output
  format, such JSON, PCAP (for packets), or Bro logs.

**ARCHIVE**
  Compressed bulk storage of the all events.

**INDEX**
  Accelerates queries by constructing bitmap indexes over a subset of the event
  data.

**IMPORTER**
  Accepts events from **SOURCEs**, assigns them unique 64-bit IDs, and relays
  them to **ARCHIVE** and **INDEX**.

**EXPORTER**
  Accepts query expressions from users, spawns **QUERY** actors, and relays
  them to **ARCHIVE** and **INDEX**.

**QUERY**
  A handle to a running query for retrieving index hits and extracting results.

OPTIONS
-------

This section describes the global `vast` *options*, irrespective of the
*command* issued. When not specifying any *options*, `vast` attempts to connect
to the process running at the local machine. 

`-a` *address* [*127.0.0.1*, *::1*]
  The IP *address* or hostname of the `vast` instance to connect to.

`-p` *port* [*42000*]
  The port of a `vast` instance to connect to.

`-l` *log-level* [*3*]
  The verbosity of console log messages shown on standard error. The
  *log-level* can take on the following values:
    - *0* *(quiet)*: do not produce any output
    - *1* *(error)*: report failures which constitute a program error
    - *2* *(warn)*: notable issues that do not affect correctness
    - *3* *(info)*: status messages representing system activity
    - *4* *(verbose)*: more fine-grained activity reports
    - *5* *(debug)*: copious amounts of low-level implementation details
    - *6* *(trace)*: log function entry and exit

`-v`
  Print the VAST version and exit.

COMMANDS
--------

This section describes each *command* and its *arguments*. 
When not specifying a *command*, `vast` enters an interactive shell. The
following commands exist:

  - *start*         starts a `vast` instance
  - *stop*          stops a `vast` instance
  - *topology*      shows the node and actor topology
  - *connect*       connects two actors
  - *disconnect*    disconnects two connected actors
  - *spawn*         creates a new actor
  - *import*        imports data from standard input
  - *export*        performs a query and exports results to standard output

### start 

Synopsis:

  *start* [*arguments*]

Starts a `vast` instance on this node.

Available *arguments*:
  
`-n` *name* [*hostname*]
  A unique name for this instance. The default name is the system hostname.

`-a` *address*... [*0.0.0.0*]
  The list of IP *address* where this instance listens on.

`-p` *port* [*42000*]
  The port where this instance binds to.

`-d` *directory* [*vast*]
  The path on the file system where to store persistent state.

`-l` *log-level* [*4*]
  The verbosity of the log file. See section **OPTIONS** for a description of
  possible values.

`-c`
  Spawn a *core* immediately after starting.

`-w` *threads* [*std::thread::hardware_concurrency()*]
  The number of worker threads to use for CAF's scheduler.

`-t` *messages* [*std::thread::hardware_concurrency()*]
  The CAF worker throughput expressed in the maximum number of messages to
  process when a worker gets scheduled.

`-p` *seconds* [*1*]
  Enable CAF profiling of worker threads and actors at the resolution
  *seconds*.

### stop

Synopsis:

  *stop* [*arguments*]

Stops a `vast` instance on this node. 

Available *arguments*:

`-n` *name*
  The name of the instance to stop.

### topology

Shows the system topology.

### connect

Synopsis:

  *connect* *A* *B*

Connect two actors named *A* and *B* by registering *A* as source for *B* and
*B* as sink for *A*.

### disconnect

Synopsis:

  *disconnect* *A* *B*

Removes a previously estalbished connection between *A* and *B*.

### spawn

Synopsis:

  *spawn* [*arguments*] *actor* [*parameters*]

Creates a new instance of type *actor*. Some actor types can have at most one
instance while others can have multiple instances.

Available *arguments*:

`-n` *name*
  Controls the spawn location. If `-n` *name* is given, *actor* will be spawned
  on the `vast` instance *name*. Otherwise *actor* will be spawned on the
  connected instance.

`-l` *label*
   A unique textual identifier within one `vast` instance. The default label
   has the form *actorN* where *N* is a running counter increased for each
   spawned instance of *actor*. 

Available *actor* values with corresponding *parameters*:

*archive* [*parameters*]
  `-s` *size* [*128*]
    Maximum segment size in MB
  `-c` *segments* [*10*]
    Number of cached segments

*index* [*parameters*]
  `-a` *partitions* [*5*]
    Number of active partitions to load-balance events over.
  `-p` *partitions* [*10*]
    Number of passive partitions.
  `-e` *events* [*1,000,000*]
    Maximum events per partition. When an active partition reaches its
    maximum, **INDEX** evicts it from memory and replaces it with an empty
    partition.

*importer*

*exporter*

*source* **X** [*parameters*]
  **X** specifies the format of *source*. Each source format has its own set of
  parameters, the following apply to all:
  `-b` *batch-size* [*100,000*]
    Number of events to read in one batch.
  `-s` *schema*
    Path to an alterative *schema* file which overrides the default attributes.
  `-r` *path*
    Name of the filesystem *path* (file or directory) to read events from.
  `-I` *importer*
    If no **IMPORTER** runs on the connected instance, one must specify `-I`
    *importer* to indicate the endpoint receiving the generated events.

*source* *bro*

*source* *bgpdump*

*source* *test* [*parameters*]
  `-n` *events*
    The maximum number of *events* to generate.

*source* *pcap* [*parameters*]
  `-i` *interface*
    Name of the network *interface* to read packets from.
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
  **X** specifies the format of *sink*. Each sink format has its own set of
  parameters, the following apply to all:
  `-w` *path*
    Name of the filesystem *path* (file or directory) to write events to.

*sink* *ascii*

*sink* *bro*

*sink* *json*

*sink* *pcap* [*parameters*]
  `-f` *flush* [*1000*]
    Flush the output PCAP trace after having processed *flush* packets.

*query* [*parameters*] *expression*
  `-c`
    Marks this query as **continuous**.
  `-h`
    Marks this query as **historical**.
  `-u`
    Marks this query as **unified**, which is equivalent to specifying both
    `-c` and `-h`.
  `-l` *n* [*0*]
    Limit the number of results to *n* entries. The value *n = 0* means
    unlimited.

*profiler* [*parameters*]
  If compiled with gperftools, eanbles the gperftools CPU or heap profiler to
  collect samples at a given resolution.
  `-c`
    Launch the CPU profiler.
  `-h`
    Launch the heap profiler.
  `-r` *seconds* [*1*]
    The profiling resolution in seconds.

### import

Synopsis:

  *import* [*arguments*]

Imports data on standard input. This command is a shorthand for spawning a
**SOURCE**, connecting it with an **IMPORTER**, and associating standard input
of the process invoking *import* with input stream of the spawned **SOURCE**.

### export

Synopsis:

  *export* [*arguments*] *expression*

Issues a query and exports results to standard output. This command is a
shorthand for spawning a **QUERY** and **SINK**, connecting the two, and
relaying the result stream to standard output of the process invoking *export*.

EXAMPLES
--------

Start a core:

  `vast` *start* `-c`

Connect to a `vast` instance running host *foo* at a port *31337*:

  `vast` `-a` *foo* `-p` *31337*

Import a Bro log files:

  zcat log.gz | `vast` *import* *bro*

Run a historical query with at most 100 results, printed in ASCII.

  `vast` *export* *ascii* -h -l 100 &type == "conn" && :addr in 10.0.0.0/8

Read packets from network interface *bge0* and send them to a remote
**IMPORTER** at node *foo*:

  `vast` *spawn* *source* *pcap* `-i` *bge0* `-I` *importer0*`@`*foo*

BUGS
----

If you encounter a bug or have suggestions for improvement, please file an
issue at https://github.com/mavam/vast/issues.

SEE ALSO
--------

Visit https://github.com/mavam/vast for more information about VAST.
