# VASTD 1 "@MAN_DATE@" @VERSION_MAJ_MIN@ "Visibility Across Space and Time"

NAME
----

`vastd` -- VAST daemon

SYNOPSIS
--------

`vastd` [*options*]

OVERVIEW
--------

VAST is a distributed platform for large-scale network forensics. It's modular
system architecture is exclusively implemented in terms of the actor model. In
this model, concurrent entities (actors) execute in parallel and
communicate asynchronously solely via message passing. Users spawn system
components as actors and connect them together to create a custom system
topology.

DESCRIPTION
-----------

Typically, each physical machine in a VAST deployment runs a single `vastd`
process. For single-node deployments all actors run inside this process,
whereas cluster deployments consist of multiple nodes with actors spread across
them.

A `vastd` process spawn a special *node* actor which acts as a container for
other actors. Nodes can enter a peering relationship to share global state. The
set of peering nodes constitutes a VAST *ecosystem*: an overlay network in
which actors can be connected irrespective of process boundaries.

OPTIONS
-------

The following *options* are available:

`-c`
  Start all core actors and connect them. Specifying this option automatically
  executes the following commands after starting the node:

      vast spawn identifier
      vast spawn importer
      vast spawn archive
      vast spawn index
      vast connect importer identifier
      vast connect importer archive
      vast connect importer index

`-d` *directory* [*vast*]
  The path on the file system where to store persistent state.

`-e` *endpoint* [*127.0.0.1:42000*]
  The endpoint of the node to connect to or launch. (See below)

`-f`
  Start daemon in foreground, i.e., do not detach from controlling terminal and
  run in background.

`-h`
  Display a help message and exit.

`-l` *verbosity* [*3*]
  The logging verbosity. (See below)

`-m` *messages* [*-1*]
  The CAF worker throughput expressed in the maximum number of messages to
  process when a worker gets scheduled. The default value of *-1* means an
  unlimited number of messages.

`-n` *name* [*hostname*]
  Overrides the node *name*, which defaults to the system hostname. Each node
  in an ecosystem must have a unique name, otherwise peering fails.

`-p` *logfile*
  Enable CAF profiling of worker threads and actors and write the per-second
  sampled data to *logfile*.

`-t` *threads* [*std::thread::hardware_concurrency()*]
  The number of worker threads to use for CAF's scheduler.

`-v`
  Print VAST version and exit.

### endpoint

An endpoint has the format *host:port* where *host* is a hostname or IP address
and *port* the transport-layer port of the listening daemon. Either can be
omitted: *host* or *:port* are also valid endpoints. IPv6 addresses must be
enclosed in brackets when also specifying a *port*, e.g., *[::1]:42001*.

### verbosity

The verbosity controls the amount of status messages displayed on console and
in log files. It can take on the following values:
  - *0* *(quiet)*: do not produce any output
  - *1* *(error)*: report failures which constitute a program error
  - *2* *(warn)*: notable issues that do not affect correctness
  - *3* *(info)*: status messages representing system activity
  - *4* *(verbose)*: more fine-grained activity reports
  - *5* *(debug)*: copious low-level implementation details
  - *6* *(trace)*: log function entry and exit


EXAMPLES
--------

Start a node in the foreground with debugging logging verbosity and launch all
core actors:

    vastd -l 5 -f -c

Start a node at a different port with 10 worker threads:

    vastd -e :6666 -t 10

BUGS
----

If you encounter a bug or have suggestions for improvement, please file an
issue at https://github.com/mavam/vast/issues.

SEE ALSO
--------

vast(1)

Visit https://github.com/mavam/vast for more information about VAST.
