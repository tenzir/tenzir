**VAST** is a platform for network forensics at scale. It ingests security
telemetry in a unified data model and offers a type-safe search interface to
extract a data in various formats.

The `vast` executable manages a VAST deployment by starting and interacting
with a **node**, the server-side component that manages the application state.

## Usage

The command line interface (CLI) is the primary way to interact with VAST.
All functionality is available in the form of *commands*, each of which
have their own set of options:

```
vast [options] [command] [options] [command] ...
```

Commands are recursive and the top-level root command is the `vast` executable
itself. Usage follows typical UNIX applications:

- *standard input* feeds data to commands
- *standard output* represents the result of a command
- *standard error* includes logging output

The `help` subcommand always prints the usage instructions for a given command,
e.g., `vast help` lists all available top-level subcommands.

More information about subcommands is available using `help` and `documentation`
subcommands. E.g., `vast import suricata help` prints a helptext for `vast
import suricata`, and `vast start documentation` prints a longer documentation
for `vast start`.

## Configuration

In addition to command options, a configuration file `vast.conf` allows for
persisting option values and tweaking system parameters. Command line options
always override configuration file values.

During startup, `vast` looks for a `vast.conf` in the current directory. If
the file does not exist, `vast` then attempts to open `PREFIX/etc/vast.conf`
where `PREFIX` is the installation prefix (which defaults to `/usr/local`).

## System Architecture

VAST consists of multiple *components*, each of which implement
specific system functionality. The following key componetns exist:

**source**
  Generates events by parsing a particular data format, such as packets from a
  network interface, IDS log files, or generic CSV or JSON data.

**sink**
  Produces events by printing them in a particular format, such as ASCII, CSV,
  JSON, PCAP, or Zeek logs.

**archive**
  Stores the raw event data.

**index**
  Accelerates queries by constructing index structures that point into the
  **archive**.

**importer**
  Ingests events from **source**s, assigns them unique IDs, and relays
  them to **archive** and **index** for persistence.

**exporter**
  Accepts query expressions from users, extracts events, and relays results
  to **sink**s.

### Schematic

```
                +--------------------------------------------+
                | node                                       |
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
`vast` processes, and are responsible for parsing the input and formatting the
search results.
