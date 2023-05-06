---
sidebar_position: 5
---

# Modifiers

Operator modifiers are keywords that may occur before an operator.

## Location Overrides

Pipelines run across multiple processes:
- The local `vast exec` process, and
- the remote `vast start` processes (commonly referred to as _nodes_).

Some pipeline operators prefer running either local or remote. For example, the
`from` and `to` operators run locally, and the `serve` operator runs remotely by
default. Operators that do not have a preferred location use the location of the
previous operator.

The special keywords `local` and `remote` allow for specifying the location of
an operator explicitly. They may occur before any operator. For example, the
pipeline `read json | remote pass | write json` reads JSON from stdin locally,
transfers it to a remote node to do nothing with the data, and
then transfers it back to write JSON to stdout locally.

There are generally two scenarios in which you may want to use location
overrides:

1. Move compute-heavy operators to a separate machine: Operators like
   `summarize` may require a lot of resources. When collecting events from an
   edge node, you may want to instead use `remote summarize` to run the
   computation on the compute-heavy machine.

2. Change local operators to run remotely, to allow for reading a file from a
   remote host, e,g., `remote from file /tmp/suricata.sock read suricata`.
   Because such an operation allows for remotely reading files or executing
   potentially unwanted operators, you must set the following configuration
   option to enable running local operators remotely (and vice versa):

   ```yaml {0} title="vast.yaml"
   vast:
     allow-unsafe-pipelines: true
   ```
