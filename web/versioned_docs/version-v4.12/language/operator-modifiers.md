---
sidebar_position: 5
---

# Operator Modifiers

Operator modifiers are keywords that may occur before an operator.

## Scheduled Executions

The special keyword `every` enables scheduled execution of an operator.

Use the operator modifier like this:

```
every <interval> <operator> [<args...>]
```

For example, `version` prints the version number exactly once, but `every 1s
version` prints the version number once every second.

## Unordered Execution

The `unordered` modifier tells an operator that it may return results out of
order. For example, `unordered read json` may be faster than `read json`, as it
allows the JSON parser to read events out of order.

By default, operators infer ordering requirements from the next operator. For
example, in `read json | sort`, the `sort` operator already lets `read json`
know that it may return results out of order.

## Location Overrides

Pipelines run across multiple processes:

- The local `tenzir` process, and
- the remote `tenzir-node` processes (commonly referred to as *nodes*).

Some pipeline operators prefer running either local or remote. For example, the
`from` and `to` operators run locally, and the `serve` operator runs remotely by
default. Operators that do not have a preferred location use the location of the
previous operator.

The special keywords `local` and `remote` allow for specifying the location of
an operator explicitly. They may occur before any operator. For example, the
pipeline `read json | remote pass | write json` reads JSON from stdin locally,
transfers it to a remote node to do nothing with the data, and
then transfers it back to write JSON to stdout locally.

Use the operator modifier like this:

```
local  <operator> [<args...>]
remote <operator> [<args...>]
```

There are generally two scenarios in which you may want to use location
overrides:

1. Move compute-heavy operators to a separate machine: Operators like
   `summarize` may require a lot of resources. When collecting events from an
   edge node, you may want to instead use `remote summarize` to run the
   computation on the compute-heavy machine.

2. Change local operators to run remotely, to allow for reading a file from a
   remote host, e,g., `remote from file /tmp/suricata.sock read suricata`.
   Because such an operation allows for remotely reading files or executing
   potentially unwanted operators, you can disable such overrides by setting the
   following configuration option:

   ```yaml {0} title="tenzir.yaml"
   tenzir:
     no-location-overrides: true
   ```

   If you want more fine-grained control about which operators, operator
   modifiers, formats, and connectors are available, you can selectively disable
   them in the configuration:

   ```yaml {0} title="tenzir.yaml"
   tenzir:
     disable-plugins:
       - shell
       - remote
   ```
