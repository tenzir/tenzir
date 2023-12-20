---
sidebar_custom_props:
  operator:
    source: true
    transformation: true
---

# shell

Executes a system command and hooks its stdin and stdout into the pipeline.

## Synopsis

```
shell <command>
```

## Description

The `shell` operator executes the provided command by spawning a new process.
The input of the operator is forwarded to the child's standard input. Similarly,
the child's standard output is forwarded to the output of the operator.

### `<command>`

The command to execute and hook into the pipeline processing.

The value of `command` is a single string. If you would like to pass a command
line as you would on the shell, use single or double quotes for escaping, e.g.,
`shell 'jq -C'` or `shell "jq -C"`. The command is interpreted by `/bin/sh -c`.

## Examples

Show a live log from the `tenzir-node` service:

```
shell "journalctl -u tenzir-node -f | read json"
```

Consider the use case of converting CSV to JSON:

```bash
tenzir 'read csv | write json' | jq -C
```

The `write json` operator produces NDJSON. Piping this output to `jq` generates a
colored, tree-structured variation that is (arguably) easier to read. Using the
`shell` operator, you can integrate Unix tools that rely on
stdin/stdout for input/output as "native" operators that process raw bytes. For
example, in this pipeline:

```
write json | save stdout
```

The [`write`](write.md) operator produces raw bytes and [`save`](save.md)
accepts raw bytes. The `shell` operator therefore fits right in the middle:

```
write json | shell "jq -C" | save stdout
```

Using [user-defined operators](../language/user-defined-operators.md), we can
expose this (potentially verbose) post-processing more succinctly in the
pipeline language:

```yaml {0} title="tenzir.yaml"
tenzir:
  operators:
    jsonize:
      write json | shell "jq -C" | save stdout
```

Now you can use `jsonize` as a custom operator in a pipeline:

```bash
tenzir 'read csv | where field > 42 | jsonize' < file.csv
```

This mechanism allows for wrapping also more complex invocation of tools.
[Zeek](https://zeek.org), for example, converts packets into structured network
logs. Tenzir already has support for consuming Zeek output with the formats
[`zeek-json`](../formats/zeek-json.md) and
[`zeek-tsv`](../formats/zeek-tsv.md). But that requires attaching yourself
downstream of a Zeek instance. Sometimes you want instant Zeek analytics given a
PCAP trace.

With the `shell` operator, you can script a Zeek invocation and readily
post-process the output with a rich set of operators, to filter, reshape,
enrich, or route the logs as structured data. Let's define a `zeek` operator for
that:

```yaml {0} title="tenzir.yaml"
tenzir:
  operators:
    zeek:
      shell "zeek -r - LogAscii::output_to_stdout=T
             JSONStreaming::disable_default_logs=T
             JSONStreaming::enable_log_rotation=F
             json-streaming-logs"
      | read zeek-json
```

Processing a PCAP trace now is a matter of calling the `zeek` operator:

```bash
gunzip -c example.pcap.gz |
  tenzir 'zeek | select id.orig_h, id.orig_p, id.resp_h | head 3'
```

```json
{"id": {"orig_h": null, "resp_h": null, "resp_p": null}}
{"id": {"orig_h": "192.168.168.100", "resp_h": "83.135.95.78", "resp_p": 0}}
{"id": {"orig_h": "192.168.168.100", "resp_h": "83.135.95.78", "resp_p": 22}}
```

NB: because `zeek` (= `shell`) reads bytes, we can drop the implicit `load
stdin` source operator in this pipeline.
