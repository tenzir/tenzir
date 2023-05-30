# shell

Executes a system command and hooks its raw stdin and stdout into the pipeline.

## Synopsis

```
shell <command>
```

## Description

The `shell` operator forks the process and executes the provided command.
Thereafter, it connects the child's stdin to the operator's input, and the
child's stdout to the operator's output. When `shell` receive new bytes as
input, it writes them to the child's standard input. In parallel, `shell`
attempts to read from the child's stdout and copies new bytes into the operator
output.

You can also use [`shell` as source operator](../sources/shell.md) if you want
to ignore stdin.

### `<command>`

The command to execute and hook into the pipeline processing.

The value of `command` is a single string. If you would like to pass a command
line as you would on the shell, use single or double quotes for escaping, e.g.,
`shell 'jq -C'` or `shell "jq -C"`.

## Examples

Consider the use case of converting CSV to JSON:

```bash
vast exec 'read csv | write json' | jq -C
```

The `write json` operator produces NDJSON. Piping this output to `jq` generates a
colored, tree-structured variation that is (arguably) easier to read. Using the
`shell` operator, you can integrate Unix tools that rely on
stdin/stdout for input/output as "native" operators that process raw bytes. For
example, in this pipeline:

```
print json | save stdout
```

The [`print`](../transformations/print.md) operator produces raw bytes and
[`save`](../sinks/save.md) accepts raw bytes. The `shell` operator therefore
fits right in the middle:

```
print json | shell "jq -C" | save stdout
```

Using [user-defined operators](../user-defined.md), we can expose this
(potentially verbose) post-processing more succinctly in the pipeline language:

```yaml {0} title="vast.yaml"
vast:
  operators:
    jsonize: >
      print json | shell "jq -C" | save stdout
```

Now you can use `jsonize` as a custom operator in a pipeline:

```bash
vast exec 'read csv | where field > 42 | jsonize' < file.csv
```

This mechanism allows for wrapping also more complex invocation of tools.
[Zeek](https://zeek.org), for example, converts packets into structured network
logs. VAST already has support for consuming Zeek output with the formats
[`zeek-json`](../../formats/zeek-json.md) and
[`zeek-tsv`](../../formats/zeek-tsv.md). But that requires attaching yourself
downstream of a Zeek instance. Sometimes you want instant Zeek analytics given a
PCAP trace.

With the `shell` operator, you can script a Zeek invocation and readily
post-process the output with a rich set of operators, to filter, reshape,
enrich, or route the logs as structured data. Let's define a `zeek` operator for
that:

```yaml {0} title="vast.yaml"
vast:
  operators:
    zeek: >
      shell "zeek -r - LogAscii::output_to_stdout=T
             JSONStreaming::disable_default_logs=T
             JSONStreaming::enable_log_rotation=F
             json-streaming-logs"
      | parse zeek-json
```

Processing a PCAP trace now is a matter of calling the `zeek` operator:

```bash
gunzip -c example.pcap.gz |
  vast exec 'zeek | select id.orig_h, id.orig_p, id.resp_h | head 3'
```

```json
{"id": {"orig_h": null, "resp_h": null, "resp_p": null}}
{"id": {"orig_h": "192.168.168.100", "resp_h": "83.135.95.78", "resp_p": 0}}
{"id": {"orig_h": "192.168.168.100", "resp_h": "83.135.95.78", "resp_p": 22}}
```

NB: because `zeek` (= `shell`) reads bytes, we can drop the implicit `load
stdin` source operator in this pipeline.
