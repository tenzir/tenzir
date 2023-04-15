# shell

Executes a system command and hook the raw stdin and stdout into the pipeline.

## Synopsis

```
shell "<command>"
```

## Description

The `shell` operator forks the process and executes the provided command. It
then connects the child's stdin to the operator's intput, and the child's stdout
to the operator's output. When `shell` receive new bytes as input, it copies
them to the child's standard input. Afterwards, `shell` attempts to read from
the child's standard output and copies new bytes into the operator output.

### `<command>`

The command to execute and hook into the pipeline processing.

## Examples

Consider the use case of converting CSV to JSON:

```bash
vast exec 'read csv | write json' | jq
```

VAST renders the output as NDJSON. Piping this output to `jq` generates a
colored, tree-structured variation that is arguably easier to read. The above
example uses Unix pipes to connect stdout of `vast` to stdin of `jq`. The
`shell` operator makes it possible to integrate Unix tools that rely on
stdin/stdout for input/output as "native" VAST pipeline operators. We can inject
`shell` between all operators that process raw bytes. For example, in this
pipeline:

```c
print json | save stdout
```

The [`print`](../transformations/print.md) operator produces raw bytes and
[`save`](../sinks/save.md) accepts raw bytes. The `shell` operator therefore
fits right in the middle:

```c
print json | shell jq | save stdout
```

Using [user-defined operators](../user-defined.md), we can expose this
(potentially verbose) post-processing more succinctly in the pipeline language:

```yaml {0} title="vast.yaml"
vast:
  operators:
    jsonize: >
      print json | shell jq | save stdout
```

Now you can use `jsonize` as a custom operator in a pipeline:

```bash
vast exec 'read csv | where field > 42 | jsonize' < file.csv
```
