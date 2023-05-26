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

### `<command>`

The command to execute and hook into the pipeline processing.

The value of `command` is a single string. If you would like to pass a command
line as you would on the shell, use single or double quotes for escaping, e.g.,
`shell 'jq -C'` or `shell "jq -C"`.

## Examples

Consider the use case of converting CSV to JSON:

```bash
vast exec 'read csv | write json' | jq
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
