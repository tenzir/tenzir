
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

Convert a tree-structured JSON file to NDJSON via `jq`:

```c
shell "jq -c ." | parse json
```

Note that this is equivalent to:

```bash
jq -c | vast exec 'read json'
```

Now add an [operator alias](../user-defined.md) to your `vast.yaml`:

```yaml {0} title="vast.yaml"
vast:
  operators:
    jq: >
      shell "jq -c ." | parse json
```

Now you can use `jq` as a custom operator in a pipeline:

```bash
vast exec 'jq | where field > 42' < tree.json
```
