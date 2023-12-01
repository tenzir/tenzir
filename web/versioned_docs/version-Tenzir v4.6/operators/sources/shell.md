# shell

Executes a system command and hooks its stdout into the pipeline.

## Synopsis

```
shell <command>
```

## Description

Refer to [`shell` as transformation](../transformations/shell.md) for usage
instructions.

The difference to the transformation is that the source operator receives the
standard input from the terminal where `tenzir` is executed from. If the
pipeline was not spawned by a terminal, no standard input is provided.

## Examples

Show a live log from the `tenzir-node` service:

```
shell "journalctl -u tenzir-node -f | read json"
```
