# shell

Executes a system command and hooks its stdout into the pipeline.

## Synopsis

```
shell <command>
```

## Description

Refer to [`shell` as transformation](../transformations/shell.md) for usage
instructions.

The difference to the transformation is that the source operator ignores the
command's stdin.

## Examples

Show a live log from the `tenzir-node` service:

```
shell "journalctl -u tenzir-node -f | parse json"
```
