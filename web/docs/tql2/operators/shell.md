# shell

Executes a system command and hooks its stdin and stdout into the pipeline.

```tql
shell cmd:str
```

## Description

The `shell` operator executes the provided command by spawning a new process.
The input of the operator is forwarded to the child's standard input. Similarly,
the child's standard output is forwarded to the output of the operator.

### `cmd: str`

The command to execute and hook into the pipeline processing.

The command is interpreted by `/bin/sh -c`.

:::tip Lots of escaping?
Try using raw string literals to simplify: `r#"This is a valid "string" with
nested quotes(") "#`.
:::

## Examples

```tql
shell ""
```
