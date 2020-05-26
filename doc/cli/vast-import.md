The `import` command ingests data. An optional filter expression allows for
restricing the input to matching events. The format of the imported data must
be explicitly specified:

```
vast import [options] <format> [options] [expr]
```

The `import` command is the dual to the `export` command.

The `--type` / `-t` option can be specified for each format to filter the known
types based on a prefix. E.g., `vast import json -t zeek` matches all event
types that begin with `zeek`, and restricts the event types known to the import
command accordingly.

Imported event types are made available to the node permanently, and do not need
to be specified again for consecutive imports.
