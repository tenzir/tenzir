The `import` command ingests data. An optional filter expression allows for
restricing the input to matching events. The format of the imported data must
be explicitly specified:

```
vast import [options] <format> [options] [expr]
```

The `import` command is the dual to the `export` command.

The `--type` / `-t` option filters known event types based on a prefix.  E.g.,
`vast import json -t zeek` matches all event types that begin with `zeek`, and
restricts the event types known to the import command accordingly.

VAST permanently tracks imported event types. They do not need to be specified
again for consecutive imports.
