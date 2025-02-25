# read_feather

Parses an incoming Feather byte stream into events.

```tql
read_feather
```

## Description

Transforms the input [Feather] (a thin wrapper around
[Apache Arrow's IPC][arrow-ipc] wire format) byte stream to event stream.

[feather]: https://arrow.apache.org/docs/python/feather.html
[arrow-ipc]: https://arrow.apache.org/docs/python/ipc.html

## Examples

### Publish a feather logs file

```tql
load_file "logs.feather"
read_feather
pulish "log"
```
