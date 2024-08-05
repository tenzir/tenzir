---
sidebar_custom_props:
  operator:
    transformation: true
---

# throttle

Limits the bandwidth of a pipeline.

## Synopsis

```
throttle <bandwidth>
```

## Description

The `throttle` operator limits the amount of data flowing through it to a
maximum bandwidth.

### `<bandwidth>`

An unsigned integer giving the maximum bandwidth that is enforced for
this pipeline, in bytes per second.

## Examples

Read a TCP stream and print the data to stdout at a rate of 1 character per
second:

```
load tcp://0.0.0.0:4000 | throttle 1
```

Load a sample input data file at a speed of at most 1MiB/s and import
it into the node:

```
load https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst
| throttle 1Mi
| decompress zstd
| read zeek-tsv
| import
```
