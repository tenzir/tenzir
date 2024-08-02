---
sidebar_custom_props:
  operator:
    transformation: true
---

# taste

Limits the bandwidth of a pipeline.

## Synopsis

```
throttle [<max_bandwidth>]
```

## Description

The `throttle` operator limits the amount of data flowing through it to a
maximum bandwidth.

::: Caution
The operator does not split up incoming packets, but sleeps until
the average bandwidth is below the configured limit. When the size of incoming
data packets is very large compared to the limit, this can lead to a bursty
behavior where the pipeline does nothing most of the time and then handles
a large amount of events at once.
:::

### `<max_bandwidth>`

An unsigned integer giving the maximum bandwidth that is enforced for
this pipeline, in bits per second.

## Examples

Read a tcp stream limited to 4KiB/s and print the data to stdout:

```
load tcp://0.0.0.0:4000 | throttle 4096
```

Load a sample input data file at a speed of at most 1MiB/s and import
it into the node:

```
load https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst
| throttle 1Mi
| decompress
| read zeek-tsv
| import
```
