# throttle

Limits the bandwidth of a pipeline.

```tql
throttle bandwidth:uint, [within=duration]
```

## Description

The `throttle` operator limits the amount of data flowing through it to a
bandwidth.

### `bandwidth: uint`

The maximum bandwidth that is enforced for this pipeline, in bytes per the
specified interval.

### `within = duration (optional)`

The duration over which to measure the maximum bandwidth.

Defaults to `1s`.

## Examples

Read a TCP stream at a rate of 1 character per second:

```tql
load_tcp "tcp://0.0.0.0:4000"
throttle 1
```

Load a sample input data file at a speed of at most 1MiB every 10s and import it
into the node:

```tql
load_http "https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst"
throttle 1Mi, within=10s
decompress "zstd"
read_zeek_tsv
import
```
