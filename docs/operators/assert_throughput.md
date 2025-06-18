---
title: assert_throughput
category: Filter
example: 'assert_throughput 1000, within=1s'
---

Emits a warning if the pipeline does not have the expected throughput

```tql
assert_throughput min_events:int, within=duration, [retries=int]
```

## Description

The `assert_throughput` operator checks a pipeline's throughput, emitting a
warning if the minimum specified throughput is unmet, and optionally an error if
the number of retries is exceeded.

## Examples

### Require 1,000 events per second, failing if the issue persists for 30s

```tql
from "udp://0.0.0.0:514" { read_syslog }
assert_throughput 1k, within=1s, retries=30
```

## See Also

[`assert`](/reference/operators/assert)
