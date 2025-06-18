---
title: "Deprecate the msgpack table slice"
type: change
authors: tobim
pr: 2087
---

The `msgpack` encoding option is now deprecated. VAST issues a warning on
startup and automatically uses the `arrow` encoding instead. A future version of
VAST will remove this option entirely.

The experimental aging feature is now deprecated. The [compaction
plugin](https://vast.io/docs/about/use-cases/data-aging) offers a superset
of the aging functionality.
