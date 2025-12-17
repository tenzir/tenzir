---
title: "Move rebatching into the `importer` actor"
type: feature
author: dominiklohmann
created: 2025-03-19T14:07:42Z
pr: 5056
---

The import buffer timeout is now configurable via the
`tenzir.import-buffer-timeout` option. The option defaults to 1 second, and
controls how long the `import` operator buffers events for batching before
forwarding them. Set the option to `0s` to enable an unbuffered mode with
minimal latency, or to a higher value to increase performance.
