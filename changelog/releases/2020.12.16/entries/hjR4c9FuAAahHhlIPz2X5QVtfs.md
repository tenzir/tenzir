---
title: "Remove CAF-encoded table slices"
type: change
author: dominiklohmann
created: 2020-11-04T19:05:08Z
pr: 1142
---

CAF-encoded table slices no longer exist. As such, the option
`vast.import.batch-encoding` now only supports `arrow` and `msgpack` as
arguments.
