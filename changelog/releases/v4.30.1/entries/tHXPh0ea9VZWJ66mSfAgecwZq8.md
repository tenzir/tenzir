---
title: "Move rebatching into the `importer` actor"
type: change
author: dominiklohmann
created: 2025-03-19T14:07:42Z
pr: 5056
---

The default value for the `tenzir.active-partition-timeout` option increased
from 30s to 5min. The option controls how long the `import` operators waits
until flushing events to disk. In the past, this value was set so low because
the `export` operator was only able to access already flushed events. This is no
longer the case, removing the need for the low timeout. Note that the `import`
operator always immediately flushes events after a pipeline with `import`
completes, or when the node shuts down.
