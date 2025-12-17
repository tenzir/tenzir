---
title: "Make the source actor more responsive"
type: change
author: tobim
created: 2020-10-21T09:44:42Z
pr: 1096
---

The new option `import.read-timeout` allows for setting an input timeout for low
volume sources. Reaching the timeout causes the current batch to be forwarded
immediately. This behavior was previously controlled by `import.batch-timeout`,
which now only controls the maximum buffer time before the source forwards
batches to the server.
