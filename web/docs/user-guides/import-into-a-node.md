---
sidebar_position: 3
---

# Import into a node

Importing (or *ingesting*) data is equivalent to [running a
pipeline](run-a-pipeline/README.md) that ends with the
[`import`](../operators/sinks/import.md) sink oeprator.

Consider this example that takes a Zeek conn.log from our M57 dataset:

```
from file Zeek/conn.log read zeek-tsv
| select id.orig_h, id.resp_h, orig_bytes, resp_bytes
| where orig_bytes > 1 MiB
| import
```

The `import` operator requires a node execution context. To invoke the above
pipeline successfully, you need to [deploy a
node](../setup-guides/deploy-a-node/README.md) first. In the Tenzir app, you
always have a node, but it will fail on the command line without one. The reason
is that the `tenzir` process schedules the first three operators in the local
process and attempts to place the final `import` operator at a remote node, by
transparently connecting to the (default) endpoint `127.0.0.1:5158`.
