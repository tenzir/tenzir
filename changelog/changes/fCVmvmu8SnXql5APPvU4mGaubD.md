---
title: "Forcefully emit batches on input timeout error"
type: feature
authors: dominiklohmann
pr: 916
---

The `import` command gained a new `--read-timeout` option that forces data to be
forwarded to the importer regardless of the internal batching parameters and
table slices being unfinished. This allows for reducing the latency between the
`import` command and the node. The default timeout is 10 seconds.
