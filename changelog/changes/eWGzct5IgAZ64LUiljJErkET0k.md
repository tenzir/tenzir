---
title: "Improve import batching options"
type: change
authors: dominiklohmann
pr: 1058
---

The options that affect batches in the `import` command received new, more
user-facing names: `import.table-slice-type`, `import.table-slice-size`, and
`import.read-timeout` are now called `import.batch-encoding`,
`import.batch-size`, and `import.read-timeout` respectively.
