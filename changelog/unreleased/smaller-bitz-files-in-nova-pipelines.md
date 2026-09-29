---
title: Smaller BITZ files in Nova pipelines
type: change
authors:
  - aljazerzen
created: 2026-09-29T10:03:11.90114Z
---

Nova-enabled `write_bitz` now produces smaller BITZ files for repeated or sparse bitmaps, small integer values, losslessly representable floats, and mixed-type values. `read_bitz` continues to read older BITZ encodings.

For example, run `from {value: 42} | write_bitz` with `--nova=true` to write a BITZ stream with the new encodings.
