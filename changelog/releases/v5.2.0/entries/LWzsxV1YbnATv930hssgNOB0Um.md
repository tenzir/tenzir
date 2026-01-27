---
title: "Add `from_file` operator"
type: feature
author: jachris
created: 2025-05-23T12:30:34Z
pr: 5203
---

The new `from_file` operator can be used to read multiple files from a
potentially remote filesystem using globbing expressions. It also supports
watching for new files and deletion after a file has been read.

`read_lines` now has an additional `binary=true` option which should be used if
the incoming byte stream is not valid UTF-8.
