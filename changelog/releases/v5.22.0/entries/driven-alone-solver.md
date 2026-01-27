---
title: "Fixed missing Zeek fields"
type: bugfix
author: IyeOnline
created: 2025-12-08T16:56:43Z
pr: 5445
---

Zeek JSON contains fields such as `io.data.read.bytes` and
`io.data.read.bytes.per-second`. These fields would previously overwrite each other
in order of appearance.

With this change `bytes` now is a record and the original value is kept under the
key `""`.
