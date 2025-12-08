---
title: "Fixed missing Zeek fields"
type: bugfix
authors: IyeOnline
pr: 5445
---

Zeek JSON contains fields such as `io.data.read.bytes` and
`io.data.read.bytes.per-second`. These fields would previously overwrite each other
in order of appearance.

With this change `bytes` now is a record and the original value is kept under the
key `""`.
