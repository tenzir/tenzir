---
title: "Fix the wrong type for the version record type in the `zeek.software` schema"
type: bugfix
author: Dakostu
created: 2023-09-25T06:40:52Z
pr: 3538
---

The `zeek.software` does not contain an incomplete `version` record type
anymore.

The `version.minor` type in the `zeek.software` schema is now a `uint64`
instead of a `double` to comply with Zeek's version structure.
