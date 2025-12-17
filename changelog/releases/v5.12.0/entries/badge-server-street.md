---
title: "Metadata handling of `ocsf::derive` and `ocsf::trim`"
type: bugfix
author: jachris
created: 2025-08-03T22:38:35Z
pr: 5402
---

The `ocsf::derive` and `ocsf::trim` operators now correctly preserve the
metadata (such as `@name`) of the incoming event instead of overwriting it with
the internal metadata used to encode OCSF schemas.
