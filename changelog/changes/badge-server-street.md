---
title: "Metadata handling of `ocsf::derive`"
type: bugfix
authors: jachris
pr: 5402
---

The `ocsf::derive` operator now correctly uses the metadata (such as `@name`) of
the incoming event instead of overwriting it with the internal metadata used to
encode OCSF schemas.
