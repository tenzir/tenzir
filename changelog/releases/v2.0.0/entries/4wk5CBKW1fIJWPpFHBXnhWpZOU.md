---
title: "Mark `experimental` encoding as `arrow.v2`"
type: change
author: dominiklohmann
created: 2022-03-31T09:01:20Z
pr: 2159
---

VAST's internal data model now completely preserves the nesting of the stored
data when using the `arrow` encoding, and maps the pattern, address,
subnet, and enumeration types onto Arrow extension types rather than using the
underlying representation directly. This change enables use of the `export
arrow` command without needing information about VAST's type system.

Transform steps that add or modify columns now transform the columns
in-place rather than at the end, preserving the nesting structure of the
original data.

The deprecated `msgpack` encoding no longer exists. Data imported using the
`msgpack` encoding can still be accessed, but new data will always use the
`arrow` encoding.
