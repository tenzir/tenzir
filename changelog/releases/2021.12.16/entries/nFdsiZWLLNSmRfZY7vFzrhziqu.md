---
title: "Type FlatBuffers"
type: change
author: dominiklohmann
created: 2021-11-25T10:47:07Z
pr: 1888
---

VAST's internal type system has a new on-disk data representation. While we
still support reading older databases, reverting to an older version of VAST
will not be possible after this change. Alongside this change, we've
implemented numerous fixes and streamlined handling of field name lookups,
which now more consistently handles the dot-separator. E.g., the query `#field
== "ip"` still matches the field `source.ip`, but no longer the field
`source_ip`. The change is also performance-relevant in the long-term: For data
persisted from previous versions of VAST we convert to the new type system on
the fly, and for newly ingested data we now have near zero-cost deserialization
for types, which should result in an overall speedup once the old data is
rotated out by the disk monitor.
