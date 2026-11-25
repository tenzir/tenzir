---
title: Filters move past replace
type: change
authors:
  - mavam
created: 2026-11-25T07:30:25.81181Z
---

Filters after `replace` now move upstream instead of stopping at it, so sources can apply them. This matters for mappings that clean up sentinel values first, such as the Windows OCSF mappings:

```tql
from_file "events.json"
replace what="-", with=null
where user.name == "alice"
```

The `where` now runs before `replace` as `user.name == "alice"`, where a source that supports filter pushdown can apply it. This covers scalar comparisons (`==`, `!=`, `in`) combined with `and`, `or`, and `not` over scalar `replace` operations; other filters—such as function calls, record comparisons, or replacements with records or `what=null`—stay behind `replace` so results stay the same.
