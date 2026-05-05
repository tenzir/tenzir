---
title: Take expression keyword for consumed fields
type: change
authors:
  - mavam
created: 2026-05-05T18:28:58.096342Z
---

The `take` expression keyword now replaces `move` for consuming fields in assignments:

```tql
new_field = take old_field
```

The previous `move` expression keyword is deprecated but still accepted during the transition. The statement-level `move` operator remains unchanged.
