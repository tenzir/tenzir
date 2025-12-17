---
title: "Experiment with a trailing `?` for field access"
type: feature
author: dominiklohmann
created: 2025-04-23T16:30:54Z
pr: 5128
---

The `.?field` operator for field access with suppressed warnings is now
deprecated in favor of `.field?`. We added the `.?` operator just recently, and
it quickly gained a lot of popularity. However, suppressing warnings in
top-level fields required writing `this.?field`, which is a mouthful. Now, with
the trailing questionmark, this is just `field?` instead. Additionally, the
trailing `?` operator works for index-based access, e.g., `field[index]?`. The
`.?` operator will be removed in the near future. We're sorry for the
inconvenience.
