---
title: "Timezone troubles from `parse_time()`"
type: bugfix
author: raxyte
created: 2025-08-25T11:15:50Z
pr: 5435
---

We fixed assertion failures when using the `parse_time` function with the `%z`
or `%Z` specifiers.
